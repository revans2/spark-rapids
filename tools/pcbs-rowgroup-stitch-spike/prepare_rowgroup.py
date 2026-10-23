# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generate isolated row-group PCBS artifacts from the frozen benchmark baseline."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import zipfile

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location(
    "frozen_preparer", str(HERE.parent / "pcbs-real-data-comparison/prepare_gpu_overlay.py"))
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
replace = base.replace_once

CONSUMER = """private class ParquetBufferConsumer(val numRows: Int, schema: StructType)
    extends HostBufferConsumer with
    AutoCloseable {
  private val regions = mutable.ArrayBuffer.empty[(HostMemoryBuffer, Long)]
  private var closed = false
  Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => close()))

  override def handleBuffer(buffer: HostMemoryBuffer, len: Long): Unit = {
    require(!closed, "Writer callback after close")
    regions += ((buffer, len))
  }

  // The research GPU writer must never concatenate the complete file on the heap.
  def getBuffer: Array[Byte] =
    throw new UnsupportedOperationException("Use independent row-group records")

  def records: Iterator[ParquetCachedBatch] = {
    val source = ParquetRowGroupFiles.fromHostRegions(regions.toSeq)
    val pieces = ParquetRowGroupFiles.split(source, 256 * 1024 * 1024)
    new Iterator[ParquetCachedBatch] {
      override def hasNext: Boolean = {
        val available = pieces.hasNext
        if (!available) close()
        available
      }
      override def next(): ParquetCachedBatch = {
        require(!closed, "Row-group source already closed")
        try {
          val piece = pieces.next()
          RowGroupMetrics.recordStored(
            ParquetRowGroupFiles.describe(piece, schema))
          if (!pieces.hasNext) close()
          piece
        } catch {
          case failure: Throwable =>
            close()
            throw failure
        }
      }
    }
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      regions.map(_._1).safeClose()
      regions.clear()
    }
  }
}
"""

def transform(pcbs):
    start = pcbs.index("private class ParquetBufferConsumer(")
    end = pcbs.index("\nprivate object ParquetCachedBatch", start)
    pcbs = pcbs[:start] + CONSUMER + pcbs[end:]
    pcbs = replace(pcbs, "new ParquetBufferConsumer(table.getRowCount.toInt)",
                   "new ParquetBufferConsumer(table.getRowCount.toInt, schema)")
    pcbs = replace(pcbs, "import com.nvidia.spark.rapids.Arm.withResource",
                   "import com.nvidia.spark.rapids.Arm.{closeOnExcept, withResource}")
    pcbs = replace(pcbs,
        "          withResource(putOnGpuIfNeeded(batch)) { gpuCB =>\n",
        "          withResource(putOnGpuIfNeeded(batch)) { gpuCB =>\n"
        "            org.apache.spark.sql.pcbsrealdata.PcbsCodecCounters.recordProduced(\n"
        "              gpuCB.numRows().toLong, GpuColumnVector.getTotalDeviceMemoryUsed(gpuCB))\n")
    pcbs = replace(pcbs,
        "      useCompression: Boolean): List[ParquetCachedBatch] = {",
        "      useCompression: Boolean): Iterator[ParquetCachedBatch] = {")
    pcbs = replace(pcbs, "return List.empty[ParquetCachedBatch]",
                   "return Iterator.empty")
    pcbs = replace(pcbs, "val buffers = new ListBuffer[ParquetCachedBatch]",
                   "val buffers = new ListBuffer[Iterator[ParquetCachedBatch]]")
    assert pcbs.count("buffers += ParquetCachedBatch(buffer)") == 2
    pcbs = pcbs.replace("buffers += ParquetCachedBatch(buffer)", "buffers += buffer.records")
    pcbs = replace(pcbs, "      buffers.toList", "      buffers.iterator.flatten")
    pcbs = replace(pcbs,
        ".withStatisticsFrequency(StatisticsFrequency.ROWGROUP).build()",
        ".withRowGroupSizeRows(65536).withRowGroupSizeBytes(8388608)\n"
        "        .withStatisticsFrequency(StatisticsFrequency.ROWGROUP).build()")
    before = """    withResource(Table.writeParquetChunked(opts, buffer)) { writer =>
      writer.write(table)
    }
    buffer"""
    after = """    closeOnExcept(buffer) { consumer =>
      RowGroupMetrics.recordEncodeAttempt()
      withResource(Table.writeParquetChunked(opts, consumer)) { writer =>
        writer.write(table)
      }
      RowGroupMetrics.recordEncoded(table.getRowCount)
      consumer
    }"""
    pcbs = replace(pcbs, before, after)
    start = pcbs.index("    val cbRdd: RDD[ColumnarBatch] = input.map {")
    end = pcbs.index("\n    cbRdd\n", start)
    old = pcbs[start:end]
    cast_start = old.index("          withResource(table) { table =>")
    cast_end = old.index("\n        }\n      case _ =>", cast_start)
    cast = old[cast_start:cast_end]
    # Keep the existing deep casts and caller-owned output unchanged.
    replacement = """    val cbRdd: RDD[ColumnarBatch] = input.mapPartitions { records =>
      val cacheRecords = records.buffered
      new Iterator[ColumnarBatch] {
        override def hasNext: Boolean = cacheRecords.hasNext
        override def next(): ColumnarBatch = {
          GpuSemaphore.acquireIfNecessary(TaskContext.get())
          if (cacheRecords.head.sizeInBytes == 0) {
            val empty = cacheRecords.next().asInstanceOf[ParquetCachedBatch]
            withResource(new GpuColumnarBatchBuilder(
                originalSelectedAttributes.toStructType, empty.numRows)) { builder =>
              builder.build(empty.numRows)
            }
          } else {
            withResource(ParquetRowGroupFiles.stageNextGroup(cacheRecords,
                cacheAttributes.toStructType, 1024L * 1024 * 1024, 256 * 1024 * 1024)) { files =>
              RowGroupMetrics.recordBundle(
                files.map(_.info.estimatedDecodedBytes).sum,
                files.map(_.info.encodedBytes.toLong).sum)
              val parquetOptions = ParquetOptions.builder()
                .includeColumn(selectedAttributes.map(_.name).asJavaCollection).build()
              val result = withRetryNoSplit[ColumnarBatch] {
                withResource(files.safeMap(_.getHostBuffer())) { hostBuffers =>
                  RowGroupMetrics.recordDecodeAttempt()
                  val table = ai.rapids.cudf.ParquetMultiFileReader.read(
                    parquetOptions, hostBuffers)
CAST
                }
              }
              closeOnExcept(result) { batch =>
                RowGroupMetrics.recordDecoded(files.length, batch.numRows().toLong,
                  GpuColumnVector.getTotalDeviceMemoryUsed(batch))
                batch
              }
            }
          }
        }
      }
    }"""
    pcbs = pcbs[:start] + replacement.replace("CAST", cast) + pcbs[end:]
    return pcbs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--native-build", type=Path, required=True)
    parser.add_argument("--compression", choices=["NONE", "SNAPPY"], default="SNAPPY")
    parser.add_argument("--old-artifacts", type=Path, default=Path(
        "/data/pcbs-oom/controlled-cache-comparison/artifacts"))
    args = parser.parse_args()
    old = json.loads((args.old_artifacts / "preparation-inputs.json").read_text())
    plugin = Path(old["baseline_plugin"])
    assert base.sha(plugin) == base.BASE_SHA256
    spark = Path(old["spark_home"])
    jars = sorted((spark / "assembly/target/scala-2.12/jars").glob("*.jar"))
    overlay = Path(old["spark_codec_overlay"])
    assert base.sha(overlay) == old["spark_codec_overlay_sha256"]
    native_jar = args.native_build / "pcbs-parquet-multifile.jar"
    native_lib = args.native_build / "libpcbs_parquet_multifile.so"
    assert native_jar.is_file() and native_lib.is_file()
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=False)
    source = out / "sources"
    source.mkdir()
    for name in ("ParquetRowGroupFiles.scala", "RowGroupMetrics.scala",
                 "PcbsRecordCodec.scala", "prepare_rowgroup.py"):
        shutil.copyfile(HERE / name, source / name)
    original = base.command(
        ["git", "show", base.BASE_REVISION + ":" + base.PCBS], cwd=base.ROOT)
    (source / "baseline-PCBS.scala").write_text(original)
    pcbs = source / "ParquetCachedBatchSerializer.scala"
    pcbs.write_text(transform(original))
    scan = base.command(["git", "show", base.BASE_REVISION + ":" + base.SCAN], cwd=base.ROOT)
    scan = replace(scan, "      numOutputRows += cb.numRows()\n      cb",
        "      com.nvidia.spark.rapids.Arm.closeOnExcept(cb) { batch =>\n"
        "        org.apache.spark.sql.pcbsrealdata.PcbsCodecCounters.recordDecoded(\n"
        "          batch.numRows().toLong,\n"
        "          com.nvidia.spark.rapids.GpuColumnVector.getTotalDeviceMemoryUsed(batch))\n"
        "        numOutputRows += batch.numRows()\n        batch\n      }")
    (source / "GpuInMemoryTableScanExec.scala").write_text(scan)
    deps = args.old_artifacts / "plugin-compile-classes"
    assert deps.is_dir()
    cp = ":".join(map(str, [overlay, native_jar, plugin, deps] + jars))
    fixture_classes = out / "fixture-classes"
    fixture_classes.mkdir()
    base.command(["java", "-Xmx2g", "-cp", cp, "scala.tools.nsc.Main",
        "-classpath", cp, "-d", str(fixture_classes), str(source / "PcbsRecordCodec.scala")],
        log=out / "compile-fixture.log")
    fixture = out / "rowgroup-codec-fixture.jar"
    base.command(["jar", "cf", str(fixture), "-C", str(fixture_classes), "."],
                 log=out / "package-fixture.log")
    classes = out / "plugin-classes"
    classes.mkdir()
    sources = [pcbs, source / "GpuInMemoryTableScanExec.scala",
               source / "ParquetRowGroupFiles.scala", source / "RowGroupMetrics.scala"]
    base.command(["java", "-Xmx3g", "-cp", cp, "scala.tools.nsc.Main",
        "-classpath", str(fixture) + ":" + cp, "-d", str(classes)] + list(map(str, sources)),
        log=out / "compile-plugin.log")
    replacements = {"spark-shared/" + p.relative_to(classes).as_posix(): p
                    for p in classes.rglob("*.class")}
    artifact = out / "rowgroup-plugin.jar"
    with zipfile.ZipFile(plugin) as original, zipfile.ZipFile(artifact, "w") as target:
        for item in original.infolist():
            if item.filename not in replacements:
                with original.open(item) as src, target.open(item, "w") as dest:
                    shutil.copyfileobj(src, dest, length=1024 * 1024)
        for entry, path in replacements.items():
            target.write(path, entry, compress_type=zipfile.ZIP_DEFLATED)
    files = [{"role": role, "path": str(path), "sha256": base.sha(path)} for role, path in (
        ("plugin", artifact), ("codec_fixture", fixture), ("native_reader_jar", native_jar),
        ("native_library", native_lib), ("codec_overlay", overlay))]
    manifest = dict(research_only=True, spark_version="3.5.5",
        global_batch_bytes=1073741824, rowgroup_target_rows=65536,
        rowgroup_target_bytes=8388608, decode_target_bytes=1073741824,
        record_safety_limit_bytes=268435456, compression=args.compression, files=files,
        sources={str(p): base.sha(p) for p in source.iterdir() if p.is_file()},
        baseline_revision=base.BASE_REVISION, baseline_plugin_sha256=base.BASE_SHA256,
        runtime_cudf_revision="69866c121b4cf2d48b6ef631025292547cde528f",
        runtime_baseline=json.loads((args.native_build / "build-result.json").read_text()),
        replaced_entries=sorted(replacements), remaining_unverified=["GPU gates", "large proof"])
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"prepared": True, "manifest": str(out / "manifest.json")}))

if __name__ == "__main__":
    main()
