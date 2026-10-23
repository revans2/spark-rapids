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
"""Prepare isolated PCBS research artifacts; never change installed Spark or plugin sources."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import time
import zipfile

BASE_REVISION = "3cdf711ded8e8b72d0f3ee92866401df5d9d2a06"
BASE_SHA256 = "f3bc760d51c1c8596ac9108242f2c9366f570d7b5612a1239aba88e73ee25af5"
PCBS = "sql-plugin/src/main/scala/com/nvidia/spark/rapids/parquet/ParquetCachedBatchSerializer.scala"
SCAN = "sql-plugin/src/main/scala/org/apache/spark/sql/rapids/GpuInMemoryTableScanExec.scala"
COALESCE = "sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuCoalesceBatches.scala"
ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def replace_once(text, before, after):
    assert text.count(before) == 1, "Unexpected frozen source anchor: " + before[:100]
    return text.replace(before, after, 1)


def command(argv, cwd=None, log=None):
    if log is None:
        return subprocess.check_output(argv, cwd=cwd, universal_newlines=True)
    log.with_suffix(".command.json").write_text(json.dumps({
        "argv": list(map(str, argv)), "cwd": str(cwd) if cwd else None}, indent=2) + "\n")
    with log.open("w") as stream:
        subprocess.run(argv, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT,
                       check=True, timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-home", type=Path, required=True)
    parser.add_argument("--baseline-plugin", type=Path, required=True)
    parser.add_argument("--spark-codec-overlay", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--include-baseline", action="store_true")
    args = parser.parse_args()
    assert sha(args.baseline_plugin) == BASE_SHA256, "Frozen baseline plugin mismatch"
    assert args.spark_codec_overlay.is_file()
    jars = sorted((args.spark_home / "assembly/target/scala-2.12/jars").glob("*.jar"))
    assert jars and any(p.name == "spark-core_2.12-3.5.5.jar" for p in jars)
    out = args.output_dir
    out.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    (out / "preparation-inputs.json").write_text(json.dumps({
        "baseline_plugin": str(args.baseline_plugin), "baseline_sha256": BASE_SHA256,
        "baseline_revision": BASE_REVISION, "spark_home": str(args.spark_home),
        "spark_codec_overlay": str(args.spark_codec_overlay),
        "spark_codec_overlay_sha256": sha(args.spark_codec_overlay),
        "script_sha256": sha(Path(__file__)),
        "include_baseline": args.include_baseline}, indent=2) + "\n")
    source = out / "sources"
    source.mkdir()
    for name in ("PcbsRecordCodec.scala", "prepare_gpu_overlay.py"):
        shutil.copyfile(Path(__file__).with_name(name), source / name)
    marker = ROOT / "tools/pcbs-blockmanager-codec-spike/SqlCacheCodecProvider.scala"
    shutil.copyfile(marker, source / marker.name)
    original_pcbs = command(["git", "show", BASE_REVISION + ":" + PCBS], cwd=ROOT)
    original_scan = command(["git", "show", BASE_REVISION + ":" + SCAN], cwd=ROOT)
    original_coalesce = command(["git", "show", BASE_REVISION + ":" + COALESCE], cwd=ROOT)
    (source / "baseline-coalesce.scala").write_text(original_coalesce)
    (source / "baseline-PCBS.scala").write_text(original_pcbs)
    (source / "baseline-scan.scala").write_text(original_scan)
    # Extract only compile dependencies. This is not a runtime overlay or a source checkout.
    dependency_classes = out / "plugin-compile-classes"
    dependency_classes.mkdir()
    with zipfile.ZipFile(args.baseline_plugin) as archive:
        for name in archive.namelist():
            for prefix in ("spark-shared/", "spark355/"):
                if name.startswith(prefix) and name.endswith(".class"):
                    dest = dependency_classes / name[len(prefix):]
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    dest.write_bytes(archive.read(name))
                    break
    cp = ":".join(map(str, [args.spark_codec_overlay, args.baseline_plugin,
                            dependency_classes] + jars))
    fixture_classes = out / "fixture-classes"
    fixture_classes.mkdir()
    fixture_compile = ["java", "-Xmx2g", "-cp", cp, "scala.tools.nsc.Main",
                       "-classpath", cp, "-d", str(fixture_classes),
                       str(source / "PcbsRecordCodec.scala")]
    command(fixture_compile, log=out / "compile-fixture.log")
    fixture = out / "pcbs-record-codec.jar"
    command(["jar", "cf", str(fixture), "-C", str(fixture_classes), "."],
            log=out / "package-fixture.log")
    cap_pcbs = replace_once(original_pcbs,
        "  private def getGpuBytesAllowedPerBatch(conf: SQLConf): Long =\n"
        "    math.min(getBytesAllowedPerBatch(conf), maxGpuBytesAllowedPerBatch)",
        "  private def getGpuBytesAllowedPerBatch(conf: SQLConf): Long = {\n"
        "    // Research overlay: reserve metadata within a private 32 MiB cache-record budget.\n"
        "    val cacheTargetBytes = 32L * 1024 * 1024\n"
        "    val cacheBytesAllowed = (cacheTargetBytes - cacheTargetBytes * 0.5 / 100).toLong\n"
        "    math.min(math.min(getBytesAllowedPerBatch(conf), maxGpuBytesAllowedPerBatch),\n"
        "      cacheBytesAllowed)\n"
        "  }")
    # Observe only decoded batches, with caller ownership unchanged even if observation fails.
    observed_scan = replace_once(original_scan,
        "      numOutputRows += cb.numRows()\n      cb",
        "      com.nvidia.spark.rapids.Arm.closeOnExcept(cb) { batch =>\n"
        "        org.apache.spark.sql.pcbsrealdata.PcbsCodecCounters.recordDecoded(\n"
        "          batch.numRows().toLong,\n"
        "          com.nvidia.spark.rapids.GpuColumnVector.getTotalDeviceMemoryUsed(batch))\n"
        "        numOutputRows += batch.numRows()\n"
        "        batch\n"
        "      }")
    observed_coalesce = replace_once(original_coalesce,
        "    val batches = child.executeColumnar()",
        "    val observeCachedOutput =\n"
        "      child.isInstanceOf[org.apache.spark.sql.rapids.GpuInMemoryTableScanExec]\n"
        "    val batches = child.executeColumnar()")
    observed_coalesce = replace_once(observed_coalesce,
        '              localCodecConfigs)\n          }\n        case batchingGoal: BatchedByKey =>',
        '              localCodecConfigs).map { cb =>\n'
        '              if (observeCachedOutput) {\n'
        '                com.nvidia.spark.rapids.Arm.closeOnExcept(cb) { batch =>\n'
        '                  org.apache.spark.sql.pcbsrealdata.PcbsCodecCounters.recordCombined(\n'
        '                    batch.numRows().toLong,\n'
        '                    GpuColumnVector.getTotalDeviceMemoryUsed(batch))\n'
        '                  batch\n'
        '                }\n'
        '              } else {\n'
        '                cb\n'
        '              }\n'
        '            }\n'
        '          }\n        case batchingGoal: BatchedByKey =>')
    variants = {}
    # The baseline variant changes observation only, never the original PCBS split policy.
    requested = [("cap32-only", True, False), ("cap32-combine", True, True)]
    if args.include_baseline:
        requested.insert(0, ("baseline-codec", False, False))
    for name, cap, combine in requested:
        variant_source = source / name
        variant_source.mkdir()
        pcbs_path = variant_source / "ParquetCachedBatchSerializer.scala"
        scan_path = variant_source / "GpuInMemoryTableScanExec.scala"
        pcbs_path.write_text(cap_pcbs if cap else original_pcbs)
        scan_text = observed_scan
        if combine:
            scan_text = replace_once(scan_text,
                "  override def output: Seq[Attribute] = attributes",
                "  // Research overlay: reuse the normal transition/coalescing rule.\n"
                "  override def coalesceAfter: Boolean = true\n\n"
                "  override def output: Seq[Attribute] = attributes")
        scan_path.write_text(scan_text)
        coalesce_path = variant_source / "GpuCoalesceBatches.scala"
        coalesce_path.write_text(observed_coalesce)
        classes = out / (name + "-classes")
        classes.mkdir()
        # The baseline needs only the scan observation class, leaving PCBS bytecode untouched.
        compile_sources = [scan_path, coalesce_path] + ([pcbs_path] if cap else [])
        compile_argv = ["java", "-Xmx3g", "-cp", cp, "scala.tools.nsc.Main",
                        "-classpath", str(fixture) + ":" + cp, "-d", str(classes)]
        compile_argv += list(map(str, compile_sources))
        command(compile_argv, log=out / ("compile-" + name + ".log"))
        replacements = {}
        with zipfile.ZipFile(args.baseline_plugin) as original:
            original_names = set(original.namelist())
            for path in sorted(classes.rglob("*.class")):
                relative = path.relative_to(classes).as_posix()
                entry = "spark-shared/" + relative
                assert entry in original_names, "Unexpected class placement: " + entry
                replacements[entry] = path
            artifact = out / (name + ".jar")
            with zipfile.ZipFile(artifact, "w") as target:
                for item in original.infolist():
                    if item.filename not in replacements:
                        with original.open(item) as src, target.open(item, "w") as dest:
                            shutil.copyfileobj(src, dest, length=1024 * 1024)
                for entry, path in replacements.items():
                    target.write(path, entry, compress_type=zipfile.ZIP_DEFLATED)
        variants[name] = {
            "plugin": str(artifact), "plugin_sha256": sha(artifact),
            "cap_mib": 32 if cap else None, "coalesce_after": combine,
            "compile_argv": compile_argv,
            "sources": {str(path): sha(path) for path in (pcbs_path, scan_path, coalesce_path)},
            "replaced_entries": sorted(replacements)}
    manifest = {
        "research_only": True, "spark_version": "3.5.5", "scala_version": "2.12",
        "baseline_revision": BASE_REVISION,
        "baseline_plugin": str(args.baseline_plugin), "baseline_plugin_sha256": BASE_SHA256,
        "spark_codec_overlay": str(args.spark_codec_overlay),
        "spark_codec_overlay_sha256": sha(args.spark_codec_overlay),
        "fixture": str(fixture), "fixture_sha256": sha(fixture),
        "serializer": "org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer",
        "snapshot_class": "org.apache.spark.sql.pcbsrealdata.PcbsCodecSnapshot",
        "fixed_global_batch_bytes": 1073741824,
        "fixture_compile_argv": fixture_compile,
        "sources": {str(path): sha(path) for path in source.glob("*") if path.is_file()},
        "runtime_jars": {str(path): sha(path) for path in jars},
        "variants": variants, "elapsed_s": time.monotonic() - started,
        "remaining_unverified": ["GPU class identity/disk roundtrip", "coalescer plan placement",
                                 "actual post-coalescer batch sizes", "large-data behavior"]}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"valid_preparation": True, "manifest": str(out / "manifest.json")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
