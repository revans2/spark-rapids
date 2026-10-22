#!/usr/bin/env python3
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
"""Compile the primary heap-codec proof against exact Spark 4.2.0 classes.

This deliberately makes no runtime claim: two unavailable transitive libraries
are absent from the supplied compile-only dependency graph, without substitutes.
No native storage or installed Spark/source modification is performed.
"""
import argparse
import difflib
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

REVISION = "32f7299601108917fb01920a54e084595b7b3bf8"
RELATIVES = (
    "core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala",
    "sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala",
)
FIXTURES = (
    "BlockManagerCodecSpike.scala", "CacheCodecContext.scala",
    "SqlCacheCodecProvider.scala", "StorageRouteChecks.scala",
)
MISSING = (
    "org.roaringbitmap:RoaringBitmap:1.6.10",
    "com.google.flatbuffers:flatbuffers-java:25.2.10",
)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError("Expected exactly one source anchor: " + old)
    return text.replace(old, new)


def verify(output):
    manifest = json.loads((output / "manifest.json").read_text())
    if digest(output / "compile_spike_420.py") != manifest["adapter_sha256"]:
        raise ValueError("Changed retained adapter")
    for group in ("original_sources", "adapted_sources", "compile_jars"):
        for path, expected in manifest[group].items():
            if digest(path) != expected:
                raise ValueError("Changed input: " + path)
    if digest(output / "codec-spike-420.jar") != manifest["artifact_sha256"]:
        raise ValueError("Changed compiled artifact")
    log = (output / "compile.log").read_text()
    result = {
        "valid": manifest["compiler_exit"] == 0,
        "compile_pass": manifest["compiler_exit"] == 0,
        "runtime_tested": False,
        "status": "COMPILE_PASS_RUNTIME_BLOCKED",
        "spark_version": "4.2.0",
        "spark_revision": REVISION,
        "missing_runtime_dependencies": list(MISSING),
        "checked_hashes": sum(len(manifest[g]) for g in (
            "original_sources", "adapted_sources", "compile_jars")) + 2,
        "compile_log_sha256": digest(output / "compile.log"),
        "compiler_errors": "error:" in log,
    }
    if result["compiler_errors"]:
        raise ValueError("Compiler errors in retained log")
    (output / "verifier.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-git", type=Path)
    parser.add_argument("--classpath-file", type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if args.verify_only:
        print(json.dumps(verify(output)))
        return
    if args.source_git is None or args.classpath_file is None:
        parser.error("--source-git and --classpath-file are required to compile")
    output.mkdir(parents=True, exist_ok=False)
    shared = Path(__file__).resolve().parents[2]
    originals = output / "original"
    adapted = output / "adapted"
    originals.mkdir()
    adapted.mkdir()
    jars = [Path(p) for p in args.classpath_file.read_text().strip().split(":")]
    if not jars or not all(p.is_file() for p in jars):
        raise ValueError("All supplied compile jars must exist")
    spark_jars = [p for p in jars if p.name.startswith("spark-")]
    if not spark_jars or any(not p.name.endswith("_2.13-4.2.0.jar") for p in spark_jars):
        raise ValueError("Only matching Spark 4.2.0 modules are allowed")
    common = next(p for p in spark_jars if p.name.startswith("spark-common-utils_"))
    with zipfile.ZipFile(common) as archive:
        version = archive.read("spark-version-info.properties").decode()
    if "version=4.2.0\n" not in version or "revision=" + REVISION not in version:
        raise ValueError("Target binary does not match the pinned source revision")
    (output / "spark-version-info.properties").write_text(version)

    source_hashes = {}
    patch = []
    for relative in RELATIVES:
        content = subprocess.check_output(
            ["git", "show", REVISION + ":" + relative], cwd=args.source_git).decode()
        original = originals / Path(relative).name
        original.write_text(content)
        source_hashes[str(original)] = digest(original)
        if relative.endswith("SerializerManager.scala"):
            changed = replace_once(content, "    if (autoPick && canUseKryo(ct)) {",
                "    if (ct.isInstanceOf[CacheCodecClassTag[_]]) {\n"
                "      ct.asInstanceOf[CacheCodecClassTag[_]].codec\n"
                "    } else if (autoPick && canUseKryo(ct)) {")
        else:
            if "PartitionKeyedAccumulator" not in content:
                raise ValueError("Target must retain partition-keyed materialization")
            changed = replace_once(content,
                "    val cached = cb.mapPartitionsInternal { it =>",
                "    val cacheTag: scala.reflect.ClassTag[CachedBatch] = serializer match {\n"
                "      case provider: SqlCacheCodecProvider =>\n"
                "        new org.apache.spark.serializer.CacheCodecClassTag[CachedBatch](\n"
                "          scala.reflect.classTag[CachedBatch], provider.cacheCodec(cachedPlan.output))\n"
                "      case _ => scala.reflect.classTag[CachedBatch]\n"
                "    }\n"
                "    val cached = cb.mapPartitionsInternal({ it =>")
            changed = replace_once(changed, "    }.persist(storageLevel)",
                                   "    })(cacheTag).persist(storageLevel)")
        (adapted / original.name).write_text(changed)
        patch.extend(difflib.unified_diff(content.splitlines(True), changed.splitlines(True),
                                         fromfile="a/" + relative, tofile="b/" + relative))
    (output / "spark-research.patch").write_text("".join(patch))

    for name in FIXTURES:
        shutil.copyfile(shared / name, originals / name)
        source_hashes[str(originals / name)] = digest(originals / name)
        text = (originals / name).read_text()
        if name == "BlockManagerCodecSpike.scala":
            text = replace_once(text, "import org.apache.spark.sql.SparkSession",
                                "import org.apache.spark.sql.classic.SparkSession")
            text = replace_once(text,
                "StorageLevel.DISK_ONLY, spark.range(100).queryExecution.executedPlan, None)",
                "StorageLevel.DISK_ONLY, spark.range(100).queryExecution.executedPlan, None,\n"
                "      spark.range(100).queryExecution.logical)")
            text = replace_once(text, "relation.cacheBuilder.sizeInBytesStats.value",
                                "relation.cacheBuilder.materializedSizeInBytes")
            text = replace_once(text, "relation.cacheBuilder.rowCountStats.value",
                                "relation.cacheBuilder.materializedRowCount")
        (adapted / name).write_text(text)
    (output / "compile_spike_420.py").write_bytes(Path(__file__).read_bytes())
    classes = output / "classes"
    classes.mkdir()
    cp = ":".join(map(str, jars))
    command = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
               "-d", str(classes)] + [str(p) for p in sorted(adapted.glob("*.scala"))]
    with (output / "compile.log").open("w") as log:
        completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    if completed.returncode:
        raise SystemExit("Compilation failed; inspect " + str(output / "compile.log"))
    artifact = output / "codec-spike-420.jar"
    subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
    manifest = {
        "spark_version": "4.2.0", "spark_revision": REVISION,
        "compiler_exit": completed.returncode, "compile_argv": command,
        "original_sources": source_hashes,
        "adapted_sources": {str(p): digest(p) for p in adapted.iterdir()},
        "compile_jars": {str(p): digest(p) for p in jars},
        "artifact_sha256": digest(artifact),
        "adapter_sha256": digest(Path(__file__)),
        "partition_keyed_stats_preserved": True,
        "runtime_tested": False, "missing_runtime_dependencies": list(MISSING),
        "reason": "Exact jars absent from Maven/Coursier/Ivy caches; Maven hosts fail DNS.",
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    result = verify(output)
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
