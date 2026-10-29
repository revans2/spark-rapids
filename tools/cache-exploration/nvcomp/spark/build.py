#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Compile the isolated Spark3.5.5 GPU cache prototype without modifying Maven targets."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import zipfile

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
SPARK = Path("/home/roberte/src/spark_3.5.5/dist")

def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()

def artifact(path):
    result = {"path": str(path), "sha256": digest(path)}
    with zipfile.ZipFile(path) as archive:
        result["version_properties"] = {
            name: archive.read(name).decode("utf-8", errors="replace")
            for name in archive.namelist() if name.endswith("version-info.properties")}
    return result

def validate_source_build(path, expected_head):
    """Reject unverified or stale plugin inputs before compiling or launching anything."""
    path = path.resolve(strict=True)
    proof_hash = digest(path)
    proof = json.loads(path.read_text())
    if proof.get("source_clean") is not True or type(proof.get("returncode")) is not int or \
            proof["returncode"] != 0:
        raise RuntimeError("Source build must have succeeded from clean tracked sources")
    command = proof.get("command")
    if not isinstance(command, list) or not command or \
            not all(isinstance(part, str) and part for part in command):
        raise RuntimeError("Source build command is missing")
    root = Path(proof["source_root"])
    if not root.is_absolute() or not root.is_dir():
        raise RuntimeError("Source build root must be an existing absolute directory")
    expected_head = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "--verify", expected_head + "^{commit}"],
        text=True).strip()
    head = subprocess.check_output(
        ["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
        text=True).strip()
    if expected_head != proof["source_head"]:
        raise RuntimeError("Source build proof does not match the expected source commit")
    if head != expected_head or dirty:
        raise RuntimeError("Source build checkout changed since the recorded build")
    for name in ["build_log", "aggregator", "jni", "private"]:
        entry = proof[name]
        artifact_path = Path(entry["path"])
        if not artifact_path.is_absolute() or digest(artifact_path) != entry["sha256"]:
            raise RuntimeError("Changed source build " + name + ": " + str(artifact_path))
    expected_aggregator = (root / "aggregator/target/spark355/"
                          "rapids-4-spark-aggregator_2.12-26.12.0-SNAPSHOT-spark355.jar")
    if Path(proof["aggregator"]["path"]).resolve() != expected_aggregator.resolve():
        raise RuntimeError("Aggregator must be the recorded source worktree build output")
    prefix = "com/nvidia/spark/rapids/parquet/"
    with zipfile.ZipFile(proof["aggregator"]["path"]) as archive:
        required = [prefix + name + ".class" for name in [
            "ParquetCachedBatchSerializer", "ParquetCachedBatchReader"]]
        if any(name not in archive.namelist() for name in required):
            raise RuntimeError("Source aggregator is missing the current PCBS reader/serializer")
        if b"ParquetCachedBatchReader" not in archive.read(required[0]):
            raise RuntimeError("Source aggregator serializer does not reference PCBS reader")
    with zipfile.ZipFile(proof["aggregator"]["path"]) as aggregator, \
            zipfile.ZipFile(proof["private"]["path"]) as private, \
            zipfile.ZipFile(proof["jni"]["path"]) as jni:
        private_metadata = "cudf-spark-private-version-info.properties"
        if aggregator.read(private_metadata) != private.read(private_metadata):
            raise RuntimeError("Aggregator private build metadata differs from pinned private jar")
        relocations = {"com/google/flatbuffers": "com/nvidia/shaded/spark/com/google/flatbuffers",
                       "org/roaringbitmap": "com/nvidia/shaded/spark/org/roaringbitmap"}
        mismatches = []
        private_classes = [name for name in private.namelist() if name.endswith(".class")]
        if not private_classes:
            raise RuntimeError("Pinned private jar contains no classes")
        for name in private_classes:
            expected_name = name
            for original, relocated in relocations.items():
                if name.startswith(original + "/"):
                    expected_name = relocated + name[len(original):]
                    break
            original_bytes = private.read(name)
            if expected_name not in aggregator.namelist():
                mismatches.append(name + " (missing " + expected_name + ")")
            elif aggregator.read(expected_name) != original_bytes:
                shaded_refs = [original for original in relocations
                               if original.encode() in original_bytes]
                context = ("; references known relocation " + ",".join(shaded_refs)
                           if shaded_refs else "")
                mismatches.append(name + " (class bytes differ" + context + ")")
        if mismatches:
            raise RuntimeError("Pinned private class verification failed: " + "; ".join(mismatches))
        jni_payload = {name for name in jni.namelist()
                       if name.endswith((".class", ".so", ".dll", ".dylib")) or ".so." in name}
        overlap = sorted(jni_payload.intersection(aggregator.namelist()))
        if overlap:
            raise RuntimeError("Aggregator includes excluded JNI class/native entries: " +
                               ", ".join(overlap))
    if digest(path) != proof_hash:
        raise RuntimeError("Source build proof changed during verification")
    return {"path": str(path), "sha256": proof_hash, "expected_head": expected_head,
            "proof": proof}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-build", type=Path, required=True,
                        help="Successful current-source Maven build proof JSON")
    parser.add_argument("--source-head", required=True,
                        help="Expected plugin source commit, resolved in the source build worktree")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error("Output must be a new directory")
    source_build = validate_source_build(args.source_build, args.source_head)
    proof = source_build["proof"]
    dependencies = [Path(proof[name]["path"]) for name in ["jni", "aggregator"]]
    subprocess.run(["python3.9", str(HERE.parent / "native/build.py"),
                    "--output", str(output), "--cudf-jar", str(dependencies[0])], check=True)
    subprocess.run(["python3.9", str(HERE.parent / "native/host_build.py"),
                    "--output", str(output / "host"), "--cudf-jar", str(dependencies[0])],
                   check=True)
    spark_jars = sorted(p for p in (SPARK / "jars").glob("*.jar") if p.is_file())
    if not spark_jars:
        raise RuntimeError("Need built Spark3.5.5 distribution")
    classes = output / "classes"
    classpath = os.pathsep.join(map(str, [classes] + dependencies + spark_jars))
    java_sources = [HERE.parent / name for name in [
        "CascadedCodec.java", "ByteCodec.java", "PackedSegments.java", "PackedTableCodec.java"]]
    java_sources += sorted(HERE.glob("*.java"))
    subprocess.run(["javac", "-Xlint:all", "-cp", classpath, "-d", str(classes)]
                   + list(map(str, java_sources)), check=True)
    compiler = os.pathsep.join(str(p) for p in spark_jars
                              if p.name.startswith(("scala-compiler-", "scala-library-", "scala-reflect-")))
    scala_sources = sorted(HERE.glob("*.scala"))
    subprocess.run(["java", "-Dscala.usejavacp=true", "-cp", compiler, "scala.tools.nsc.Main",
                    "-deprecation", "-feature", "-unchecked", "-classpath", classpath,
                    "-d", str(classes)] + list(map(str, scala_sources)), check=True)
    jar = output / "nvcomp-spark-exploration.jar"
    subprocess.run(["jar", "cf", str(jar), "-C", str(classes), "."], check=True)
    sources = java_sources + scala_sources + [Path(__file__),
        REPO / "tools/cache-exploration/spark_cache_bench.py",
        REPO / "tools/cache-exploration/measure_process.py", HERE.parent / "run.py"]
    for name in ["benchmark.py", "run.py", "profile.py", "cpu_profile.py"]:
        path = HERE / name
        if path.exists():
            sources.append(path)
    if validate_source_build(args.source_build, args.source_head) != source_build:
        raise RuntimeError("Source build proof changed while compiling the experiment")
    manifest = {
        "scope": "Experimental local Spark3.5.5/Scala2.12 CPU/GPU packed cache; int/long/string only",
        "git_head": subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip(),
        "git_head_scope": "Experiment checkout; plugin source revision is recorded in source_build",
        "source_build": source_build,
        "application": artifact(jar),
        "plugin_dependencies": [artifact(p) for p in dependencies],
        "spark_home": str(SPARK),
        "spark_dependencies": [artifact(p) for p in spark_jars],
        "sources": {str(p): digest(p) for p in sources},
        "native_library": str(output / "libcascaded_exploration.so"),
        "host_library": str(output / "host/libhost_packed.so"),
        "host_build_manifest": digest(output / "host/host-build.json"),
        "native_build_manifest": digest(output / "build-provenance.json"),
    }
    (output / "spark-provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "plugin-jars.txt").write_text(",".join(map(str, dependencies)) + "\n")
    print(json.dumps({"application": str(jar), "provenance": str(output / "spark-provenance.json")}))
if __name__ == "__main__":
    main()
