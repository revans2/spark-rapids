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
CUDF = Path("/home/roberte/.m2/repository/com/nvidia/cudf-spark-jni/26.12.0-SNAPSHOT/"
            "cudf-spark-jni-26.12.0-SNAPSHOT-cuda12.jar")
PRIVATE = Path("/home/roberte/.m2/repository/com/nvidia/cudf-spark-private_2.12/26.12.0-SNAPSHOT/"
               "cudf-spark-private_2.12-26.12.0-SNAPSHOT-spark355.jar")
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

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error("Output must be a new directory")
    subprocess.run(["python3.9", str(HERE.parent / "native/build.py"),
                    "--output", str(output)], check=True)
    plugin = sorted(REPO.glob("sql-plugin*/target/spark355/*-26.12.0-SNAPSHOT-spark355.jar"))
    plugin += sorted((REPO / "dist/target/deps").glob("*.jar"))
    plugin += [
        REPO / "udf-compiler/target/spark355/rapids-4-spark-udf_2.12-26.12.0-SNAPSHOT-spark355.jar",
        REPO / "delta-lake/delta-33x/target/spark355/rapids-4-spark-delta-33x_2.12-26.12.0-SNAPSHOT-spark355.jar"]
    plugin = list(dict.fromkeys(p.resolve(strict=True) for p in plugin))
    dependencies = [CUDF, PRIVATE] + plugin
    spark_jars = sorted(p for p in (SPARK / "jars").glob("*.jar") if p.is_file())
    if not spark_jars or not plugin:
        raise RuntimeError("Need built Spark3.5.5 distribution and raw plugin jars")
    classes = output / "classes"
    classpath = os.pathsep.join(map(str, [classes] + dependencies + spark_jars))
    java_sources = [HERE.parent / name for name in [
        "CascadedCodec.java", "PackedSegments.java", "PackedTableCodec.java"]]
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
    for name in ["benchmark.py", "run.py"]:
        path = HERE / name
        if path.exists():
            sources.append(path)
    manifest = {
        "scope": "Experimental real local Spark3.5.5/Scala2.12 GPU CachedBatchSerializer; CPU paths throw",
        "git_head": subprocess.check_output(["git", "-C", str(REPO), "rev-parse", "HEAD"], text=True).strip(),
        "application": artifact(jar),
        "plugin_dependencies": [artifact(p) for p in dependencies],
        "spark_home": str(SPARK),
        "spark_dependencies": [artifact(p) for p in spark_jars],
        "sources": {str(p): digest(p) for p in sources},
        "native_library": str(output / "libcascaded_exploration.so"),
        "native_build_manifest": digest(output / "build-provenance.json"),
    }
    (output / "spark-provenance.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (output / "plugin-jars.txt").write_text(",".join(map(str, dependencies)) + "\n")
    print(json.dumps({"application": str(jar), "provenance": str(output / "spark-provenance.json")}))
if __name__ == "__main__":
    main()
