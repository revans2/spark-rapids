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

"""Run actual branch PCBS GPU framing/decoding in isolation from Spark cache storage."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import zipfile


def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def properties(archive, member):
    with zipfile.ZipFile(archive) as source:
        text = source.read(member).decode("utf-8")
    return dict(line.split("=", 1) for line in text.splitlines()
                if line and not line.startswith("#") and "=" in line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cudf-jar", type=Path, required=True)
    parser.add_argument("--private-jar", type=Path,
                        help="Matching spark355 private dependency; defaults beside the cuDF Maven artifact")
    parser.add_argument("--spark-home", type=Path,
                        default=Path("/home/roberte/src/spark_3.5.5"))
    parser.add_argument("--plugin-version", default="26.12.0-SNAPSHOT")
    parser.add_argument("--expected-revision",
                        help="Require all plugin jars to identify this revision (default: HEAD)")
    parser.add_argument("--build-dir", type=Path, default=Path("/data/tmp/pcbs-codec-harness"))
    parser.add_argument("--compile-only", action="store_true")
    parser.add_argument("--fixture", choices=["fixed", "strings-low", "strings-wide"],
                        default="fixed")
    parser.add_argument("--rows", type=int, default=65536, help="Rows in the parent GPU input")
    parser.add_argument("--record-target-bytes", type=int, default=8 * 1024 * 1024)
    parser.add_argument("--gpu-target-bytes", type=int, default=1024 * 1024 * 1024,
                        help="PCBS input slicing / read-bundling target, independent of record target")
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--heap", default="1g")
    args = parser.parse_args()
    source = Path(__file__).resolve().with_name("PcbsCodecBench.java")
    repo = source.parents[2]
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip()
    expected = args.expected_revision or head
    modules = ["sql-plugin-format", "sql-plugin-fileio", "sql-plugin-columnar",
               "sql-plugin-api", "sql-plugin"]
    jars = []
    provenance = {"scope": "actual PCBS GPU codec/framing; no Spark cache storage",
                  "head": head, "expected_revision": expected, "artifacts": [],
                  "source_sha256": digest(source), "arguments": vars(args).copy()}
    for module in modules:
        artifact = "sql" if module == "sql-plugin" else module
        name = "rapids-4-spark-" + artifact + "_2.12-" + args.plugin_version + "-spark355.jar"
        jar = (repo / module / "target" / "spark355" / name).resolve(strict=True)
        info = properties(jar, "rapids4spark-version-info.properties")
        if info.get("revision") != expected:
            parser.error(str(jar) + " identifies " + str(info.get("revision"))
                         + "; rebuild with buildver=355 for revision " + expected)
        jars.append(jar)
        provenance["artifacts"].append({"path": str(jar), "sha256": digest(jar),
                                        "build_info": info})
    cudf = args.cudf_jar.resolve(strict=True)
    private = args.private_jar or (cudf.parents[2] / "cudf-spark-private_2.12"
                                  / args.plugin_version
                                  / ("cudf-spark-private_2.12-" + args.plugin_version
                                     + "-spark355.jar"))
    private = private.resolve(strict=True)
    provenance["private_dependency"] = {
        "path": str(private), "sha256": digest(private),
        "build_info": properties(private, "cudf-spark-private-version-info.properties")
    }
    spark = args.spark_home.resolve(strict=True)
    spark_jars = spark / "jars"
    if not spark_jars.is_dir():
        spark_jars = spark / "dist" / "jars"
    if not spark_jars.is_dir():
        parser.error("No Spark dependency jars found under " + str(spark))
    provenance["cudf"] = {"path": str(cudf), "sha256": digest(cudf),
                          "build_info": properties(cudf, "cudf-spark-jni-version-info.properties")}
    args.build_dir.mkdir(parents=True, exist_ok=True)
    # No Vortex/Arrow19 jars: use Spark's own dependency set and fresh flat single-shim jars.
    classpath = os.pathsep.join([str(cudf), str(private)] + [str(jar) for jar in jars]
                               + [str(spark_jars / "*")])
    compile_command = ["javac", "-encoding", "UTF-8", "--release", "17", "-cp", classpath,
                       "-d", str(args.build_dir), str(source)]
    run_command = ["java", "-ea", "-Xmx" + args.heap,
                   "--add-opens=java.base/java.nio=ALL-UNNAMED",
                   "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED", "-cp",
                   str(args.build_dir) + os.pathsep + classpath, "PcbsCodecBench", args.fixture,
                   str(args.rows), str(args.warmups), str(args.repeats),
                   str(args.record_target_bytes), str(args.gpu_target_bytes)]
    provenance["compile_command"] = compile_command
    provenance["run_command"] = run_command
    provenance_path = args.build_dir / "provenance.json"
    provenance_path.write_text(json.dumps(provenance, indent=2, default=str) + "\n")
    print("PCBS artifact provenance: " + str(provenance_path), file=sys.stderr, flush=True)
    subprocess.run(compile_command, check=True)
    if not args.compile_only:
        print("Scope: actual branch PCBS GPU codec/framing; no Spark storage/pressure claim",
              file=sys.stderr, flush=True)
        subprocess.run(run_command, check=True)


if __name__ == "__main__":
    main()
