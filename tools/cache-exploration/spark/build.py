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

"""Compile the Spark 3.5.5-only experiment outside repository build profiles."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--vortex-classpath", type=Path, required=True)
parser.add_argument("--native-lib", type=Path, required=True)
parser.add_argument("--cudf-jar", type=Path, required=True)
parser.add_argument("--private-jar", type=Path)
parser.add_argument("--plugin-version", default="26.12.0-SNAPSHOT")
parser.add_argument("--spark-home", type=Path,
                    default=Path("/home/roberte/src/spark_3.5.5"))
parser.add_argument("--build-dir", type=Path, default=Path("/data/tmp/vortex-spark-prototype"))
args = parser.parse_args()
source = Path(__file__).resolve().parent
repository = source.parents[2]
build = args.build_dir.resolve()
parent = build / "classes"
child = build / "codec"
parent.mkdir(parents=True, exist_ok=True)
child.mkdir(parents=True, exist_ok=True)
vortex_cp = args.vortex_classpath.read_text().strip()
subprocess.run(["javac", "--release", "17", "-encoding", "UTF-8", "-cp", vortex_cp,
                "-d", str(child), str(source / "VortexIpcCodec.java"),
                str(source / "VortexLifecycleProbe.java")], check=True)
native = child / "native" / "linux-amd64" / "libvortex_jni.so"
native.parent.mkdir(parents=True, exist_ok=True)
shutil.copyfile(args.native_lib, native)
(parent / "vortex-child-classpath.txt").write_text(str(child) + os.pathsep + vortex_cp)
spark_jars = sorted((args.spark_home / "dist" / "jars").glob("*.jar"))
plugin_jars = sorted(repository.glob(
    "sql-plugin*/target/spark355/*-" + args.plugin_version + "-spark355.jar"))
udf = repository / "udf-compiler" / "target" / "spark355" / (
    "rapids-4-spark-udf_2.12-" + args.plugin_version + "-spark355.jar")
plugin_jars.append(udf.resolve(strict=True))
delta = repository / "delta-lake" / "delta-33x" / "target" / "spark355" / (
    "rapids-4-spark-delta-33x_2.12-" + args.plugin_version + "-spark355.jar")
plugin_jars.append(delta.resolve(strict=True))
if not plugin_jars or not spark_jars:
    raise SystemExit("Need rebuilt raw Spark355 plugin/API jars and Spark dist/jars")
private = args.private_jar or (args.cudf_jar.resolve().parents[2] / "cudf-spark-private_2.12"
                                / args.plugin_version / ("cudf-spark-private_2.12-"
                                + args.plugin_version + "-spark355.jar"))
private = private.resolve(strict=True)
classpath = os.pathsep.join(map(str, [args.cudf_jar, private] + plugin_jars + spark_jars))
compiler = os.pathsep.join(
    str(path) for path in spark_jars
    if path.name.startswith(("scala-compiler-", "scala-library-", "scala-reflect-")))
scala_sources = [
    str(source / name) for name in
    ["SparkVortexSerializer.scala", "SparkCacheProbe.scala", "CachePayloadAudit.scala"]]
subprocess.run(["java", "-Dscala.usejavacp=true", "-cp", compiler, "scala.tools.nsc.Main",
                "-classpath", classpath, "-d", str(parent)] + scala_sources, check=True)
jar = build / "vortex-spark-exploration.jar"
subprocess.run(["jar", "cf", str(jar), "-C", str(parent), "."], check=True)
(build / "plugin-jars.txt").write_text(",".join(map(str, [args.cudf_jar, private] + plugin_jars)))
def digest(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def classpath_entry(path):
    path = path.resolve(strict=True)
    if path.is_dir():
        files = [{"relative_path": str(file.relative_to(path)),
                  "size_bytes": file.stat().st_size, "sha256": digest(file)}
                 for file in sorted(path.rglob("*")) if file.is_file()]
        # Include names and contents so additions/removals are detected at launch.
        manifest = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return {"path": str(path), "kind": "directory",
                "sha256": hashlib.sha256(manifest).hexdigest(), "files": files}
    return {"path": str(path), "kind": "file",
            "size_bytes": path.stat().st_size, "sha256": digest(path)}


def artifact(path):
    path = path.resolve(strict=True)
    info = {"path": str(path), "sha256": digest(path)}
    with zipfile.ZipFile(path) as archive:
        for entry in ["rapids4spark-version-info.properties",
                      "cudf-spark-private-version-info.properties",
                      "cudf-spark-jni-version-info.properties"]:
            if entry in archive.namelist():
                info[entry] = archive.read(entry).decode()
    return info

provenance = {
    "scope": "local Spark 3.5.5 exploratory cache prototype",
    "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository,
                                        text=True).strip(),
    "arguments": vars(args),
    "sources": [{"path": str(path), "sha256": digest(path)}
                for path in sorted(source.iterdir()) if path.is_file()],
    "application": artifact(jar),
    "plugin_dependencies": [artifact(path) for path in [args.cudf_jar, private] + plugin_jars],
    "vortex_native": {"path": str(args.native_lib.resolve()),
                      "sha256": digest(args.native_lib)},
    "child_classpath": str(child) + os.pathsep + vortex_cp,
    "child_classpath_entries": [classpath_entry(Path(entry)) for entry in
                                (str(child) + os.pathsep + vortex_cp).split(os.pathsep)],
}
(build / "provenance.json").write_text(json.dumps(provenance, indent=2, default=str) + "\n")
print(jar)
print("Raw plugin/native jars: " + str(build / "plugin-jars.txt"))
print("Scope: local Spark 3.5.5 / Scala 2.12 prototype; not a supported integration")
