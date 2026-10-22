#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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
"""Disposable native lifetime proof using an existing Stage A codec overlay."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import zipfile

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--spark-home", type=Path, required=True)
p.add_argument("--codec-jar", type=Path, required=True)
p.add_argument("--rapids-jar", type=Path, required=True)
p.add_argument("--output-dir", type=Path, required=True)
p.add_argument("--master", default="local[2]")
a = p.parse_args()
a.output_dir.mkdir(parents=True, exist_ok=False)
source = Path(__file__).resolve().parent
flat = a.output_dir / "rapids-flat-research.jar"
# The published plugin loads shim classes through its plugin loader. This small fixture
# uses no SQLPlugin, so expose the already built shared/355 classes explicitly.
with zipfile.ZipFile(a.rapids_jar) as original, zipfile.ZipFile(flat, "w") as target:
    added = set()
    for prefix in ("spark355/", "spark-shared/"):
        for entry in original.infolist():
            if entry.filename.startswith(prefix) and entry.filename.endswith(".class"):
                name = entry.filename[len(prefix):]
                if name not in added:
                    target.writestr(name, original.read(entry))
                    added.add(name)
jars = sorted((a.spark_home / "assembly/target/scala-2.12/jars").glob("*.jar"))
assert jars
extra = [a.codec_jar, flat, a.rapids_jar]
cp = ":".join(map(str, extra + jars))
classes = a.output_dir / "classes"
classes.mkdir()
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp, "-d",
                str(classes), str(source / "BlockManagerNativeSpike.scala")]
with (a.output_dir / "compile.log").open("w") as log:
    result = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=120)
if result.returncode:
    print((a.output_dir / "compile.log").read_text())
    raise SystemExit(result.returncode)
artifact = a.output_dir / "native-spike.jar"
subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
extra.insert(0, artifact)
events = a.output_dir / "events"
events.mkdir()
snapshots = a.output_dir / "sources"
snapshots.mkdir()
for path in source.glob("*"):
    if path.is_file():
        (snapshots / path.name).write_bytes(path.read_bytes())
runtime = a.output_dir / "runtime"
runtime.mkdir()
for child in a.spark_home.iterdir():
    if child.name != "work":
        (runtime / child.name).symlink_to(child, target_is_directory=child.is_dir())
(runtime / "work").mkdir()
conf = {
    "spark.home": str(runtime),
    "spark.plugins": "", "spark.sql.extensions": "",
    "spark.driver.extraClassPath": ":".join(map(str, extra)),
    "spark.executor.extraClassPath": ":".join(map(str, extra)),
    "spark.executor.memory": "1g", "spark.ui.enabled": "false",
    "spark.eventLog.enabled": "true", "spark.eventLog.dir": events.as_uri(),
    "spark.eventLog.compress": "false",
    "spark.local.dir": str(a.output_dir / "local"),
    "spark.memory.offHeap.enabled": "false",
    "spark.memory.offHeap.size": "0",
    "spark.network.maxRemoteBlockSizeFetchToMem": "1k",
}
argv = [str(a.spark_home / "bin/spark-submit"), "--class",
        "org.apache.spark.storage.nativecachespike.BlockManagerNativeSpike",
        "--master", a.master, "--driver-memory", "1g",
        "--jars", ",".join(map(str, [a.codec_jar, flat, a.rapids_jar]))]
for key, value in conf.items():
    argv += ["--conf", key + "=" + value]
argv.append(str(artifact))
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {"argv": argv, "compile_argv": compile_argv, "master": a.master,
            "sources": {str(path): sha(path) for path in snapshots.iterdir()},
            "spark_runtime_jars": {str(path): sha(path) for path in jars},
            "artifacts": {str(path): sha(path) for path in extra},
            "storage_level": "MEMORY_AND_DISK deserialized",
            "rapids_host_limit_bytes": 64 * 1024 * 1024,
            "spark_off_heap_enabled": False}
(a.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
with (a.output_dir / "run.log").open("w") as log:
    status = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=240,
                            env=dict(os.environ, SPARK_HOME=str(runtime),
                                     SPARK_WORKER_DIR=str(a.output_dir / "workers")))
markers = [line for line in (a.output_dir / "run.log").read_text().splitlines()
           if line.startswith("NATIVE_SPIKE ")]
expected = ["LOCAL_NATIVE_PASS", "RAPIDS_SPILL_RESTORE_PASS",
            "BLOCKMANAGER_DISK_RELOAD_PASS", "BLOCKING_REMOVAL_AND_INDEPENDENT_REFERENCE_PASS",
            "LIFETIME_PATHS_PASS", "FRAMEWORK_SHUTDOWN_HOST_ZERO_PASS"]
if a.master.startswith("local-cluster"):
    expected.append("MULTI_JVM_REMOTE_NATIVE_PASS")
records = [json.loads(line) for path in events.iterdir() if not path.name.startswith(".")
           for line in path.read_text().splitlines() if line.strip()]
versions = [row["Spark Version"] for row in records if row["Event"] == "SparkListenerLogStart"]
ends = sum(row["Event"] == "SparkListenerApplicationEnd" for row in records)
failures = [row for row in records if row["Event"] == "SparkListenerTaskEnd"
            and row["Task End Reason"]["Reason"] != "Success"]
valid = (status.returncode == 0 and ends == 1 and versions == ["3.5.5"] and not failures
         and all(markers.count("NATIVE_SPIKE " + item) == 1 for item in expected))
result = {"valid": valid, "exit_code": status.returncode, "markers": markers,
          "spark_versions": versions, "application_ends": ends,
          "task_failures": len(failures)}
(a.output_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
raise SystemExit(0 if valid else 1)
