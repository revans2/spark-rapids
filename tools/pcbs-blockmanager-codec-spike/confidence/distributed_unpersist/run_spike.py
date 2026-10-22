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
"""Distributed blocking DataFrame.unpersist confidence proof; no native backend."""
import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--spark-home", type=Path, required=True)
p.add_argument("--codec-jar", type=Path, required=True)
p.add_argument("--inspection-jar", type=Path, required=True)
p.add_argument("--output-dir", type=Path, required=True)
a = p.parse_args()
a.output_dir.mkdir(parents=True, exist_ok=False)
source = Path(__file__).resolve().parent
snapshot = a.output_dir / "sources"
snapshot.mkdir()
for item in source.iterdir():
    if item.is_file():
        shutil.copyfile(item, snapshot / item.name)
jars = sorted((a.spark_home / "assembly/target/scala-2.12/jars").glob("*.jar"))
assert jars
dependencies = [a.codec_jar, a.inspection_jar]
cp = ":".join(map(str, dependencies + jars))
classes = a.output_dir / "classes"
classes.mkdir()
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                "-d", str(classes), str(snapshot / "DistributedUnpersistSpike.scala")]
with (a.output_dir / "compile.log").open("w") as log:
    status = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=120)
if status.returncode:
    print((a.output_dir / "compile.log").read_text())
    raise SystemExit(status.returncode)
artifact = a.output_dir / "distributed-unpersist-spike.jar"
subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
runtime = a.output_dir / "runtime"
runtime.mkdir()
for child in a.spark_home.iterdir():
    if child.name != "work":
        (runtime / child.name).symlink_to(child, target_is_directory=child.is_dir())
(runtime / "work").mkdir()
events = a.output_dir / "events"
events.mkdir()
conf = {
    "spark.home": str(runtime),
    "spark.plugins": "org.apache.spark.sql.cleanreceiverspike.InspectionPlugin",
    "spark.sql.extensions": "",
    "spark.sql.cache.serializer": "org.apache.spark.sql.codeccachespike.ResearchCachedBatchSerializer",
    "spark.driver.extraClassPath": ":".join(map(str, dependencies + [artifact])),
    "spark.executor.extraClassPath": ":".join(map(str, dependencies + [artifact])),
    "spark.executor.memory": "1g", "spark.executor.instances": "2",
    "spark.ui.enabled": "false", "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "1", "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": events.as_uri(), "spark.eventLog.compress": "false",
    "spark.local.dir": str(a.output_dir / "local"),
}
argv = [str(a.spark_home / "bin/spark-submit"), "--class",
        "org.apache.spark.sql.distributedunpersistspike.DistributedUnpersistSpike",
        "--master", "local-cluster[2,1,1024]", "--driver-memory", "1g",
        "--jars", ",".join(map(str, dependencies))]
for key, value in conf.items():
    argv += ["--conf", key + "=" + value]
argv.append(str(artifact))
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {
    "compile_argv": compile_argv, "run_argv": argv,
    "sources": {str(path): sha(path) for path in snapshot.iterdir()},
    "artifacts": {str(path): sha(path) for path in dependencies + [artifact]},
    "runtime_jars": {str(path): sha(path) for path in jars}, "native": False,
}
(a.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
with (a.output_dir / "run.log").open("w") as log:
    status = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=240,
                            env=dict(os.environ, SPARK_HOME=str(runtime),
                                     SPARK_WORKER_DIR=str(a.output_dir / "workers")))
raw = (a.output_dir / "run.log").read_text()
markers = [line for line in raw.splitlines() if line.startswith("DISTRIBUTED_UNPERSIST ")]
expected = [
    "SQL_UNPERSIST_CALL blocking=true",
    "SQL_UNPERSIST_BLOCKED actualDatasetAndMasterStack=true readerHeld=true",
    "READER_RELEASE", "READER_FINISHED rows=1000", "SQL_UNPERSIST_RETURN",
]
errors = []
if status.returncode:
    errors.append("application process failed")
for name in expected:
    if markers.count("DISTRIBUTED_UNPERSIST " + name) != 1:
        errors.append("missing or duplicate " + name)
if not errors:
    positions = {name: markers.index("DISTRIBUTED_UNPERSIST " + name) for name in expected}
    assert positions[expected[0]] < positions[expected[1]] < positions[expected[2]]
    assert positions[expected[2]] < positions["SQL_UNPERSIST_RETURN"]
    assert positions[expected[2]] < positions["READER_FINISHED rows=1000"]
passes = [line for line in markers if line.startswith("DISTRIBUTED_UNPERSIST PASS ")]
held = [line for line in markers if line.startswith("DISTRIBUTED_UNPERSIST READER_HELD ")]
assert len(passes) == len(held) == 1
details = dict(re.findall(r"(\w+)=([^ ]+)", passes[0]))
assert details["replicasRemoved"] == "2" and details["rows"] == "1000"
logs = {}
for path in (runtime / "work").rglob("stdout"):
    text = path.read_text()
    ids = re.findall(r"CLEAN_RECEIVER REGISTER executor=(\S+)", text)
    assert len(ids) == 1
    logs[ids[0]] = {"path": str(path), "sha256": sha(path),
                   "markers": [line for line in text.splitlines()
                               if line.startswith(("DISTRIBUTED_UNPERSIST ", "CLEAN_RECEIVER INSPECT "))]}
assert len(logs) == 2
assert logs[details["reader"]]["markers"].count(
    "DISTRIBUTED_UNPERSIST READER_RESUMED executor=" + details["reader"]) == 1
for executor, evidence in logs.items():
    absent = f"CLEAN_RECEIVER INSPECT executor={executor} present=false rows=0 values=0 taskContext=false"
    assert evidence["markers"].count(absent) == 1
    assert markers.count("DISTRIBUTED_UNPERSIST EXECUTOR_ABSENT executor=" + executor +
                         " block=rdd_" + details["owner"] + "_0") == 1
counts = Counter()
versions = []
failures = []
unpersist_ids = []
for path in events.iterdir():
    if path.is_file() and not path.name.startswith("."):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            counts[event["Event"]] += 1
            if event["Event"] == "SparkListenerLogStart":
                versions.append(event["Spark Version"])
            elif event["Event"] == "SparkListenerTaskEnd":
                reason = event["Task End Reason"]["Reason"]
                if reason != "Success":
                    failures.append(reason)
            elif event["Event"] == "SparkListenerUnpersistRDD":
                unpersist_ids.append(event["RDD ID"])
assert versions == ["3.5.5"]
assert counts["SparkListenerExecutorAdded"] == 2
assert counts["SparkListenerApplicationEnd"] == 1 and not failures
assert unpersist_ids == [int(details["owner"])]
valid = not errors
report = {"valid": valid, "errors": errors, "exit_code": status.returncode,
          "details": details, "markers": markers, "executor_logs": logs,
          "events": dict(counts), "unpersist_rdd_ids": unpersist_ids,
          "task_failures": failures, "spark_versions": versions}
(a.output_dir / "result.json").write_text(json.dumps(report, indent=2) + "\n")
(a.output_dir / "verifier.json").write_text(json.dumps({
    "valid": valid, "manifest_sha256": sha(a.output_dir / "manifest.json"),
    "result_sha256": sha(a.output_dir / "result.json"),
    "event_files": {str(path): sha(path) for path in events.iterdir()
                    if path.is_file() and not path.name.startswith(".")},
    "driver_log_sha256": sha(a.output_dir / "run.log"),
}, indent=2) + "\n")
print(json.dumps({"valid": valid, "errors": errors, "output": str(a.output_dir)}))
raise SystemExit(0 if valid else 1)
