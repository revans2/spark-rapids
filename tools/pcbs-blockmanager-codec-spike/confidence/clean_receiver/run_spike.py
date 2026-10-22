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
"""Fresh three-executor, one-partition SQL cache clean-receiver proof."""
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
cp = ":".join(map(str, [a.codec_jar] + jars))
classes = a.output_dir / "classes"
classes.mkdir()
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                "-d", str(classes), str(snapshot / "CleanReceiverSpike.scala")]
with (a.output_dir / "compile.log").open("w") as log:
    status = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=120)
if status.returncode:
    print((a.output_dir / "compile.log").read_text())
    raise SystemExit(status.returncode)
artifact = a.output_dir / "clean-receiver-spike.jar"
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
    "spark.driver.extraClassPath": str(a.codec_jar) + ":" + str(artifact),
    "spark.executor.extraClassPath": str(a.codec_jar) + ":" + str(artifact),
    "spark.executor.memory": "1g",
    "spark.executor.instances": "3",
    "spark.ui.enabled": "false",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "1",
    "spark.eventLog.enabled": "true",
    "spark.eventLog.dir": events.as_uri(),
    "spark.eventLog.compress": "false",
    "spark.local.dir": str(a.output_dir / "local"),
}
argv = [str(a.spark_home / "bin/spark-submit"), "--class",
        "org.apache.spark.sql.cleanreceiverspike.CleanReceiverSpike",
        "--master", "local-cluster[3,1,1024]", "--driver-memory", "1g",
        "--jars", str(a.codec_jar)]
for key, value in conf.items():
    argv += ["--conf", key + "=" + value]
argv.append(str(artifact))
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {
    "compile_argv": compile_argv, "run_argv": argv,
    "sources": {str(path): sha(path) for path in snapshot.iterdir()},
    "artifacts": {str(path): sha(path) for path in (a.codec_jar, artifact)},
    "runtime_jars": {str(path): sha(path) for path in jars},
    "native": False,
}
(a.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
with (a.output_dir / "run.log").open("w") as log:
    status = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=240,
                            env=dict(os.environ, SPARK_HOME=str(runtime),
                                     SPARK_WORKER_DIR=str(a.output_dir / "workers")))
raw = (a.output_dir / "run.log").read_text()
markers = [line for line in raw.splitlines() if line.startswith("CLEAN_RECEIVER ")]
results = [line for line in markers if line.startswith("CLEAN_RECEIVER RESULT ")]
errors = []
if status.returncode:
    errors.append("application process failed")
if len(results) != 1:
    errors.append("one structured result required")
else:
    result = dict(re.findall(r"(\w+)=([^ ]+)", results[0]))
    producer = result["producer"]
    receiver = result["receiver"]
    assert result["registered"] == "3" and result["partitions"] == "1"
    assert result["rows"] == "1000" and result["values"] == "2000"
    assert result["upstreamComputes"] == "1" and producer != receiver
    logs = {}
    for path in (runtime / "work").rglob("stdout"):
        text = path.read_text()
        registration = re.findall(r"CLEAN_RECEIVER REGISTER executor=(\S+)", text)
        assert len(registration) == 1
        logs[registration[0]] = {
            "path": str(path), "sha256": sha(path),
            "codec_writes": [line for line in text.splitlines()
                             if line.startswith("CODEC_SPIKE WRITE ")],
            "codec_reads": [line for line in text.splitlines()
                            if line.startswith("CODEC_SPIKE READ ")],
            "produces": [line for line in text.splitlines()
                         if line.startswith("CLEAN_RECEIVER PRODUCE ")],
            "inspections": [line for line in text.splitlines()
                            if line.startswith("CLEAN_RECEIVER INSPECT ")],
        }
    assert len(logs) == 3
    assert logs[producer]["produces"] == ["CLEAN_RECEIVER PRODUCE executor=" + producer]
    assert len(logs[producer]["codec_writes"]) >= 1
    assert not logs[receiver]["produces"] and not logs[receiver]["codec_writes"]
    assert logs[receiver]["codec_reads"] == [
        "CODEC_SPIKE READ executor=" + receiver + " columns=2 records=8 taskContext=false"]
    for executor, evidence in logs.items():
        if executor != producer:
            assert not evidence["produces"] and not evidence["codec_writes"]
        if executor not in (producer, receiver):
            assert not evidence["codec_reads"]
        expected_present = executor in (producer, receiver)
        assert evidence["inspections"] == [
            f"CLEAN_RECEIVER INSPECT executor={executor} present={str(expected_present).lower()} "
            f"rows={1000 if expected_present else 0} values={2000 if expected_present else 0} "
            "taskContext=false"]
    result["executor_logs"] = logs
counts = Counter()
versions = []
failures = []
for path in events.iterdir():
    if path.is_file() and not path.name.startswith("."):
        for line in path.read_text().splitlines():
            item = json.loads(line)
            counts[item["Event"]] += 1
            if item["Event"] == "SparkListenerLogStart":
                versions.append(item["Spark Version"])
            if item["Event"] == "SparkListenerTaskEnd":
                reason = item["Task End Reason"]["Reason"]
                if reason != "Success":
                    failures.append(reason)
assert versions == ["3.5.5"]
assert counts["SparkListenerExecutorAdded"] == 3
assert counts["SparkListenerApplicationEnd"] == 1 and not failures
assert markers.count("CLEAN_RECEIVER PASS") == 1
assert markers.count("CLEAN_RECEIVER TRIPWIRES_PASS") == 1
valid = not errors
report = {"valid": valid, "errors": errors, "exit_code": status.returncode,
          "result": result if results else None, "markers": markers,
          "events": dict(counts), "task_failures": failures, "spark_versions": versions}
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
