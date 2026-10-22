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
"""Build/run a source-overlay research fixture without changing installed Spark."""
import argparse
import hashlib
import json
import os
import collections
import shutil
from pathlib import Path
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--spark-home", required=True, type=Path)
p.add_argument("--output-dir", required=True, type=Path)
p.add_argument("--master", default="local-cluster[2,1,1024]")
a = p.parse_args()
source = Path(__file__).resolve().parent
a.output_dir.mkdir(parents=True, exist_ok=False)
overlay = a.output_dir / "overlay"
subprocess.run(["python", str(source / "prepare_overlay.py"), "--spark-home",
                str(a.spark_home), "--output-dir", str(overlay)], check=True)
jars = sorted((a.spark_home / "assembly/target/scala-2.12/jars").glob("*.jar"))
assert jars
cp = ":".join(map(str, jars))
classes = a.output_dir / "classes"
classes.mkdir()
snapshot = a.output_dir / "sources"
snapshot.mkdir()
for item in list(source.glob("*.scala")) + [source / "run_spike.py", source / "prepare_overlay.py"]:
    shutil.copyfile(item, snapshot / item.name)
sources = sorted(snapshot.glob("*.scala")) + sorted(overlay.glob("*.scala"))
artifact = a.output_dir / "blockmanager-codec-spike.jar"
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                "-d", str(classes)] + list(map(str, sources))
with (a.output_dir / "compile.log").open("w") as log:
    subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
events = a.output_dir / "events"
events.mkdir()
runtime = a.output_dir / 'runtime'
runtime.mkdir()
for child in a.spark_home.iterdir():
    if child.name != 'work':
        (runtime / child.name).symlink_to(child, target_is_directory=child.is_dir())
(runtime / 'work').mkdir()
conf = {
    'spark.home': str(runtime),
    "spark.plugins": "", "spark.sql.extensions": "",
    "spark.sql.cache.serializer": "org.apache.spark.sql.codeccachespike.ResearchCachedBatchSerializer",
    "spark.driver.extraClassPath": str(artifact),
    "spark.executor.extraClassPath": str(artifact),
    "spark.executor.memory": "1g",
    "spark.ui.enabled": "false",
    "spark.sql.adaptive.enabled": "false",
    "spark.sql.shuffle.partitions": "2",
    "spark.eventLog.enabled": "true", "spark.eventLog.dir": events.as_uri(),
    "spark.eventLog.compress": "false",
    "spark.local.dir": str(a.output_dir / "local"),
    "spark.network.maxRemoteBlockSizeFetchToMem": "1k",
}
argv = [str(a.spark_home / "bin/spark-submit"), "--class",
        "org.apache.spark.sql.codeccachespike.BlockManagerCodecSpike",
        "--master", a.master, "--driver-memory", "1g"]
for k, v in conf.items():
    argv += ["--conf", k + "=" + v]
argv.append(str(artifact))
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {"master": a.master, "compile_argv": compile_argv, "run_argv": argv,
            "sources": {str(path): sha(path) for path in sources},
            "artifact_sha256": sha(artifact), "spark_off_heap_used": False,
            "runner_sha256": sha(Path(__file__)),
            "prepare_sha256": sha(source / "prepare_overlay.py"),
            "original_sources": {relative: sha(a.spark_home / relative) for relative in (
                "core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala",
                "sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala")},
            "runtime_jars": {str(path): sha(path) for path in jars}}
(a.output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
with (a.output_dir / "run.log").open("w") as log:
    status = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=300,
                            env=dict(os.environ, SPARK_HOME=str(runtime), SPARK_WORKER_DIR=str(a.output_dir / 'workers')))
raw = (a.output_dir / "run.log").read_text()
markers = [line for line in raw.splitlines() if line.startswith("CODEC_SPIKE ")]
required = ["DEFAULT_RDD_PASS", "DEFAULT_SQL_WRAPPER_PASS",
            "EVICTION_DISK_RELOAD_PASS", "BLOCKING_REMOVE_READER_PASS",
            "EMPTY_PASS", "TRIPWIRE_PASS", "PRIMARY_PATHS_PASS"]
required += ["PARTIAL_UNROLL_PASS finishToStream=" + mode + " records=32"
             for mode in ("true", "false")]
required += ["NETTY_PASS level=" + str(level) + " columns=" + str(columns)
             for level in range(4) for columns in (1, 2)]
required += ["REMOTE_PASS level=" + str(level) + " columns=" + str(columns)
             for level in range(4) for columns in (1, 2)]
required += ["REPLICAS_PASS columns=" + str(columns) for columns in (1, 2)]
missing = [name for name in required if markers.count("CODEC_SPIKE " + name) != 1]
sql_passes = [line for line in markers if line.startswith("CODEC_SPIKE SQL_PASS ")]
counts = collections.Counter()
failures = []
versions = set()
for path in events.rglob("*"):
    if path.is_file() and not path.name.startswith("."):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            counts[event["Event"]] += 1
            if event["Event"] == "SparkListenerLogStart":
                versions.add(event["Spark Version"])
            if event["Event"] == "SparkListenerTaskEnd":
                reason = event["Task End Reason"]["Reason"]
                if reason != "Success":
                    failures.append(reason)
executor_logs = {}
for path in (runtime / "work").rglob("stdout"):
    lines = [line for line in path.read_text().splitlines()
             if line.startswith("CODEC_SPIKE ")]
    executor_logs[str(path)] = {"sha256": sha(path), "markers": lines}
replica_context_pass = all(
    all(any("CODEC_SPIKE READ executor=" in line
            and "columns=" + str(columns) + " records=4 taskContext=false" in line
            for line in item["markers"]) for columns in (1, 2))
    for item in executor_logs.values())
valid = (replica_context_pass and status.returncode == 0 and not missing and len(sql_passes) == 8
         and counts["SparkListenerApplicationEnd"] == 1 and not failures
         and versions == {"3.5.5"} and len(executor_logs) == 2)
result = {"valid": valid, "exit_code": status.returncode, "markers": markers,
          "missing_checks": missing, "events": dict(counts), "task_failures": failures,
          "spark_versions": sorted(versions), "executor_logs": executor_logs,
          "remaining_unverified": ["decommission", "genuine shuffle recovery with codec",
                                   "native payload accounting", "newer Spark execution"]}
(a.output_dir / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps({"valid": valid, "missing_checks": missing,
                  "exit_code": status.returncode, "output": str(a.output_dir)}))
raise SystemExit(0 if valid else 1)
