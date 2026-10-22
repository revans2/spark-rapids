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
"""Run genuine recovery and BlockManager decommission against a verified 3.5 overlay."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--base-run", type=Path, required=True)
p.add_argument("--output-dir", type=Path, required=True)
a = p.parse_args()
base = a.base_run.resolve()
out = a.output_dir.resolve()
out.mkdir(parents=True, exist_ok=False)
manifest = json.loads((base / "manifest.json").read_text())
assert json.loads((base / "result.json").read_text())["valid"]
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
overlay = base / "blockmanager-codec-spike.jar"
assert sha(overlay) == manifest["artifact_sha256"]
jars = list(manifest["runtime_jars"])
assert all(sha(Path(path)) == checksum for path, checksum in manifest["runtime_jars"].items())
classes = out / "classes"
classes.mkdir()
source = Path(__file__).resolve().parent / "recovery" / "RecoverySpike.scala"
snapshot = out / source.name
shutil.copyfile(source, snapshot)
shutil.copyfile(__file__, out / "run_recovery.py")
cp = ":".join([str(overlay)] + jars)
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                "-d", str(classes), str(snapshot)]
with (out / "compile.log").open("w") as log:
    code = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=180)
if code.returncode:
    raise SystemExit("Compilation failed: inspect compile.log")
artifact = out / "recovery-spike.jar"
subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
argv = [str(value).replace(str(base), str(out)) for value in manifest["run_argv"]]
argv[argv.index("--class") + 1] = "org.apache.spark.sql.codeccachespike.RecoverySpike"
for index, value in enumerate(argv):
    if value.startswith(("spark.driver.extraClassPath=", "spark.executor.extraClassPath=")):
        argv[index] = value.split("=")[0] + "=" + str(artifact) + ":" + str(overlay)
argv[-1:-1] = ["--conf", "spark.decommission.enabled=true",
              "--conf", "spark.storage.decommission.enabled=true",
              "--conf", "spark.storage.decommission.rddBlocks.enabled=true",
              "--conf", "spark.storage.decommission.shuffleBlocks.enabled=false"]
argv[-1] = str(artifact)
runtime = out / "runtime"
runtime.mkdir()
original = Path("/home/roberte/src/spark_3.5.5")
for child in original.iterdir():
    if child.name != "work":
        (runtime / child.name).symlink_to(child, target_is_directory=child.is_dir())
(runtime / "work").mkdir()
(out / "events").mkdir()
proof = {"base_manifest_sha256": sha(base / "manifest.json"),
         "base_result_sha256": sha(base / "result.json"),
         "base_run": str(base), "overlay_sha256": sha(overlay),
         "source_sha256": sha(snapshot), "runner_sha256": sha(Path(__file__)),
         "compile_argv": compile_argv, "run_argv": argv, "artifact_sha256": sha(artifact)}
(out / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n")
with (out / "run.log").open("w") as log:
    completed = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                               env=dict(os.environ, SPARK_HOME=str(runtime)), timeout=180)
raw = (out / "run.log").read_text()
markers = [line for line in raw.splitlines() if line.startswith("CODEC_RECOVERY ")]
counts = collections.Counter()
reasons = collections.Counter()
versions = set()
for path in (out / "events").rglob("*"):
    if path.is_file() and not path.name.startswith("."):
        for line in path.read_text().splitlines():
            event = json.loads(line)
            counts[event["Event"]] += 1
            if event["Event"] == "SparkListenerLogStart":
                versions.add(event["Spark Version"])
            if event["Event"] == "SparkListenerTaskEnd":
                reasons[event["Task End Reason"]["Reason"]] += 1
worker_markers = []
worker_logs = {}
for path in (runtime / "work").rglob("stdout"):
    worker_logs[str(path)] = sha(path)
    worker_markers.extend(line for line in path.read_text().splitlines()
                          if line.startswith("CODEC_RECOVERY "))
gate_pass = (markers.count("CODEC_RECOVERY INITIAL_CACHE_LOCATIONS_PRESENT blocks=4") == 1
             and sum("DRIVER_CACHE_REMOVAL" in line for line in markers) >= 2
             and worker_markers.count(
                 "CODEC_RECOVERY CACHE_REMOVAL_GATE blocks=4 beforeParentRead=true") >= 2)
valid = (gate_pass and completed.returncode == 0 and versions == {"3.5.5"}
         and counts["SparkListenerApplicationEnd"] == 1
         and reasons["FetchFailed"] > 0 and set(reasons) <= {"Success", "FetchFailed", "TaskKilled"}
         and sum("GENUINE_RECOVERY_PASS" in line for line in markers) == 1
         and sum("DECOMMISSION_PASS" in line for line in markers) == 1
         and sum("SHUFFLE_REMOVED" in line for line in markers) == 1)
result = {"valid": valid, "exit_code": completed.returncode, "markers": markers,
          "task_end_reasons": dict(reasons), "event_counts": dict(counts),
          "gate_pass": gate_pass, "worker_markers": worker_markers, "worker_logs": worker_logs,
          "spark_versions": sorted(versions),
          "scope": "small heap-backed codec fixture; real BM migration, not executor termination"}
(out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
print(json.dumps(result))
raise SystemExit(0 if valid else 1)
