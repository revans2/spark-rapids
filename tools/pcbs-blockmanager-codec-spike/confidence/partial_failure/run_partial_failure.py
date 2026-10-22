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
"""Observe partial SQL cache consumption and once-injected decode failure."""
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-run", required=True, type=Path)
parser.add_argument("--spark-home", required=True, type=Path)
parser.add_argument("--output-dir", required=True, type=Path)
args = parser.parse_args()
base = args.base_run.resolve()
output = args.output_dir.resolve()
output.mkdir(parents=True, exist_ok=False)
sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = json.loads((base / "manifest.json").read_text())
assert json.loads((base / "result.json").read_text())["valid"]
overlay = base / "blockmanager-codec-spike.jar"
assert sha(overlay) == manifest["artifact_sha256"]
jars = list(manifest["runtime_jars"])
assert all(sha(Path(path)) == checksum for path, checksum in manifest["runtime_jars"].items())
results = {}
for mode in ("partial", "failure"):
    out = output / mode
    out.mkdir()
    classes = out / "classes"
    classes.mkdir()
    original = Path(__file__).resolve().parent / "PartialFailureSpike.scala"
    source = out / original.name
    shutil.copyfile(original, source)
    shutil.copyfile(__file__, out / "run_partial_failure.py")
    cp = ":".join([str(overlay)] + jars)
    compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                    "-d", str(classes), str(source)]
    with (out / "compile.log").open("w") as log:
        code = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=180)
    if code.returncode:
        raise SystemExit("Compilation failed; inspect " + str(out / "compile.log"))
    artifact = out / "partial-failure-spike.jar"
    subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
    argv = [str(value).replace(str(base), str(out)) for value in manifest["run_argv"]]
    argv[argv.index("--class") + 1] = "org.apache.spark.sql.codeccachespike.PartialFailureSpike"
    for index, value in enumerate(argv):
        if value.startswith(("spark.driver.extraClassPath=", "spark.executor.extraClassPath=")):
            argv[index] = value.split("=")[0] + "=" + str(artifact) + ":" + str(overlay)
        if value.startswith("spark.sql.cache.serializer="):
            argv[index] = ("spark.sql.cache.serializer=" +
                           "org.apache.spark.sql.codeccachespike.PartialFailureCachedBatchSerializer")
    argv[-1] = str(artifact)
    argv.append(mode)
    runtime = out / "runtime"
    runtime.mkdir()
    for child in args.spark_home.iterdir():
        if child.name != "work":
            (runtime / child.name).symlink_to(child, target_is_directory=child.is_dir())
    (runtime / "work").mkdir()
    (out / "events").mkdir()
    proof = {"base_run": str(base), "base_manifest_sha256": sha(base / "manifest.json"),
             "base_result_sha256": sha(base / "result.json"), "overlay_sha256": sha(overlay),
             "source_sha256": sha(source), "runner_sha256": sha(Path(__file__)),
             "artifact_sha256": sha(artifact), "compile_argv": compile_argv, "run_argv": argv}
    (out / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n")
    with (out / "run.log").open("w") as log:
        completed = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT, timeout=180,
                                   env=dict(os.environ, SPARK_HOME=str(runtime)))
    markers = [line for line in (out / "run.log").read_text().splitlines()
               if line.startswith("PARTIAL_FAILURE ")]
    reasons = collections.Counter()
    counts = collections.Counter()
    for path in (out / "events").rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            for line in path.read_text().splitlines():
                event = json.loads(line)
                counts[event["Event"]] += 1
                if event["Event"] == "SparkListenerTaskEnd":
                    reasons[event["Task End Reason"]["Reason"]] += 1
    behavior_valid = (
        counts["SparkListenerApplicationEnd"] == 1
        and markers.count("PARTIAL_FAILURE EXACT_LATER_READ_PASS mode=" + mode +
                          " injected=" + str(1 if mode == "failure" else 0)) == 1
        and all(markers.count("PARTIAL_FAILURE COMPLETE_BLOCK partition=" + str(index) +
                              " records=4 rows=500 diskBytes=4032") == 1 for index in (0, 1))
        and (reasons["ExceptionFailure"] == 1 if mode == "failure"
             else set(reasons) == {"Success"}))
    ownership_pass = completed.returncode == 0 and markers.count(
        "PARTIAL_FAILURE ALL_CHECKS_PASS") == 1
    result = {"valid": behavior_valid and ownership_pass, "behavior_valid": behavior_valid,
              "ownership_pass": ownership_pass, "exit_code": completed.returncode,
              "markers": markers, "task_end_reasons": dict(reasons), "event_counts": dict(counts)}
    (out / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    verifier = {"source_unchanged": sha(original) == proof["source_sha256"],
                "artifact_unchanged": sha(artifact) == proof["artifact_sha256"],
                "runtime_hashes_match": all(sha(Path(path)) == checksum
                                           for path, checksum in manifest["runtime_jars"].items()),
                "result_sha256": sha(out / "result.json"), "manifest_sha256": sha(out / "manifest.json")}
    (out / "verification.json").write_text(json.dumps(verifier, indent=2) + "\n")
    results[mode] = {"valid": result["valid"], "behavior_valid": behavior_valid,
                     "ownership_pass": ownership_pass, "exit_code": completed.returncode}
summary = {"valid": all(result["valid"] for result in results.values()), "cases": results}
(output / "partial-failure-result.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps(summary))
raise SystemExit(0 if summary["valid"] else 1)
