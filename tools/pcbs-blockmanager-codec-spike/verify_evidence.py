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
"""Independently check retained sources, artifacts and raw Spark events."""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def audit(directory, version, native=False):
    directory = directory.resolve()
    manifest = json.loads((directory / "manifest.json").read_text())
    result = json.loads((directory / "result.json").read_text())
    require(result["valid"] and result["exit_code"] == 0, "Unsuccessful fixture")
    checked = 0
    for field in ("sources", "runtime_jars", "spark_runtime_jars", "artifacts"):
        for path, expected in manifest.get(field, {}).items():
            require(digest(path) == expected, "Changed evidence input: " + path)
            checked += 1
    artifact = directory / ("native-spike.jar" if native else "blockmanager-codec-spike.jar")
    if not native:
        require(digest(artifact) == manifest["artifact_sha256"], "Changed codec artifact")
    counts = Counter()
    failures = Counter()
    observed_versions = set()
    for path in (directory / "events").rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            for line in path.read_text().splitlines():
                event = json.loads(line)
                counts[event["Event"]] += 1
                if event["Event"] == "SparkListenerLogStart":
                    observed_versions.add(event["Spark Version"])
                if event["Event"] == "SparkListenerTaskEnd":
                    reason = event["Task End Reason"]["Reason"]
                    if reason != "Success":
                        failures[reason] += 1
    require(observed_versions == {version}, "Wrong runtime version")
    require(counts["SparkListenerApplicationEnd"] == 1, "Missing normal application end")
    require(not failures, "Unexpected failed tasks: " + repr(failures))
    require(counts["SparkListenerExecutorAdded"] == 2, "Expected two executor JVMs")
    raw = (directory / "run.log").read_text()
    prefix = "NATIVE_SPIKE " if native else "CODEC_SPIKE "
    markers = [line for line in raw.splitlines() if line.startswith(prefix)]
    require(markers == result["markers"], "Result markers differ from raw driver log")
    if native:
        require(manifest["spark_off_heap_enabled"] is False, "Spark OFF_HEAP enabled")
        require("NATIVE_SPIKE OWNERS created=4 closed=4" in markers, "Native owner imbalance")
        cleanup = [line for line in markers if line.startswith("NATIVE_SPIKE EXECUTOR_CLEAN ")]
        require(len(cleanup) == 2, "Missing executor cleanup")
        require(all(line.endswith(",1,1,0,0,0,0.015625)") for line in cleanup),
                "Unexpected executor owners, handles, Spark offheap or RAPIDS usage")
        require("NATIVE_SPIKE FRAMEWORK_SHUTDOWN_HOST_ZERO_PASS" in markers,
                "Missing framework host cleanup")
    else:
        for level in range(4):
            for columns in (1, 2):
                for route in ("NETTY", "REMOTE"):
                    marker = f"CODEC_SPIKE {route}_PASS level={level} columns={columns}"
                    require(markers.count(marker) == 1, "Missing exact route: " + marker)
        for mode in ("false", "true"):
            require(markers.count("CODEC_SPIKE PARTIAL_UNROLL_PASS "
                                  f"finishToStream={mode} records=32") == 1,
                    "Missing partial admission route")
        logs = list((directory / "runtime/work").rglob("stdout"))
        require(len(logs) == 2, "Missing executor stdout")
        for log in logs:
            text = log.read_text()
            for columns in (1, 2):
                require(any(f"columns={columns}" in line and "taskContext=false" in line
                            and line.startswith("CODEC_SPIKE READ ")
                            for line in text.splitlines()), "Missing replica context evidence")
    return {"directory": str(directory), "spark_version": version, "valid": True,
            "kind": "native lifetime" if native else "heap codec",
            "verified_hashes": checked, "artifact_sha256": digest(artifact),
            "task_ends": counts["SparkListenerTaskEnd"],
            "executor_jvms": counts["SparkListenerExecutorAdded"],
            "application_ends": counts["SparkListenerApplicationEnd"],
            "task_failures": dict(failures)}


def audit_recovery(directory):
    directory = directory.resolve()
    manifest = json.loads((directory / "manifest.json").read_text())
    result = json.loads((directory / "result.json").read_text())
    require(result["valid"] and result["gate_pass"], "Unsuccessful recovery fixture")
    base = Path(manifest["base_run"])
    inputs = {
        directory / "RecoverySpike.scala": manifest["source_sha256"],
        directory / "run_recovery.py": manifest["runner_sha256"],
        directory / "recovery-spike.jar": manifest["artifact_sha256"],
        base / "manifest.json": manifest["base_manifest_sha256"],
        base / "result.json": manifest["base_result_sha256"],
        base / "blockmanager-codec-spike.jar": manifest["overlay_sha256"],
    }
    inputs.update({Path(path): value for path, value in result["worker_logs"].items()})
    for path, expected in inputs.items():
        require(digest(path) == expected, "Changed recovery input: " + str(path))
    reasons = Counter()
    ends = 0
    versions = set()
    for path in (directory / "events").rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            for line in path.read_text().splitlines():
                event = json.loads(line)
                if event["Event"] == "SparkListenerTaskEnd":
                    reasons[event["Task End Reason"]["Reason"]] += 1
                elif event["Event"] == "SparkListenerApplicationEnd":
                    ends += 1
                elif event["Event"] == "SparkListenerLogStart":
                    versions.add(event["Spark Version"])
    require(versions == {"3.5.5"} and ends == 1, "Wrong recovery runtime/end")
    require(reasons == result["task_end_reasons"], "Recovery event report differs")
    require(reasons["FetchFailed"] >= 1 and set(reasons) <=
            {"Success", "FetchFailed", "TaskKilled"}, "Unexpected recovery outcome")
    raw = (directory / "run.log").read_text()
    require("MetadataFetchFailedException" in raw, "No genuine missing-shuffle exception")
    require("CODEC_RECOVERY SHUFFLE_REMOVED id=0 files=4" in raw, "No physical loss")
    markers = [line for line in raw.splitlines() if line.startswith("CODEC_RECOVERY ")]
    require(markers == result["markers"], "Recovery markers differ from raw log")
    require(markers.count("CODEC_RECOVERY INITIAL_CACHE_LOCATIONS_PRESENT blocks=4") == 1,
            "Missing cache presence before scheduling")
    worker_markers = [line for path in result["worker_logs"]
                      for line in Path(path).read_text().splitlines()]
    require(worker_markers.count("CODEC_RECOVERY CACHE_REMOVAL_GATE blocks=4 "
                                 "beforeParentRead=true") >= 2,
            "Missing worker gates before parent reads")
    require(sum("DRIVER_CACHE_REMOVAL" in line for line in markers) >= 2,
            "No before-read removal gates on original attempt and retry")
    require(sum("DECOMMISSION_PASS" in line for line in markers) == 1,
            "No completed migration")
    return {"directory": str(directory), "spark_version": "3.5.5", "valid": True,
            "kind": "genuine recovery and BlockManager migration",
            "verified_hashes": len(inputs), "task_end_reasons": dict(reasons),
            "application_ends": ends}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codec-355", required=True, type=Path)
    parser.add_argument("--codec-401", required=True, type=Path)
    parser.add_argument("--native-355", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--recovery-355", type=Path)
    args = parser.parse_args()
    results = [
        audit(args.codec_355, "3.5.5"),
        audit(args.codec_401, "4.0.1"),
        audit(args.native_355, "3.5.5", native=True),
    ]
    if args.recovery_355 is not None:
        results.append(audit_recovery(args.recovery_355))
    args.output.write_text(json.dumps({"valid": True, "runs": results}, indent=2) + "\n")
    print(json.dumps({"valid": True, "runs": len(results), "output": str(args.output)}))


if __name__ == "__main__":
    main()
