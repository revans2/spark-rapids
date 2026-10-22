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
"""Run the existing recovery/migration proof with the verified Spark 4.0.1 overlay.

Only generated fixture snapshots are adapted; existing Spark installations and
shared fixtures remain unchanged. This is a bounded heap-codec research proof.
"""
import argparse
from collections import Counter
import hashlib
import re
import json
from pathlib import Path
import subprocess
import sys


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError("Shared fixture changed; expected exactly one: " + old)
    return text.replace(old, new)


def verify_evidence(output):
    """Reparse raw events/gates and rehash retained inputs independently."""
    run = output / "run"
    adapter = json.loads((output / "adapter-manifest.json").read_text())
    manifest = json.loads((run / "manifest.json").read_text())
    result = json.loads((run / "result.json").read_text())

    def require(condition, message):
        if not condition:
            raise ValueError(message)

    require(result["valid"] and result["exit_code"] == 0, "Unsuccessful fixture")
    base = Path(manifest["base_run"])
    inputs = {
        output / "run_recovery_401.py": adapter["adapter_sha256"],
        run / "RecoverySpike.scala": manifest["source_sha256"],
        run / "run_recovery.py": manifest["runner_sha256"],
        run / "recovery-spike.jar": manifest["artifact_sha256"],
        base / "manifest.json": manifest["base_manifest_sha256"],
        base / "result.json": manifest["base_result_sha256"],
        base / "blockmanager-codec-spike.jar": manifest["overlay_sha256"],
        output / "fixture/recovery/RecoverySpike.scala": adapter["adapted_source_sha256"],
        output / "fixture/run_recovery.py": adapter["adapted_runner_sha256"],
    }
    base_manifest = json.loads((base / "manifest.json").read_text())
    inputs.update({Path(p): h for p, h in base_manifest["runtime_jars"].items()})
    inputs.update({Path(p): h for p, h in result["worker_logs"].items()})
    for path, expected in inputs.items():
        require(digest(path) == expected, "Changed retained input: " + str(path))
    require(len(result["worker_logs"]) == 2, "Expected two executor logs")
    raw = (run / "run.log").read_text()
    markers = [line for line in raw.splitlines() if line.startswith("CODEC_RECOVERY ")]
    require(markers == result["markers"], "Driver marker report differs from raw log")
    require("org.apache.spark.shuffle.MetadataFetchFailedException" in raw,
            "No genuine missing-shuffle exception")
    require(markers.count("CODEC_RECOVERY SHUFFLE_REMOVED id=0 files=4") == 1,
            "Expected four physical shuffle files removed")
    require(markers.count(
        "CODEC_RECOVERY INITIAL_CACHE_LOCATIONS_PRESENT blocks=4") == 1,
        "Missing initial cached ancestor")
    require(sum("DRIVER_CACHE_REMOVAL" in line for line in markers) >= 2,
            "Missing driver removal gates")
    worker_gates = sum(Path(p).read_text().splitlines().count(
        "CODEC_RECOVERY CACHE_REMOVAL_GATE blocks=4 beforeParentRead=true")
        for p in result["worker_logs"])
    require(worker_gates >= 2, "Missing worker before-read gates")
    require(markers.count(
        "CODEC_RECOVERY GENUINE_RECOVERY_PASS fetchFailed=1 rows=10000") == 1,
        "Missing exact recovery result")
    migrations = [re.fullmatch(
        r"CODEC_RECOVERY DECOMMISSION_PASS old=([0-9]+) new=([0-9]+) rows=10000", line)
        for line in markers if "DECOMMISSION_PASS" in line]
    require(len(migrations) == 1 and migrations[0] is not None
            and migrations[0].group(1) != migrations[0].group(2),
            "Missing actual block relocation")
    counts, reasons = Counter(), Counter()
    versions = set()
    stages = []
    for path in (run / "events").rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            for line in path.read_text().splitlines():
                event = json.loads(line)
                counts[event["Event"]] += 1
                if event["Event"] == "SparkListenerLogStart":
                    versions.add(event["Spark Version"])
                elif event["Event"] == "SparkListenerTaskEnd":
                    reasons[event["Task End Reason"]["Reason"]] += 1
                elif event["Event"] == "SparkListenerStageSubmitted":
                    info = event["Stage Info"]
                    stages.append((info["Stage ID"], info["Stage Attempt ID"]))
    require(versions == {"4.0.1"}, "Wrong runtime version")
    require(reasons == {"Success": 13, "FetchFailed": 1},
            "Expected thirteen successes and exactly one genuine fetch failure")
    require(dict(reasons) == result["task_end_reasons"], "Task report differs")
    require(counts["SparkListenerExecutorAdded"] == 2, "Expected two executor JVMs")
    require(counts["SparkListenerApplicationEnd"] == 1, "Missing normal completion")
    require(any(attempt == 1 for _, attempt in stages), "Missing stage retry")
    checked = {"valid": True, "spark_version": "4.0.1",
               "verified_hashes": len(inputs), "task_end_reasons": dict(reasons),
               "worker_gates": worker_gates, "stage_submissions": stages,
               "application_ends": counts["SparkListenerApplicationEnd"]}
    (output / "checked-evidence.json").write_text(json.dumps(checked, indent=2) + "\n")
    return checked


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", type=Path)
    parser.add_argument("--verify-only", action="store_true")
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--spark-home", type=Path,
                        default=Path("/home/roberte/src/spark_4.0.1"))
    args = parser.parse_args()
    output = args.output_dir.resolve()
    if args.verify_only:
        print(json.dumps(verify_evidence(output)))
        return
    if args.base_run is None:
        parser.error("--base-run is required unless --verify-only is set")
    base = args.base_run.resolve()
    spark_home = args.spark_home.resolve()
    result = json.loads((base / "result.json").read_text())
    if not result["valid"] or result["spark_versions"] != ["4.0.1"]:
        raise ValueError("A successful Spark 4.0.1 primary-codec base run is required")
    output.mkdir(parents=True, exist_ok=False)
    shared = Path(__file__).resolve().parents[1]
    fixture = output / "fixture"
    (fixture / "recovery").mkdir(parents=True)

    source = shared / "recovery" / "RecoverySpike.scala"
    original = source.read_text()
    adapted_source = replace_once(
        original, "import org.apache.spark.sql.SparkSession",
        "import org.apache.spark.sql.classic.SparkSession")
    (fixture / "recovery" / source.name).write_text(adapted_source)

    runner = shared / "run_recovery.py"
    adapted_runner = replace_once(
        runner.read_text(),
        'original = Path("/home/roberte/src/spark_3.5.5")',
        "original = Path(" + repr(str(spark_home)) + ")")
    adapted_runner = replace_once(
        adapted_runner, 'versions == {"3.5.5"}', 'versions == {"4.0.1"}')
    adapted_runner = replace_once(
        adapted_runner, 'reasons["FetchFailed"] > 0', 'reasons["FetchFailed"] == 1')
    adapted_runner = replace_once(
        adapted_runner, 'against a verified 3.5 overlay.',
        'against a verified 4.0.1 overlay.')
    adapted_path = fixture / runner.name
    adapted_path.write_text(adapted_runner)

    command = [sys.executable, str(adapted_path),
               "--base-run", str(base), "--output-dir", str(output / "run")]
    (output / "run_recovery_401.py").write_bytes(Path(__file__).read_bytes())
    manifest = {
        "adapter_sha256": digest(Path(__file__)),
        "base_run": str(base),
        "base_manifest_sha256": digest(base / "manifest.json"),
        "base_result_sha256": digest(base / "result.json"),
        "original_source_sha256": digest(source),
        "adapted_source_sha256": digest(fixture / "recovery" / source.name),
        "original_runner_sha256": digest(runner),
        "adapted_runner_sha256": digest(adapted_path),
        "spark_home": str(spark_home),
        "command": command,
        "changes": [
            "Use Spark 4 classic SparkSession for CacheManager Dataset APIs.",
            "Use Spark 4.0.1 runtime and require its raw event version.",
            "Require exactly one genuine FetchFailed; all original gates retained.",
        ],
        "scope": "Heap codec recovery and BlockManager migration; no executor kill.",
        "spark_off_heap_used": False,
    }
    (output / "adapter-manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n")
    completed = subprocess.run(command)
    if completed.returncode:
        raise SystemExit(completed.returncode)
    outcome = json.loads((output / "run/result.json").read_text())
    if (not outcome["valid"] or not outcome["gate_pass"]
            or outcome["spark_versions"] != ["4.0.1"]
            or outcome["task_end_reasons"].get("FetchFailed") != 1):
        raise ValueError("Spark 4.0.1 recovery confidence criteria failed")
    verify_evidence(output)
    print(json.dumps({"valid": True, "output": str(output),
                      "task_end_reasons": outcome["task_end_reasons"]}))


if __name__ == "__main__":
    main()
