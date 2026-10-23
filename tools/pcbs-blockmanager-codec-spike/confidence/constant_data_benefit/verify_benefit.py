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
"""Independently parse raw codec-benefit evidence and recheck immutable inputs."""
import argparse
import collections
import hashlib
import json
from pathlib import Path

sha = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify(directory):
    directory = Path(directory)
    proof = json.loads((directory / "manifest.json").read_text())
    process = json.loads((directory / "process-result.json").read_text())
    raw = (directory / "run.log").read_text()
    markers = [json.loads(line[len("CONSTANT_DATA_BENEFIT "):])
               for line in raw.splitlines() if line.startswith("CONSTANT_DATA_BENEFIT ")]
    hashes = {}
    for path, expected in proof["inputs"].items():
        hashes[path] = sha(path) == expected
    for path, expected in proof.get("raw_output_hashes", {}).items():
        hashes[path] = sha(path) == expected
    hashes[str(directory / "constant-data-benefit-spike.jar")] = (
        sha(directory / "constant-data-benefit-spike.jar") == proof["artifact_sha256"])
    phases = [item for item in markers if item["event"] == "phase_finished"]
    controls = [item for item in markers if item["event"] == "controls"]
    completed = [item for item in markers if item["event"] == "complete"]
    fill = [item for item in markers if item["event"] == "cache_filled"]
    blocks = [item for item in markers if item["event"] == "block"]
    paths = [item for item in markers if item["event"] == "path_counts"]
    gc = [item for item in markers if item["event"] == "gc_after"]
    samples = [item for item in markers if item["event"] == "heap_sample"]
    phase_starts = [item for item in markers if item["event"] == "phase_started"]
    counts = collections.Counter()
    reasons = collections.Counter()
    versions = set()
    invalid_event_lines = 0
    active_blocks = {}
    maximum_published = 0
    publication_metrics = []
    for path in (directory / "events").rglob("*"):
        if path.is_file() and not path.name.startswith("."):
            for line in path.read_text().splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    invalid_event_lines += 1
                    continue
                counts[event["Event"]] += 1
                if event["Event"] == "SparkListenerBlockUpdated":
                    status = event["Block Updated Info"]
                    block_id = status["Block ID"]
                    if block_id.startswith("rdd_"):
                        disk_bytes = status["Disk Size"]
                        if disk_bytes > 0:
                            active_blocks[block_id] = disk_bytes
                            publication_metrics.append({"block": block_id, "disk_bytes": disk_bytes})
                        else:
                            active_blocks.pop(block_id, None)
                        maximum_published = max(maximum_published, len(active_blocks))
                if event["Event"] == "SparkListenerLogStart":
                    versions.add(event["Spark Version"])
                if event["Event"] == "SparkListenerTaskEnd":
                    reasons[event["Task End Reason"]["Reason"]] += 1
    oom = ("OutOfMemoryError: Java heap space" in raw or
           "OutOfMemoryError: GC overhead limit exceeded" in raw)
    outcome = ("complete" if process["exit_code"] == 0 and len(completed) == 1 else
               "heap_oom" if oom else "timeout" if process["timed_out"] else "unexpected_failure")
    expected_bytes = 96 * 1024**3
    expected_records = 98304 // proof["record_mib"]
    expected_per_partition = expected_records // 16
    controls_valid = False
    if len(controls) == 1:
        control = controls[0]
        controls_valid = (control["mode"] == proof["mode"]
            and control["record_bytes"] == proof["record_mib"] * 1024 * 1024
            and control["records_per_partition"] == expected_per_partition
            and control["logical_rows"] == 384 and control["unit_bytes"] == 256 * 1024**2
            and control["total_payload_bytes"] == expected_bytes and control["partitions"] == 16
            and control["task_threads"] == 8 and control["storage"] == "DISK_ONLY"
            and 40 * 1024**3 <= control["heap_max_bytes"] <= 48 * 1024**3
            and set(control["collectors"].split(",")) == {"PS Scavenge", "PS MarkSweep"})
    path_valid = False
    if len(paths) == 1:
        expected_reads = expected_records * (proof["measured_reads"] + 2)
        if proof["mode"] == "codec":
            path_valid = (paths[0]["java_writes"] == 0 and paths[0]["java_reads"] == 0
                          and paths[0]["codec_writes"] == expected_records
                          and paths[0]["codec_reads"] == expected_reads)
        else:
            path_valid = (paths[0]["codec_writes"] == 0 and paths[0]["codec_reads"] == 0
                          and paths[0]["java_writes"] == expected_records
                          and paths[0]["java_reads"] == expected_reads)
    correct = False
    if outcome == "complete":
        expected_phases = ["fill", "warmup"] + ["read_" + str(i)
                                               for i in range(1, proof["measured_reads"] + 1)]
        correct = (
            [item["phase"] for item in phases] == expected_phases
            and all(item["validated_records"] == expected_records
                    and item["validated_logical_rows"] == 384
                    and item["validated_payload_bytes"] == expected_bytes for item in phases)
            and len(fill) == 1 and fill[0]["generated_records"] == expected_records
            and fill[0]["sql_rows"] == 384 and fill[0]["sql_size_bytes"] == expected_bytes
            and fill[0]["generated_payload_bytes"] == expected_bytes
            and fill[0]["filled_partitions"] == 16
            and all(item["payload_records"] == expected_per_partition
                    and item["payload_bytes"] == 6 * 1024**3 for item in blocks)
            and len(blocks) == 16 and {item["partition"] for item in blocks} == set(range(16))
            and sum(item["disk_bytes"] for item in blocks) == fill[0]["total_disk_bytes"]
            and path_valid and maximum_published == 16
            and counts["SparkListenerApplicationEnd"] == 1
            and set(reasons) == {"Success"} and invalid_event_lines == 0)
    valid = (all(hashes.values()) and bool(proof.get("raw_output_hashes"))
             and controls_valid and versions == {"3.5.5"}
             and (correct if outcome == "complete" else outcome == "heap_oom"))
    return {
        "valid": valid, "outcome": outcome, "mode": proof["mode"],
        "record_mib": proof["record_mib"], "payload_bytes_if_complete": expected_bytes,
        "controls": controls, "controls_valid": controls_valid, "path_valid": path_valid,
        "phase_metrics": phases, "cache_filled": fill,
        "last_started_phase": phase_starts[-1]["phase"] if phase_starts else None,
        "heap_progress_samples": samples,
        "observed_sampled_heap_peak_bytes": max(
            [item["global_sampled_heap_peak_bytes"] for item in samples] +
            [item["sampled_heap_peak_bytes"] for item in phases] +
            [item["global_sampled_heap_peak_bytes"] for item in paths], default=None),
        "maximum_published_rdd_blocks": maximum_published,
        "published_block_metrics": publication_metrics,
        "block_metrics": blocks, "path_counts": paths,
        "gc_post_event_count": len(gc),
        "gc_post_occupancy_peak_bytes": max(
            (event["after_heap_occupancy_bytes"] for event in gc), default=None),
        "event_counts": dict(counts), "task_end_reasons": dict(reasons),
        "invalid_event_lines": invalid_event_lines,
        "all_hashes_match": all(hashes.values()), "hashes_checked": len(hashes),
        "process": process, "completed_correctness": correct,
        "limitations": [
            "Synthetic padded seed cache format, not Parquet, PCBS or GPU compute",
            "DISK_ONLY isolates serialization retention; no MemoryAndDisk/unroll fix claimed",
            "Heap peaks/post-GC are occupancy samples, not a measured live set",
            "One JVM per case, warm filesystem/JIT effects; no statistical confidence estimate"],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    result = verify(args.run_dir)
    (args.run_dir / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    raise SystemExit(0 if result["valid"] else 1)
