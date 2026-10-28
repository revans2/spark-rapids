#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Phase/thread CPU and wall-stack analysis for the isolated real Spark cache probe."""
import argparse
from collections import Counter, defaultdict
import hashlib
import json
from pathlib import Path
import subprocess


READER = "org.apache.spark.sql.cacheexploration.CpuProfileReader"


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            value.update(block)
    return value.hexdigest()


def role(name, frames):
    joined = "\n".join(frames)
    task_stack = "org.apache.spark.executor.Executor$TaskRunner.run" in joined
    executor_name = name.startswith("Executor task launch worker")
    if task_stack and executor_name:
        return "task"
    if task_stack:
        return "task_stack_only"
    if executor_name:
        return "executor_without_task_stack"
    if "collectToPython" in joined or ("org.apache.spark.sql.Dataset.collect" in joined
                                      and "py4j." in joined):
        return "driver_query"
    if "CpuPhaseProfiler" in joined or "one.profiler." in joined or "Async-profiler" in name:
        return "profiler"
    if "CompilerThread" in name or "CompileBroker" in joined or "C2 Compiler" in name:
        return "compiler"
    if name.startswith(("G1 ", "GC Thread", "G1 Conc", "ZGC", "Shenandoah")):
        return "gc"
    if "org.apache.spark.scheduler." in joined or name.startswith(
            ("dag-scheduler", "task-result-getter", "dispatcher-", "map-output-dispatcher")):
        return "scheduler_or_rpc"
    if "py4j." in joined:
        return "py4j_other"
    if any(value in joined for value in (
            "ThreadPoolExecutor.getTask", "LinkedBlockingQueue.take", "DelayedWorkQueue.take")):
        return "idle_pool"
    if name.startswith(("VM Thread", "VM Periodic", "Service Thread", "Monitor Deflation",
                        "Reference Handler", "Finalizer", "Signal Dispatcher", "Sweeper thread")):
        return "jvm_service"
    return "other"


def ranked(counter, limit=16):
    total = sum(counter.values())
    return [{"name": name, "samples": count,
             "share_of_this_table": count / total if total else 0}
            for name, count in counter.most_common(limit)]


def aggregate(rows):
    buckets = {}
    for row in rows:
        name = row.get("java_name") or row.get("os_name") or ""
        category = role(name, row["frames"])
        key = (row["sampler"], category)
        if key not in buckets:
            buckets[key] = {"samples": 0, "states": Counter(), "leaves": Counter(),
                            "inclusive": Counter(), "stacks": Counter(), "threads": Counter(),
                            "truncated_samples": 0}
        bucket = buckets[key]
        count = row["count"]
        bucket["samples"] += count
        bucket["states"][row["state"]] += count
        bucket["threads"][(row["java_id"], row["os_id"], name)] += count
        bucket["truncated_samples"] += count if row["truncated"] else 0
        bucket["leaves"][row["frames"][0] if row["frames"] else "<missing stack>"] += count
        for frame in set(row["frames"]):
            bucket["inclusive"][frame] += count
        bucket["stacks"]["\n".join(row["frames"]) or "<missing stack>"] += count
    result = []
    for (sampler, category), bucket in sorted(buckets.items()):
        result.append({
            "sampler": sampler, "role": category, "samples": bucket["samples"],
            "states": dict(bucket["states"]), "truncated_samples": bucket["truncated_samples"],
            "top_leaves": ranked(bucket["leaves"]),
            "top_inclusive_frames": [
                {"name": name, "samples": count,
                 "share_of_bucket_samples": count / bucket["samples"]}
                for name, count in bucket["inclusive"].most_common(24)],
            "top_stacks": ranked(bucket["stacks"]),
            "threads": [{"java_id": key[0], "os_id": key[1], "name": key[2], "samples": count}
                        for key, count in bucket["threads"].most_common()]})
    return result


def counter_delta(before, after, key):
    a, b = before.get(key), after.get(key)
    return b-a if a is not None and b is not None and a >= 0 and b >= a else None


def cpu_counters(phase, rows):
    before, after = phase["before"], phase["after"]
    start = {row["id"]: row for row in before["threads"]}
    stop = {row["id"]: row for row in after["threads"]}
    sampled_roles = defaultdict(Counter)
    for row in rows:
        if row["java_id"]:
            name = row.get("java_name") or row.get("os_name") or ""
            sampled_roles[row["java_id"]][role(name, row["frames"])] += row["count"]
    changes = []
    unavailable = []
    role_cpu = Counter()
    matched = 0
    for thread_id in sorted(start.keys() & stop.keys()):
        a, b = start[thread_id], stop[thread_id]
        cpu = counter_delta(a, b, "cpu_nanos")
        user = counter_delta(a, b, "user_nanos")
        if cpu is None:
            unavailable.append(thread_id)
            continue
        matched += cpu
        name = b["name"] or a["name"]
        # Counter roles describe whole thread CPU within the window. Stack roles are finer grained.
        category = ("executor_worker" if name.startswith("Executor task launch worker")
                    else sampled_roles[thread_id].most_common(1)[0][0]
                    if sampled_roles[thread_id] else role(name, []))
        role_cpu[category] += cpu
        changes.append({"id": thread_id, "before_name": a["name"], "after_name": b["name"],
                        "role": category, "cpu_ms": cpu/1e6,
                        "user_ms": user/1e6 if user is not None else None,
                        "sample_roles": dict(sampled_roles[thread_id])})
    process = counter_delta(before, after, "process_cpu_nanos")
    gc_before = {row["name"]: row for row in before.get("gc", [])}
    gc = []
    for row in after.get("gc", []):
        if row["name"] in gc_before:
            gc.append({"name": row["name"],
                       "collections": counter_delta(gc_before[row["name"]], row, "count"),
                       "collection_time_ms": counter_delta(gc_before[row["name"]], row, "time_ms")})
    return {
        "process_cpu_ms": process/1e6 if process is not None else None,
        "matched_java_thread_cpu_ms": matched/1e6,
        "process_minus_matched_java_cpu_ms": (process-matched)/1e6 if process is not None else None,
        "role_cpu_ms": {key: value/1e6 for key, value in sorted(role_cpu.items())},
        "threads": sorted(changes, key=lambda row: row["cpu_ms"], reverse=True),
        "new_threads_after": [stop[key] for key in sorted(stop.keys()-start.keys())],
        "threads_missing_after": [start[key] for key in sorted(start.keys()-stop.keys())],
        "unavailable_cpu_counter_ids": unavailable,
        "gc": gc,
        "compilation_time_delta_ms": counter_delta(before, after, "compilation_time_ms"),
        "loaded_classes_delta": counter_delta(before, after, "loaded_classes"),
        "snapshot_duration_before_ns": before.get("snapshot_duration_nanos"),
        "snapshot_duration_after_ns": after.get("snapshot_duration_nanos")}


def read_phase(phase, jar):
    start = phase.get("sample_start_epoch_nanos", phase["before"]["epoch_nanos"])
    end = phase.get("sample_end_epoch_nanos", phase["after"]["epoch_nanos"])
    command = ["java", "-Xmx512m", "-cp", str(jar), READER, phase["file"], str(start), str(end)]
    rows = []
    metadata = None
    with subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                          text=True) as process:
        for line in process.stdout:
            row = json.loads(line)
            if row["type"] == "metadata":
                metadata = row
            else:
                rows.append(row)
        stderr = process.stderr.read()
        code = process.wait()
    if code:
        raise RuntimeError("JFR reader failed: " + stderr)
    if metadata is None or metadata["included"] != sum(row["count"] for row in rows):
        raise ValueError("JFR reader count mismatch")
    if metadata["included"] == 0:
        raise ValueError("No samples within action " + phase["name"])
    return rows, metadata


def summarize(run, build):
    raw = json.loads((run / "result.json").read_text())
    if raw.get("status") != "PASS":
        raise ValueError("Refuse a failed/incomplete benchmark result")
    phases = raw.get("cpu_phases")
    if not phases:
        raise ValueError("No cpu_phases in result.json")
    jar = build / "nvcomp-spark-exploration.jar"
    results = []
    grouped = defaultdict(list)
    group_counters = defaultdict(list)
    for phase in phases:
        options = phase["profiler"]["options"]
        version = phase["profiler"]["version"].strip()
        if not version.startswith("3.0") or "event=ctimer" not in options or "wall=5ms" not in options:
            raise ValueError("Reader mapping requires recorded async-profiler3 CPU+wall5ms configuration")
        rows, metadata = read_phase(phase, jar)
        counters = cpu_counters(phase, rows)
        start = phase.get("sample_start_epoch_nanos", phase["before"]["epoch_nanos"])
        end = phase.get("sample_end_epoch_nanos", phase["after"]["epoch_nanos"])
        if not phase["before"]["epoch_nanos"] <= start < end <= phase["after"]["epoch_nanos"]:
            raise ValueError("Sample window outside CPU counter snapshots")
        if phase["name"] == "population":
            collect_ms = raw.get("population_ns")
        else:
            _, mode, projection, iteration = phase["name"].split("/")
            matches = [sample["elapsed_ns"] for sample in raw.get("reads", [])
                       if sample["requested_mode"] == mode
                       and sample["narrow"] == (projection == "id-only")
                       and sample["iteration"] == int(iteration)]
            if len(matches) > 1:
                raise ValueError("Ambiguous collect timer for " + phase["name"])
            collect_ms = matches[0] if matches else None
        collect_ms = collect_ms/1e6 if collect_ms is not None else None
        counter_wall = (phase["after"]["epoch_nanos"]-phase["before"]["epoch_nanos"])/1e6
        item = {"name": phase["name"], "file": phase["file"],
                "jfr_sha256": digest(Path(phase["file"])), "window_wall_ms": (end-start)/1e6,
                "collect_timer_ms": collect_ms, "counter_window_wall_ms": counter_wall,
                "counter_minus_sample_window_ms": counter_wall-(end-start)/1e6,
                "sample_window_minus_collect_ms": (end-start)/1e6-collect_ms if collect_ms is not None else None,
                "window_start_epoch_ns": start, "window_end_epoch_ns": end,
                "reader": metadata, "cpu_counters": counters, "samples": aggregate(rows)}
        results.append(item)
        name = phase["name"]
        if name == "population":
            group = "population"
        else:
            try:
                iteration = int(name.rsplit("/", 1)[1])
            except (IndexError, ValueError):
                raise ValueError("Unknown action phase name " + name)
            if iteration < 0:
                continue
            group = name.rsplit("/", 1)[0]
        grouped[group].extend(rows)
        group_counters[group].append(item)
    pooled = []
    for name, rows in sorted(grouped.items()):
        items = group_counters[name]
        role_cpu = Counter()
        for item in items:
            role_cpu.update(item["cpu_counters"]["role_cpu_ms"])
        pooled.append({"name": name, "actions": len(items),
                       "total_window_wall_ms": sum(item["window_wall_ms"] for item in items),
                       "total_process_cpu_ms": (sum(item["cpu_counters"]["process_cpu_ms"] for item in items)
                                                if all(item["cpu_counters"]["process_cpu_ms"] is not None
                                                       for item in items) else None),
                       "role_cpu_ms": dict(role_cpu), "samples": aggregate(rows)})
    return {
        "run": str(run.resolve()), "result_sha256": digest(run / "result.json"),
        "reader_jar_sha256": digest(jar), "analyzer_sha256": digest(Path(__file__)),
        "scope": "Separate diagnostic recordings of the actual Spark cache job. Samples are filtered "
                 "exactly to declared tight timestamp anchors, which include small Py4J boundary "
                 "overhead around the collect timer. CPU counters cover a slightly wider snapshot "
                 "window, reported separately. Profiler start/stop are excluded. async-profiler3 "
                 "STATE_DEFAULT is CPU ctimer sampling; RUNNABLE/SLEEPING are wall samples and "
                 "do not establish CPU execution. Counts are samples, not elapsed percentages. "
                 "CPU counters are thread/process CPU time, not wall time, and overlap across "
                 "concurrent threads. CPU delta reconciliation is approximate because snapshots "
                 "are sequential; native JVM/GC and new/missing threads may be untracked. GC "
                 "collection time and compilation counters are coarse, can overlap, and are "
                 "not additive CPU components. Inclusive frame counts overlap by definition. "
                 "Role assignment is stack/name-based, never a claim of exclusive subsystem time.",
        "state_mapping_evidence": {
            "version": "async-profiler v3.0",
            "cpu_default": "CpuEngine::signalHandler uses ExecutionEvent default THREAD_UNKNOWN",
            "wall": "WallClock::signalHandler sets THREAD_RUNNING/THREAD_SLEEPING for idle-thread sampling",
            "jfr_encoding": "flightRecorder.cpp writeThreadStates maps UNKNOWN/RUNNING/SLEEPING to DEFAULT/RUNNABLE/SLEEPING"},
        "phases": results, "pooled_measured_actions": pooled}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run", type=Path)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output must be a new file")
    result = summarize(args.run, args.build_dir)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    for group in result["pooled_measured_actions"]:
        print(json.dumps({key: value for key, value in group.items() if key != "samples"}))


if __name__ == "__main__":
    main()
