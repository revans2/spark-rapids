#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Summarize separate Nsight diagnostics within real Spark action NVTX windows."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sqlite3


PREFIX = "cache-exploration/"
# Nsight serializes a global thread id as the global process id plus a 24-bit tid.
TID_BITS = 24


def union_ns(intervals):
    end = None
    total = 0
    for start, stop in sorted(intervals):
        if end is None or start > end:
            total += stop - start
        elif stop > end:
            total += stop - end
        end = max(end, stop) if end is not None else stop
    return total


def describe(events):
    return {"events": len(events),
            "sum_ms": sum(e["end"] - e["start"] for e in events) / 1e6,
            "union_ms": union_ns([(e["start"], e["end"]) for e in events]) / 1e6,
            "bytes": sum(e.get("bytes", 0) for e in events),
            "boundary_events": sum(e["boundary"] for e in events)}


def top_by_name(events):
    names = defaultdict(list)
    for event in events:
        names[event["name"]].append(event)
    return sorted(({"name": name, **describe(group)} for name, group in names.items()),
                  key=lambda row: row["sum_ms"], reverse=True)[:12]


def digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            value.update(block)
    return value.hexdigest()


def summarize(path):
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        required = {"NVTX_EVENTS", "StringIds", "CUPTI_ACTIVITY_KIND_KERNEL",
                    "CUPTI_ACTIVITY_KIND_MEMCPY", "ENUM_CUDA_MEMCPY_OPER"}
        if not required <= tables:
            raise ValueError("Missing Nsight trace tables: " + repr(sorted(required - tables)))
        strings = {row["id"]: row["value"] for row in db.execute("SELECT * FROM StringIds")}
        copy_names = {row["id"]: row["label"] for row in db.execute("SELECT * FROM ENUM_CUDA_MEMCPY_OPER")}
        phases = []
        for row in db.execute("SELECT start, end, text, textId, globalTid FROM NVTX_EVENTS"):
            name = row["text"] or strings.get(row["textId"], "")
            if name.startswith(PREFIX):
                if row["end"] is None or row["end"] <= row["start"]:
                    raise ValueError("Unclosed/empty phase " + name)
                phases.append({"name": name[len(PREFIX):], "start": row["start"],
                               "end": row["end"], "globalPid": (row["globalTid"] >> TID_BITS) << TID_BITS})
        if not phases:
            raise ValueError("No cache action phase ranges; check JVM NVTX option and trace")
        phases.sort(key=lambda phase: phase["start"])
        if any(a["end"] > b["start"] for a, b in zip(phases, phases[1:])):
            raise ValueError("Cache action ranges must not overlap")
        results = []
        for phase in phases:
            start, end, pid = phase["start"], phase["end"], phase["globalPid"]

            def events(table, thread=False):
                if table not in tables:
                    return []
                process = "(globalTid >> 24) << 24" if thread else "globalPid"
                rows = db.execute("SELECT * FROM " + table +
                                  " WHERE start < ? AND end > ? AND " + process + " = ?",
                                  (end, start, pid))
                result = []
                for row in rows:
                    event = dict(row)
                    event["boundary"] = row["start"] < start or row["end"] > end
                    event["start"] = max(start, row["start"])
                    event["end"] = min(end, row["end"])
                    result.append(event)
                return result

            kernels = events("CUPTI_ACTIVITY_KIND_KERNEL")
            if not kernels:
                raise ValueError("Phase has no traced GPU kernels: " + phase["name"])
            for event in kernels:
                event["name"] = strings.get(event["demangledName"], str(event["demangledName"]))
            copies = events("CUPTI_ACTIVITY_KIND_MEMCPY")
            memset = events("CUPTI_ACTIVITY_KIND_MEMSET")
            api = events("CUPTI_ACTIVITY_KIND_RUNTIME", thread=True)
            for event in api:
                event["name"] = strings.get(event["nameId"], str(event["nameId"]))
            groups = defaultdict(list)
            for event in copies:
                groups[copy_names.get(event["copyKind"], str(event["copyKind"]))].append(event)
            kernel_intervals = [(e["start"], e["end"]) for e in kernels]
            copy_intervals = [(e["start"], e["end"]) for e in copies]
            all_intervals = kernel_intervals + copy_intervals + [
                (e["start"], e["end"]) for e in memset]
            active = union_ns(all_intervals)
            kernel_copy_overlap = (union_ns(kernel_intervals) + union_ns(copy_intervals)
                                   - union_ns(kernel_intervals + copy_intervals))
            results.append({"phase": phase["name"], "wall_ms": (end-start)/1e6,
                            "kernels": describe(kernels),
                            "copies": {name: describe(group) for name, group in sorted(groups.items())},
                            "memsets": describe(memset),
                            "gpu_active_union_ms": active/1e6,
                            "kernel_copy_overlap_ms": kernel_copy_overlap/1e6,
                            "no_traced_gpu_activity_ms": (end-start-active)/1e6,
                            "cuda_runtime_api": describe(api),
                            "top_kernels": top_by_name(kernels),
                            "top_cuda_runtime_api": top_by_name(api)})
    return {"sqlite": str(path.resolve()), "sqlite_sha256": digest(path),
            "analyzer_sha256": digest(Path(__file__)),
            "scope": "Separate profiled run; whole Spark actions including generation during population "
                     "and query aggregation during reads. GPU events filtered to the phase JVM across "
                     "all its worker threads. Times clipped at phase boundaries; bytes count whole "
                     "overlapping copy events (boundary_events identifies any ambiguity). Sum times "
                     "can overlap across streams and exceed wall time. Union counts overlap once. "
                     "No-traced-GPU time includes CPU work, scheduling, GC and waiting; it is not CPU "
                     "execution time. CUDA traces do not measure ordinary CPU heap memcpy. Runtime "
                     "API time includes blocking/waits and overlaps GPU activity. Top kernels are "
                     "diagnostic names, not a claimed codec-only time.",
            "phases": results}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sqlite", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Output must be a new file")
    result = summarize(args.sqlite)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    for phase in result["phases"]:
        print(json.dumps({key: value for key, value in phase.items()
                          if key not in ("top_kernels", "top_cuda_runtime_api")}))


if __name__ == "__main__":
    main()
