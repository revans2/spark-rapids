#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Verify the small pruning proof and summarize its Nsight GPU activities.

Export profile.nsys-rep with nsys export --type sqlite first.
This checks feasibility and activity counts, not Spark throughput or spill behavior.
"""
import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sqlite3


def require(value, message):
    if not value:
        raise RuntimeError(message)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    completion = json.loads((output / "completion.json").read_text())
    require(completion["returncode"] == 0 and completion["stop_reason"] is None,
            "Probe did not complete")
    provenance = json.loads((output / "provenance.json").read_text())
    for entry in provenance["files"]:
        require(digest(entry["path"]) == entry["sha256"],
                "Changed research dependency: " + entry["path"])
    manifest = Path(provenance["spark_build"]) / "spark-provenance.json"
    require(digest(manifest) == provenance["spark_manifest_sha256"], "Changed Spark manifest")
    for name in ["run.py", "ColumnPruningProbe.java", "column_slice_jni.cpp"]:
        source = Path(__file__).parent / name
        require(source.read_bytes() == (output / name).read_bytes(), "Changed source: " + name)
    lines = (output / "stdout.log").read_text().splitlines()
    require(sum(line.startswith("PASS column-slice feasibility") for line in lines) == 1,
            "Missing successful value/null/order and allocation checks")
    results = json.loads((output / "results.json").read_text())
    require(results == [json.loads(line) for line in lines if line.startswith("{")],
            "Results disagree with application stdout")
    require(len(results) == 39, "Missing observations")
    count_only = [r for r in results if r.get("selection") == "count-only"]
    require(len(count_only) == 7 and all(
        r["uploaded_bytes"] == r["decoded_bytes"] == r["native_calls"] == 0 for r in count_only),
        "Count-only materialized data")
    for row in results:
        if "requested_ordinals" not in row:
            continue
        require(len(set(row["requested_ordinals"])) == row["selected_unique"],
                "Incorrect projection deduplication")
        require(len(row["requested_ordinals"]) == row["requested_columns"],
                "Incorrect output width")

    connection = sqlite3.connect((output / "profile.sqlite").as_uri() + "?mode=ro", uri=True)
    ranges = connection.execute("""
        SELECT n.start, n.end, COALESCE(n.text, s.value), n.globalTid
        FROM NVTX_EVENTS n LEFT JOIN StringIds s ON n.textId = s.id
        WHERE n.end IS NOT NULL
        ORDER BY n.start
        """).fetchall()
    activities = []
    for start, end, label, thread in ranges:
        if not label or not (label in ["column-index", "assemble-projection"] or
                label.startswith(("whole-record-compress:", "batched-column-compress:"))):
            continue
        # Nsight encodes PID in the high bits; bound attribution to this process and interval.
        pid = (thread >> 24) << 24
        kernels, crossing = connection.execute("""
            SELECT COUNT(*), COALESCE(SUM(start < ? OR end > ?), 0)
            FROM CUPTI_ACTIVITY_KIND_KERNEL
            WHERE globalPid = ? AND start < ? AND end > ?
            """, (start, end, pid, end, start)).fetchone()
        copies = connection.execute("""
            SELECT copyKind, COUNT(*), SUM(bytes)
            FROM CUPTI_ACTIVITY_KIND_MEMCPY
            WHERE globalPid = ? AND start < ? AND end > ?
            GROUP BY copyKind ORDER BY copyKind
            """, (pid, end, start)).fetchall()
        activities.append(dict(label=label, kernels=kernels, boundary_crossings=crossing,
                               copies=copies))
    counts = Counter(a["label"] for a in activities)
    require(counts["column-index"] == 14 and counts["assemble-projection"] == 64,
            "Missing indexing/assembly trace ranges")
    for activity in activities:
        require(activity["boundary_crossings"] == 0, "Ambiguous kernel attribution")
        if activity["label"] in ["column-index", "assemble-projection"]:
            require(activity["kernels"] == 0 and not activity["copies"],
                    "Indexing/assembly performed GPU work")
    comparison = []
    for full in results:
        if full.get("selected_unique") != 38 or full.get("cpu_encoded"):
            continue
        codec = full["codec"]
        selected = next(r for r in results if r["codec"] == codec and
                        r.get("requested_ordinals") == [0] and not r.get("cpu_encoded"))
        whole = [a for a in activities if a["label"] == "whole-record-compress:" + codec]
        column = [a for a in activities if a["label"] == "batched-column-compress:" + codec]
        require(len(whole) == len(column) == 1, "Missing compression range: " + codec)
        require(whole[0]["kernels"] == column[0]["kernels"],
                "Compression kernel count increased: " + codec)
        require(full["whole_encode_calls"] == full["encode_calls"],
                "Native submission count increased: " + codec)
        comparison.append(dict(codec=codec, whole_payload_bytes=full["whole_encoded_bytes"],
            column_payload_bytes=full["full_encoded_bytes"], id_payload_bytes=selected["uploaded_bytes"],
            whole_blocks=full["whole_encode_blocks"], column_blocks=full["encode_blocks"],
            whole_kernels=whole[0]["kernels"], column_kernels=column[0]["kernels"],
            whole_copies=whole[0]["copies"], column_copies=column[0]["copies"]))
    require(len(comparison) == 7, "Missing codecs")
    print(json.dumps(dict(scope="Feasibility only; payload excludes column directory and wire framing",
                          observations=len(results), index_ranges=14, assembly_ranges=64,
                          comparison=comparison), indent=2))


if __name__ == "__main__":
    main()
