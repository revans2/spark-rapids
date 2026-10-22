#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Compile and audit one fresh-JVM, CPU-only scheduler-token mechanism spike."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import time


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def audit_text(raw):
    markers = [line for line in raw.splitlines() if line.startswith("TOKEN_SPIKE ")]
    require(markers.count("TOKEN_SPIKE ALL_MECHANISM_CHECKS_PASS") == 1,
            "Missing or duplicate final mechanism marker")
    for aqe in ("false", "true"):
        for name in ("ACTUAL_OWNER_OK", "SCHEDULER_CACHE_BOUNDARY_PASS",
                     "TOKEN_MISS_RECOMPUTE_PASS", "CLEANUP_PASS"):
            prefix = "TOKEN_SPIKE " + name + " aqe=" + aqe
            require(sum(line == prefix or line.startswith(prefix + " ") for line in markers) == 1,
                    "Missing/duplicate marker " + prefix)
        for label in ("fill", "tokens-intact-shuffle-missing",
                      "token-and-payload-removed", "payload-missing-token-present"):
            expected = "TOKEN_SPIKE READ_OK aqe=" + aqe + " label=" + label + " rows=10000"
            require(markers.count(expected) == 1, "Missing/duplicate read marker " + expected)
        recovery = [line for line in markers if line.startswith(
            "TOKEN_SPIKE PAYLOAD_ONLY_MISS_RECOVERED aqe=" + aqe + " ")]
        require(len(recovery) == 1 and "fetchFailedTasks=1 retryFallback=true" in recovery[0],
                "Payload-only miss must record the observed single-FetchFailed retry fallback")
    require(sum(bool(re.fullmatch(r"TOKEN_SPIKE EMPTY_PARTITIONS_PASS count=[4-6]", line))
                for line in markers) == 1, "Missing empty-partition acceptance")
    events = [json.loads(line[len("TOKEN_SPIKE_JSON "):])
              for line in raw.splitlines() if line.startswith("TOKEN_SPIKE_JSON ")]
    require(len(events) == 30, "Missing/extra structured diagnostic events")
    for aqe in (False, True):
        selected = [event for event in events if event.get("aqe") == aqe]
        stats = [event for event in selected if event["event"] == "initial_stats"]
        require(len(stats) == 1, "Missing/duplicate initial statistics")
        stat = stats[0]
        require(stat["row_count"] == 10000 and stat["encoded_bytes"] == 80000
                and stat["partitions"] == 6
                and stat["owner_class"].endswith(".TokenOwnerRDD"),
                "Incorrect owner/statistics metadata")
        expected_plan = "AdaptiveSparkPlanExec" if aqe else "ShuffleExchangeExec"
        require(stat["cached_plan_class"].endswith("." + expected_plan),
                "AQE fixture did not use expected physical cached plan")
        tokens = [event for event in selected if event["event"] == "token"]
        require(len(tokens) == 6 and {event["partition"] for event in tokens} == set(range(6)),
                "Missing/duplicate token partitions")
        require(all(event["owner"] == stat["owner"] and event["token_mem_bytes"] == 56
                    and event["locations"] == 1
                    and event["token_class"].endswith(".SchedulerToken") for event in tokens),
                "Unexpected physical token identity/size/location")
        require(sum(event["native_rows"] for event in tokens) == 10000
                and sum(event["native_bytes"] for event in tokens) == 80000,
                "Token/native row-byte metadata differs")
        reads = {event["label"]: event for event in selected if event["event"] == "read"}
        require(set(reads) == {"fill", "tokens-intact-shuffle-missing",
                              "token-and-payload-removed", "payload-missing-token-present"},
                "Missing read diagnostic")
        initial = reads["fill"]
        intact = reads["tokens-intact-shuffle-missing"]
        require(intact["hits"] > initial["hits"]
                and intact["upstream_computes"] == initial["upstream_computes"]
                and intact["producer_computes"] == initial["producer_computes"],
                "Upstream/producer recomputed despite intact tokens")
        require(reads["token-and-payload-removed"]["upstream_computes"]
                > intact["upstream_computes"], "Missing token did not permit upstream recompute")
        boundary = [event for event in selected if event["event"] == "scheduler_boundary"]
        require(len(boundary) == 1
                and boundary[0]["upstream_before"] == boundary[0]["upstream_after"],
                "Scheduler boundary diagnostic failed")
        negative = [event for event in selected if event["event"] == "payload_only_miss"]
        require(len(negative) == 1 and negative[0]["fetch_failed_tasks"] == 1
                and negative[0]["retry_fallback"] is True,
                "Failure-free recovery must not be claimed")
    erased = [event for event in events if event["event"] == "shuffle_erased"]
    require(len(erased) == 4 and all(
        event["data_files_before"] == 4 and event["data_files_after"] == 0
        and event["outputs_before"] == 4 and event["outputs_after"] == 0 for event in erased),
        "Physical shuffle-file/tracker invalidation was not established")
    return {"markers": markers, "json_events": events}


def audit_spark_events(directory, version):
    counts = collections.Counter()
    reasons = collections.Counter()
    versions = set()
    files = [path for path in directory.rglob("*")
             if path.is_file() and not path.name.startswith(".")]
    require(files, "No Spark event log")
    for path in files:
        for line in path.read_text().splitlines():
            event = json.loads(line)
            kind = event["Event"]
            counts[kind] += 1
            if kind == "SparkListenerLogStart":
                versions.add(event["Spark Version"])
            if kind == "SparkListenerTaskEnd":
                reason = event["Task End Reason"]["Reason"]
                if reason != "Success":
                    reasons[reason] += 1
    require(versions == {version}, "Unexpected executed Spark version " + repr(versions))
    require(counts["SparkListenerApplicationEnd"] == 1, "Application did not finish")
    require(reasons == {"FetchFailed": 2}, "Unexpected task failures " + repr(reasons))
    return {"spark_versions": sorted(versions), "event_counts": dict(counts),
            "non_success_task_ends": dict(reasons)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-home", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path,
                        help="New, non-existing directory; all raw evidence is retained")
    args = parser.parse_args()
    source_dir = Path(__file__).resolve().parent
    home = args.spark_home.resolve()
    output = args.output_dir.resolve()
    jars_dir = next((path for path in (home / "jars",
                    home / "assembly/target/scala-2.12/jars") if path.is_dir()), None)
    require(jars_dir is not None, "No runtime or assembled Spark jars directory")
    jars = sorted(jars_dir.glob("*.jar"))
    cores = [path for path in jars if re.fullmatch(r"spark-core_2\.(12|13)-[^/]+\.jar", path.name)]
    require(len(cores) == 1, "Cannot establish unique Spark core artifact")
    version = re.fullmatch(r"spark-core_2\.(12|13)-(.+)\.jar", cores[0].name).group(2)
    require(version in ("3.5.5", "4.0.1"), "Only tested Spark3.5.5/4.0.1 are supported")
    suffix = "355" if version == "3.5.5" else "401"
    sources = [source_dir / "TokenCacheSpike.scala",
               source_dir / ("CacheProbeCompatibility" + suffix + ".scala")]
    require(all(path.is_file() for path in sources), "Missing spike sources")
    output.mkdir(parents=True, exist_ok=False)
    copied = output / "sources"
    copied.mkdir()
    for path in sources:
        shutil.copyfile(path, copied / path.name)
    classes = output / "classes"
    classes.mkdir()
    events_dir = output / "events"
    events_dir.mkdir()
    artifact = output / "token-cache-spike.jar"
    cp = ":".join(str(path) for path in jars)
    compile_commands = [
        ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
         "-d", str(classes)] + [str(copied / path.name) for path in sources],
        ["jar", "cf", str(artifact), "-C", str(classes), "."],
    ]
    conf = {
        "spark.plugins": "", "spark.sql.extensions": "",
        "spark.sql.cache.serializer":
            "org.apache.spark.sql.tokencachespike.TokenCachedBatchSerializer",
        "spark.eventLog.enabled": "true", "spark.eventLog.dir": events_dir.as_uri(),
        "spark.eventLog.compress": "false", "spark.eventLog.rolling.enabled": "false",
        "spark.local.dir": str(output / "local"), "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "6",
    }
    command = [str(home / "bin/spark-submit"), "--class",
               "org.apache.spark.sql.tokencachespike.TokenCacheSpike",
               "--master", "local[4]", "--driver-memory", "1g"]
    for key, value in conf.items():
        command += ["--conf", key + "=" + value]
    command.append(str(artifact))
    runtime = [{"path": str(path), "bytes": path.stat().st_size,
                "sha256": sha256(path)} for path in jars]
    manifest = {"spark_home": str(home), "spark_version": version,
                "sources": [{"name": path.name, "sha256": sha256(path)} for path in sources],
                "runner_sha256": sha256(__file__), "runtime_artifacts": runtime,
                "compile_argv": compile_commands, "run_argv": command,
                "mechanism_only": True}
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    result = {"valid": False, "spark_version": version}
    start = time.monotonic()
    try:
        with (output / "compile.log").open("w") as log:
            for argv in compile_commands:
                subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                               timeout=180, check=True)
        manifest["artifact_sha256"] = sha256(artifact)
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        with (output / "run.log").open("w") as log:
            completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, timeout=240)
        result["exit_code"] = completed.returncode
        raw = (output / "run.log").read_text()
        require(completed.returncode == 0, "Spark process failed; inspect run.log")
        result.update(audit_text(raw))
        result["spark_event_audit"] = audit_spark_events(events_dir, version)
        try:
            audit_text(raw.replace("TOKEN_SPIKE ALL_MECHANISM_CHECKS_PASS", "", 1))
        except ValueError:
            result["missing_required_marker_rejected"] = True
        else:
            raise ValueError("Negative missing-marker acceptance check failed")
        require(sha256(artifact) == manifest["artifact_sha256"], "Artifact mutated during run")
        require(all(sha256(Path(item["path"])) == item["sha256"] for item in runtime),
                "Runtime artifacts changed")
        result.update(valid=True, artifact_sha256=manifest["artifact_sha256"])
    except Exception as error:
        result["failure"] = repr(error)
    result["wall_s"] = time.monotonic() - start
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"output": str(output), "spark_version": version,
                      "valid": result["valid"], "wall_s": result["wall_s"],
                      "failure": result.get("failure")}), flush=True)
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
