#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Run one actual Spark cache experiment under the approved host budgets."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[3]
TOOLS = REPO / "tools/cache-exploration"
spec = importlib.util.spec_from_file_location("cache_process", TOOLS / "measure_process.py")
memory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(memory)
spec = importlib.util.spec_from_file_location("cache_guard", HERE.parent / "run.py")
guard = importlib.util.module_from_spec(spec)
spec.loader.exec_module(guard)
GIB = 1024**3
SERIALIZERS = {
    "uncompressed": "SparkNvcompUncompressedSerializer",
    "lz4": "SparkNvcompLz4Serializer", "zstd": "SparkNvcompZstdSerializer",
    "snappy": "SparkNvcompSnappySerializer", "ans": "SparkNvcompAnsSerializer",
    "cascaded": "SparkNvcompCascadedSerializer", "cascaded-byte": "SparkNvcompCascadedByteSerializer"}

def verify(manifest, build):
    recorded_scala = sorted(name for name in manifest["sources"]
                            if Path(name).parent == HERE and Path(name).suffix == ".scala")
    if sorted(str(p) for p in HERE.glob("*.scala")) != recorded_scala:
        raise RuntimeError("Spark prototype Scala source inventory changed")
    recorded_java = sorted(name for name in manifest["sources"]
                           if Path(name).parent == HERE and Path(name).suffix == ".java")
    if sorted(str(p) for p in HERE.glob("*.java")) != recorded_java:
        raise RuntimeError("Spark prototype Java source inventory changed")
    actual_spark_jars = sorted(str(p) for p in (Path(manifest["spark_home"]) / "jars").glob("*.jar")
                               if p.is_file())
    if actual_spark_jars != sorted(entry["path"] for entry in manifest["spark_dependencies"]):
        raise RuntimeError("Spark dependency inventory changed")
    entries = [manifest["application"]] + manifest["plugin_dependencies"] + manifest["spark_dependencies"]
    for entry in entries:
        if guard.digest(Path(entry["path"])) != entry["sha256"]:
            raise RuntimeError("Changed artifact: " + entry["path"])
    for filename, expected in manifest["sources"].items():
        if guard.digest(Path(filename)) != expected:
            raise RuntimeError("Changed source: " + filename)
    native = build / "build-provenance.json"
    if guard.digest(native) != manifest["native_build_manifest"]:
        raise RuntimeError("Changed native build manifest")
    for entry in json.loads(native.read_text())["files"]:
        if guard.digest(Path(entry["path"])) != entry["sha256"]:
            raise RuntimeError("Changed native dependency: " + entry["path"])

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--format", choices=["default", "pcbs"] + list(SERIALIZERS), required=True)
    parser.add_argument("--fixture", choices=["fixed", "fixed-wide", "strings-low", "strings-wide"],
                        default="fixed-wide")
    dataset = parser.add_mutually_exclusive_group()
    dataset.add_argument("--rows", type=int, default=None)
    dataset.add_argument("--task-mib", type=int, choices=[256, 512, 1024, 2048])
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--probe", action="store_true", help="Run bounded Scala correctness/CPU-rejection probe")
    parser.add_argument("--cpu-profile", action="store_true",
                        help="Separate diagnostic CPU/wall samples and JVM CPU counters per action")
    parser.add_argument("--profile", action="store_true",
                        help="Separate diagnostic run with Nsight CUDA/NVTX tracing; timings are perturbed")
    args = parser.parse_args()
    if args.task_mib and args.fixture != "fixed-wide":
        parser.error("Initial exact task-size model is fixed-wide; use explicit rows for strings")
    rows = args.rows if args.rows is not None else (
        args.task_mib * 1024**2 // 256 * 4 if args.task_mib else 1024)
    if rows < 1 or args.warmups < 0 or args.repeats < 1:
        parser.error("Invalid bounded rows/repetition counts")
    build = args.build_dir.resolve()
    manifest = json.loads((build / "spark-provenance.json").read_text())
    verify(manifest, build)
    gpu = guard.capture(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"])
    if gpu:
        raise RuntimeError("GPU already in use: " + gpu)
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = TOOLS / "spark_cache_bench.py"
    snapshot = output / "spark_cache_bench.py"
    snapshot.write_bytes(source.read_bytes())
    jars = ",".join([entry["path"] for entry in manifest["plugin_dependencies"]]
                    + [manifest["application"]["path"]])
    native_option = "-Dcacheexploration.cascaded.library=" + manifest["native_library"]
    if args.cpu_profile:
        if args.probe:
            parser.error("CPU profiling requires measured cache actions")
        native_option += " -Dap_loader_extraction_dir=" + str(output / "ap-native")
        native_option += (" -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints"
                          " -XX:+PreserveFramePointer")
    if args.profile:
        if args.probe:
            parser.error("Profile measured cache actions rather than the correctness probe")
        native_option += " -Dai.rapids.cudf.nvtx.enabled=true"
    command = [str(Path(manifest["spark_home"]) / "bin/spark-submit"),
               "--master", "local[2]", "--driver-memory", "16g",
               "--driver-java-options", "--add-opens=java.base/java.nio=ALL-UNNAMED " + native_option,
               "--jars", jars,
               "--conf", "spark.plugins=com.nvidia.spark.SQLPlugin",
               "--conf", "spark.executor.cores=2",
               "--conf", "spark.rapids.sql.concurrentGpuTasks=2",
               "--conf", "spark.rapids.memory.host.offHeapLimit.enabled=true",
               "--conf", "spark.rapids.memory.host.offHeapLimit.size=32g",
               "--conf", "spark.rapids.memory.pinnedPool.size=2g",
               "--conf", "spark.rapids.memory.gpu.allocSize=24g",
               "--conf", "spark.rapids.sql.enabled=true",
               "--conf", "spark.sql.session.timeZone=UTC",
               str(snapshot), "--format", args.format if args.format in ["pcbs", "default"] else "nvcomp",
               "--write-mode", "gpu", "--read-modes", "gpu",
               "--fixture", args.fixture, "--rows", str(rows), "--partitions", "4",
               "--warmups", str(args.warmups), "--repeats", str(args.repeats),
               "--storage", "dataframe-default", "--output", str(output / "result.json"),
               "--payload-audit-class", "org.apache.spark.sql.cacheexploration.NvcompPayloadAudit"]
    if args.cpu_profile:
        command += ["--cpu-profile-dir", str(output / "cpu")]
    if args.profile:
        command += ["--nvtx-phases"]
    if args.format in SERIALIZERS:
        command += ["--serializer-class", "org.apache.spark.sql.cacheexploration." + SERIALIZERS[args.format]]
    if args.task_mib:
        command += ["--task-size-mib", str(args.task_mib)]
    if args.probe:
        if args.format not in SERIALIZERS:
            parser.error("Correctness probe requires an nvCOMP prototype format")
        code = {"cascaded": "cascaded-typed", "cascaded-byte": "cascaded8"}.get(args.format, args.format)
        command = command[:command.index(str(snapshot))] + [
            "--class", "org.apache.spark.sql.cacheexploration.SparkNvcompProbe",
            manifest["application"]["path"], code]
    if args.profile:
        command = ["nsys", "profile", "--trace=cuda,nvtx", "--sample=none",
                   "--cpuctxsw=none", "--wait=all",
                   "--output=" + str(output / "profile")] + command
    environment = os.environ.copy()
    environment.update(SPARK_HOME=manifest["spark_home"], SPARK_LOCAL_IP="127.0.0.1",
                       PYSPARK_PYTHON=sys.executable, PYSPARK_DRIVER_PYTHON=sys.executable)
    launch = {"command": command, "build": str(build), "format": args.format,
              "profiled": args.profile or args.cpu_profile,
              "gpu_profiled": args.profile, "cpu_profiled": args.cpu_profile,
              "timing_scope": ("Diagnostic profiling; perturbed" if args.profile or args.cpu_profile
                               else "Unprofiled benchmark"),
              "rows": rows, "task_slots": 2, "partitions": 4,
              "rss_stop_bytes": 64*GIB, "heap_bytes": 16*GIB,
              "configured_host_budget_bytes": 32*GIB, "pinned_bytes": 2*GIB,
              "gpu_pool_cap_bytes": 24*GIB,
              "host_budget_scope": "Plugin reserves default per-task overhead within32GiB; record actual HostAlloc limit",
              "provenance": manifest, "launcher_sha256": guard.digest(Path(__file__))}
    (output / "launch.json").write_text(json.dumps(launch, indent=2) + "\n")
    started = time.monotonic()
    peak = 0
    reason = None
    with (output / "stdout.log").open("w") as stdout, (output / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(command, env=environment, stdout=stdout, stderr=stderr,
                                   start_new_session=True)
        try:
            with (output / "rss.jsonl").open("w") as samples:
                while process.poll() is None:
                    sample = memory.residency(memory.proc_tree(process.pid))
                    rss = sample["rss_bytes"] or 0
                    peak = max(peak, rss)
                    elapsed = time.monotonic()-started
                    samples.write(json.dumps({"elapsed_seconds": elapsed, **sample}) + "\n")
                    samples.flush()
                    if rss > 64*GIB or elapsed > args.timeout:
                        reason = "rss_guard" if rss > 64*GIB else "timeout"
                        break
                    time.sleep(0.25)
        except BaseException:
            reason = "launcher_exception"
            raise
        finally:
            guard.stop_process_group(process)
            returncode = process.wait()
            completion = {"returncode": returncode, "stop_reason": reason,
                          "elapsed_seconds": time.monotonic()-started,
                          "peak_sampled_tree_rss_bytes": peak}
            (output / "completion.json").write_text(json.dumps(completion, indent=2) + "\n")
    print(json.dumps({"output": str(output), **completion}), flush=True)
    if returncode or reason:
        print((output / "stderr.log").read_text()[-14000:], file=sys.stderr)
        raise SystemExit(1)
if __name__ == "__main__":
    main()
