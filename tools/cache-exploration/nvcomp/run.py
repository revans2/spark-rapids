#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Build isolated artifacts or run one GPU experiment with a sampled RSS stop guard."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
CUDF = Path("/home/roberte/.m2/repository/com/nvidia/cudf-spark-jni/26.12.0-SNAPSHOT/"
            "cudf-spark-jni-26.12.0-SNAPSHOT-cuda12.jar")
PLUGIN = REPO / ("sql-plugin/target/spark355/"
                 "rapids-4-spark-sql_2.12-26.12.0-SNAPSHOT-spark355.jar")
PLUGIN_API = REPO / ("sql-plugin-api/target/spark355/"
                     "rapids-4-spark-sql-plugin-api_2.12-26.12.0-SNAPSHOT-spark355.jar")
PLUGIN_DEPS = sorted((REPO / "dist/target/deps").glob("*.jar"))
SPARK_JARS = Path("/home/roberte/src/spark_3.5.5/assembly/target/scala-2.12/jars")
RSS_LIMIT = 64 * 1024**3

def capture(argv):
    return subprocess.run(argv, check=True, text=True, capture_output=True).stdout.strip()

def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")

def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()

def source_inventory():
    return {str(p.relative_to(REPO)): digest(p)
            for p in sorted(HERE.rglob("*"))
            if p.is_file() and "__pycache__" not in p.parts}

def artifact_inventory(directory):
    return {str(p.relative_to(directory)): digest(p)
            for p in sorted((directory / "classes").rglob("*.class"))} | {
                "libcascaded_exploration.so": digest(directory / "libcascaded_exploration.so")}

def stop_process_group(process):
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    # The leader may exit before a descendant that ignored TERM. Reap the entire group.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass

def spark_jars():
    return sorted(p for p in SPARK_JARS.glob("*.jar") if p.is_file())

def build(output):
    output = output.resolve()
    if output.exists():
        raise RuntimeError("Build output must be new")
    subprocess.run([sys.executable, str(HERE / "native/build.py"),
                    "--output", str(output)], check=True)
    classpath = os.pathsep.join([str(output / "classes"), str(CUDF), str(PLUGIN), str(PLUGIN_API)]
                                + [str(p) for p in PLUGIN_DEPS + spark_jars()])
    sources = sorted(HERE.glob("*.java"))
    subprocess.run(["javac", "-Xlint:all", "-cp", classpath, "-d",
                    str(output / "classes")] + [str(s) for s in sources], check=True)
    write_json(output / "probe-build.json", {
        "classpath": classpath,
        "java": subprocess.run(["java", "-version"], text=True, capture_output=True).stderr,
        "git_head": capture(["git", "-C", str(REPO), "rev-parse", "HEAD"]),
        "sources": source_inventory(),
        "artifacts": artifact_inventory(output),
        "native_build_manifest": digest(output / "build-provenance.json"),
        "runtime_jars": {str(p): digest(p)
                         for p in [CUDF, PLUGIN, PLUGIN_API] + PLUGIN_DEPS + spark_jars()},
        "spark_jar_inventory": [str(p) for p in spark_jars()],
    })
    print("BUILD", output)

def tree_pids(pid):
    result = [pid]
    try:
        children = Path(f"/proc/{pid}/task/{pid}/children").read_text().split()
    except (FileNotFoundError, ProcessLookupError):
        return result
    for child in children:
        result.extend(tree_pids(int(child)))
    return result

def sample_rss(pid):
    rss = 0
    for item in tree_pids(pid):
        try:
            values = Path(f"/proc/{item}/status").read_text().splitlines()
        except (FileNotFoundError, ProcessLookupError):
            continue
        for line in values:
            if line.startswith("VmRSS:"):
                rss += int(line.split()[1]) * 1024
    return rss

def run(build_dir, output, arguments, main_class):
    build_dir = build_dir.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads((build_dir / "probe-build.json").read_text())
    # A stale build must never silently stand in for changed sources.
    if source_inventory() != manifest["sources"]:
        raise RuntimeError("Source inventory changed since build")
    if artifact_inventory(build_dir) != manifest["artifacts"]:
        raise RuntimeError("Compiled artifacts changed since build")
    if [str(p) for p in spark_jars()] != manifest["spark_jar_inventory"]:
        raise RuntimeError("Spark dependency inventory changed")
    for filename, expected in manifest["runtime_jars"].items():
        if digest(Path(filename)) != expected:
            raise RuntimeError("Runtime jar changed since build: " + filename)
    native_manifest = build_dir / "build-provenance.json"
    if digest(native_manifest) != manifest["native_build_manifest"]:
        raise RuntimeError("Native build manifest changed")
    for entry in json.loads(native_manifest.read_text())["files"]:
        if digest(Path(entry["path"])) != entry["sha256"]:
            raise RuntimeError("Native dependency changed: " + entry["path"])
    gpu_processes = capture(["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"])
    if gpu_processes:
        raise RuntimeError("GPU already in use: " + gpu_processes)
    command = ["java", "-ea", "-Xmx16g",
               "-Dcacheexploration.cascaded.library=" +
               str(build_dir / "libcascaded_exploration.so"),
               "-cp", manifest["classpath"], main_class] + arguments
    if main_class.endswith(".PackedCodecBench"):
        command += ["--output", str(output / "results.json")]
    write_json(output / "launch.json", {
        "command": command, "rss_stop_bytes": RSS_LIMIT,
        "rss_guard_is_sampled_not_hard_limit": True,
        "sample_seconds": 0.25, "timeout_seconds": 900,
        "gpu": capture(["nvidia-smi", "--query-gpu=name,memory.total,driver_version",
                        "--format=csv"]),
        "build": str(build_dir), "git_head": capture(["git", "-C", str(REPO), "rev-parse", "HEAD"]),
    })
    peak = 0
    reason = None
    started = time.monotonic()
    with (output / "stdout.log").open("w") as stdout, (output / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            with (output / "rss.jsonl").open("w") as samples:
                while process.poll() is None:
                    rss = sample_rss(process.pid)
                    peak = max(peak, rss)
                    elapsed = time.monotonic() - started
                    samples.write(json.dumps({"elapsed_seconds": elapsed, "rss_bytes": rss}) + "\n")
                    samples.flush()
                    if rss > RSS_LIMIT or elapsed > 900:
                        reason = "rss_guard" if rss > RSS_LIMIT else "timeout"
                        break
                    time.sleep(0.25)
        except BaseException:
            reason = "launcher_exception"
            raise
        finally:
            stop_process_group(process)
            returncode = process.wait()
            write_json(output / "completion.json", {
                "returncode": returncode, "stop_reason": reason,
                "peak_sampled_tree_rss_bytes": peak,
                "elapsed_seconds": time.monotonic() - started,
                "includes_fixture_validation_and_reserved_pinned_pool": True,
            })
    print(json.dumps({"output": str(output), "returncode": returncode,
                      "stop_reason": reason, "peak_rss_bytes": peak}), flush=True)
    if returncode != 0 or reason:
        print((output / "stderr.log").read_text()[-8000:], file=sys.stderr)
        raise SystemExit(1)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, help="Create an isolated build and exit")
    parser.add_argument("--classes", type=Path, help="Existing isolated build directory")
    parser.add_argument("--output", type=Path, help="New directory for one run")
    parser.add_argument("--main", default="cacheexploration.nvcomp.PackedCodecBench")
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.build:
        build(args.build)
    else:
        if not args.classes or not args.output:
            parser.error("--classes and --output required for a run")
        arguments = args.arguments[1:] if args.arguments[:1] == ["--"] else args.arguments
        run(args.classes, args.output.resolve(), arguments, args.main)

if __name__ == "__main__":
    main()
