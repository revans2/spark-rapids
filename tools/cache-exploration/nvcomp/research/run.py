#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Compile/run the small column-slice research proof; do not rebuild the Spark serializer."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import time


HERE = Path(__file__).resolve().parent


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    build = args.build_dir.resolve()
    manifest = json.loads((build / "spark-provenance.json").read_text())
    spec = importlib.util.spec_from_file_location("frozen_spark_runner", HERE.parent / "spark/run.py")
    checker = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(checker)
    checker.verify(manifest, build)
    busy = subprocess.check_output(
        ["nvidia-smi", "--query-compute-apps=pid", "--format=csv,noheader"], text=True).strip()
    if busy:
        raise RuntimeError("GPU already in use: " + busy)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    classes = output / "classes"
    classes.mkdir()
    host = json.loads((build / "host/host-build.json").read_text())
    includes = [arg for arg in host["command"] if arg.startswith("-I")]
    rmm_source = next(arg for arg in host["command"] if arg.endswith("/cuda_stream_view.cpp"))
    shared = str(Path(host["cudf_library"]).parent)
    cpp_source = HERE / "column_slice_jni.cpp"
    library = output / "libcolumn_slice.so"
    cpp = ["g++", "-std=c++20", "-O2", "-fPIC", "-shared", "-Wall", "-Wextra", "-Werror"]
    cpp += includes + [str(cpp_source), rmm_source, "-L" + shared,
                       "-L/usr/local/cuda/lib64", "-Wl,-rpath," + shared,
                       "-Wl,-rpath,/usr/local/cuda/lib64", "-lcudf", "-lcudart", "-ldl",
                       "-o", str(library)]
    classpath = os.pathsep.join([manifest["application"]["path"]] +
        [entry["path"] for entry in manifest["plugin_dependencies"] + manifest["spark_dependencies"]])
    javac = ["javac", "-cp", classpath, "-d", str(classes), str(HERE / "ColumnPruningProbe.java")]
    for command in [cpp, javac]:
        subprocess.run(command, check=True)
    header_command = ["g++", "-std=c++20", "-MM"] + includes + [str(cpp_source)]
    headers = subprocess.check_output(header_command, text=True)
    dependencies = headers.replace("\\\n", " ").split(":", 1)[1].split()
    files = {Path(path).resolve() for path in dependencies}
    files.update([Path(__file__), cpp_source, HERE / "ColumnPruningProbe.java", library])
    files.update(classes.rglob("*.class"))
    command = ["java", "-Xmx1g", "-Dcacheexploration.columnSlice.library=" + str(library),
               "-Dcacheexploration.hostPacked.library=" + manifest["host_library"],
               "-Dcacheexploration.cascaded.library=" + manifest["native_library"],
               "-Dai.rapids.cudf.nvtx.enabled=true", "-cp", str(classes) + os.pathsep + classpath,
               "cacheexploration.nvcomp.ColumnPruningProbe"]
    if args.profile:
        command = ["nsys", "profile", "--trace=cuda,nvtx", "--sample=none", "--cpuctxsw=none",
                   "--wait=all", "--output=" + str(output / "profile")] + command
    provenance = {"scope": "Small feasibility proof, no Spark serializer or throughput comparison",
        "command": command, "compile_commands": [cpp, javac],
        "header_command": header_command, "profiled": args.profile,
        "spark_build": str(build), "spark_manifest_sha256": digest(build / "spark-provenance.json"),
        "files": [{"path": str(path), "sha256": digest(path)} for path in sorted(files)],
        "compiler": subprocess.check_output(["g++", "--version"], text=True),
        "gpu": subprocess.check_output(["nvidia-smi", "--query-gpu=name,memory.total",
                                       "--format=csv,noheader"], text=True)}
    # Preserve sources used by this research run, independently of future working-tree changes.
    for path in [Path(__file__), cpp_source, HERE / "ColumnPruningProbe.java"]:
        (output / path.name).write_bytes(path.read_bytes())
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    started = time.monotonic()
    reason = None
    with (output / "stdout.log").open("w") as stdout, (output / "stderr.log").open("w") as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            process.wait(timeout=120)
        except subprocess.TimeoutExpired:
            reason = "timeout"
        except BaseException:
            reason = "launcher_exception"
            raise
        finally:
            checker.guard.stop_process_group(process)
            returncode = process.wait()
            completion = {"returncode": returncode, "stop_reason": reason,
                          "elapsed_seconds": time.monotonic() - started}
            (output / "completion.json").write_text(json.dumps(completion, indent=2) + "\n")
    (output / "completion.json").write_text(json.dumps(completion, indent=2) + "\n")
    if returncode or reason:
        raise RuntimeError((output / "stderr.log").read_text()[-8000:])
    lines = (output / "stdout.log").read_text().splitlines()
    records = [json.loads(line) for line in lines if line.startswith("{")]
    if len(records) != 39 or sum(line.startswith("PASS column-slice feasibility") for line in lines) != 1:
        raise RuntimeError("Incomplete probe observations")
    (output / "results.json").write_text(json.dumps(records, indent=2) + "\n")
    print(json.dumps({"output": str(output), "observations": len(records), **completion}))


if __name__ == "__main__":
    main()
