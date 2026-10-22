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
"""Run one bounded fresh-JVM heap-backed codec-versus-Java cache case."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

from verify_benefit import verify

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--base-run", type=Path, required=True)
parser.add_argument("--spark-home", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
parser.add_argument("--mode", choices=("codec", "java"), required=True)
parser.add_argument("--record-mib", choices=(256, 1024), type=int, required=True)
parser.add_argument("--measured-reads", choices=(2, 3), type=int, required=True)
parser.add_argument("--timeout-s", type=int, default=1200)
parser.add_argument("--remove-block-files", action="store_true",
                    help="After verification, record then remove only this run's raw local block directory")
args = parser.parse_args()
base = args.base_run.resolve()
out = args.output_dir.resolve()
home = args.spark_home.resolve()
out.mkdir(parents=True, exist_ok=False)
sha = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
base_manifest = json.loads((base / "manifest.json").read_text())
assert json.loads((base / "result.json").read_text())["valid"]
overlay = base / "blockmanager-codec-spike.jar"
assert sha(overlay) == base_manifest["artifact_sha256"]
source_root = Path(__file__).resolve().parent
snapshot = out / "sources"
snapshot.mkdir()
for path in source_root.glob("*"):
    if path.is_file():
        shutil.copyfile(path, snapshot / path.name)
classes = out / "classes"
classes.mkdir()
jars = list(base_manifest["runtime_jars"])
cp = ":".join([str(overlay)] + jars)
compile_argv = ["java", "-cp", cp, "scala.tools.nsc.Main", "-classpath", cp,
                "-d", str(classes), str(snapshot / "CodecBenefitSpike.scala")]
with (out / "compile.log").open("w") as log:
    compiled = subprocess.run(compile_argv, stdout=log, stderr=subprocess.STDOUT, timeout=180)
if compiled.returncode:
    raise SystemExit("Compile failed: inspect " + str(out / "compile.log"))
artifact = out / "benefit-spike.jar"
subprocess.run(["jar", "cf", str(artifact), "-C", str(classes), "."], check=True)
(out / "events").mkdir()
serializer = ("CodecBlobSerializer" if args.mode == "codec" else "JavaBlobSerializer")
conf = {
    "spark.plugins": "", "spark.sql.extensions": "",
    "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
    "spark.sql.cache.serializer": "org.apache.spark.sql.codecbenefit." + serializer,
    "spark.driver.extraClassPath": str(artifact) + ":" + str(overlay),
    "spark.executor.extraClassPath": str(artifact) + ":" + str(overlay),
    "spark.driver.extraJavaOptions":
        "-XX:+UseParallelGC -Xlog:gc*:file=" + str(out / "gc.log") + ":time,uptime,level,tags",
    "spark.ui.enabled": "false", "spark.sql.adaptive.enabled": "false",
    "spark.eventLog.enabled": "true", "spark.eventLog.dir": (out / "events").as_uri(),
    "spark.eventLog.compress": "false", "spark.eventLog.logBlockUpdates.enabled": "true",
    "spark.local.dir": str(out / "local"),
    "spark.codecBenefit.recordBytes": str(args.record_mib * 1024 * 1024),
}
argv = [str(home / "bin/spark-submit"), "--class", "org.apache.spark.sql.codecbenefit.CodecBenefitSpike",
        "--master", "local[8]", "--driver-memory", "48g"]
for key, value in conf.items():
    argv += ["--conf", key + "=" + value]
argv += [str(artifact), args.mode, str(args.measured_reads)]
inputs = dict(base_manifest["runtime_jars"])
inputs.update({str(path): sha(path) for path in snapshot.iterdir() if path.is_file()})
inputs.update({str(overlay): sha(overlay), str(base / "manifest.json"): sha(base / "manifest.json")})
proof = {"mode": args.mode, "record_mib": args.record_mib, "measured_reads": args.measured_reads,
         "record_bytes": args.record_mib * 1024 * 1024,
         "partitions": 16, "records_per_partition": 6, "task_threads": 8,
         "heap_gib": 48, "storage_level": "DISK_ONLY", "inputs": inputs,
         "artifact_sha256": sha(artifact), "compile_argv": compile_argv, "run_argv": argv,
         "disk_free_before_bytes": shutil.disk_usage(out).free}
(out / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n")
start = time.monotonic()
with (out / "run.log").open("w") as log:
    timed_out = False
    try:
        completed = subprocess.run(argv, stdout=log, stderr=subprocess.STDOUT,
                                   env=dict(os.environ, SPARK_HOME=str(home)), timeout=args.timeout_s)
        code = completed.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        code = 124
process = {"exit_code": code, "timed_out": timed_out, "wall_s": time.monotonic() - start,
           "disk_free_after_bytes": shutil.disk_usage(out).free}
(out / "process-result.json").write_text(json.dumps(process, indent=2) + "\n")
raw_outputs = [out / "run.log", out / "gc.log"] + [
    path for path in (out / "events").rglob("*") if path.is_file() and not path.name.startswith(".")]
proof["raw_output_hashes"] = {str(path): sha(path) for path in raw_outputs if path.is_file()}
(out / "manifest.json").write_text(json.dumps(proof, indent=2) + "\n")
result = verify(out)
(out / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
if args.remove_block_files and result["valid"]:
    local = out / "local"
    files = [{"path": str(path.relative_to(out)), "bytes": path.stat().st_size}
             for path in local.rglob("*") if path.is_file()] if local.exists() else []
    cleanup = {"removed_directory": str(local), "raw_files": files,
               "raw_bytes": sum(item["bytes"] for item in files),
               "basis": "Completed exact data and metadata verification, or classified heap OOM",
               "verification_sha256": sha(out / "verification.json"),
               "full_raw_file_hashes": False}
    (out / "raw-block-cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n")
    if local.exists():
        shutil.rmtree(local)
    cleanup["directory_absent_after"] = not local.exists()
    cleanup["disk_free_after_cleanup_bytes"] = shutil.disk_usage(out).free
    (out / "raw-block-cleanup.json").write_text(json.dumps(cleanup, indent=2) + "\n")
print(json.dumps({"valid": result["valid"], "outcome": result["outcome"],
                  "output": str(out), "process_wall_s": process["wall_s"],
                  "phase_metrics": result["phase_metrics"]}))
raise SystemExit(0 if result["valid"] else 1)
