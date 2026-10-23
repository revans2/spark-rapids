# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Compile the standalone research harness against Spark jars; never run GPU work."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parent / "WorldCacheBench.scala"
    jars = sorted((args.spark / "assembly/target/scala-2.12/jars").glob("*.jar"))
    if not jars:
        raise ValueError("Spark assembly jars missing")
    args.out.mkdir(parents=True, exist_ok=False)
    classes = args.out / "classes"
    classes.mkdir()
    cp = ":".join(map(str, jars))
    command = ["java", "-Xmx2g", "-cp", cp, "scala.tools.nsc.Main",
               "-classpath", cp, "-d", str(classes), str(source)]
    (args.out / "compile-command.json").write_text(json.dumps(command, indent=2)+"\n")
    with (args.out / "compile.log").open("x") as log:
        subprocess.check_call(command, stdout=log, stderr=subprocess.STDOUT)
    artifact = args.out / "world-cache-bench.jar"
    subprocess.check_call(["jar", "cf", str(artifact), "-C", str(classes), "."])
    digest = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
    result = dict(source=str(source), source_sha256=digest(source), artifact=str(artifact),
                  artifact_sha256=digest(artifact), spark=str(args.spark),
                  compile_only=True)
    (args.out / "build-result.json").write_text(json.dumps(result, indent=2)+"\n")
    print(json.dumps(result))

if __name__ == "__main__":
    main()
