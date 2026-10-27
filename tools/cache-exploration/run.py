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

"""Compile/run the isolated Arrow/Vortex codec experiment without changing project builds."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--vortex-classpath", type=Path, required=True,
                    help="File containing the unshaded Vortex runtime classpath")
parser.add_argument("--cudf-jar", type=Path, required=True)
parser.add_argument("--native-lib", type=Path,
                    help="Exact optimized libvortex_jni.so to expose as a classpath resource")
parser.add_argument("--build-dir", type=Path, default=Path("/data/tmp/cache-bridge-harness"))
parser.add_argument("--compile-only", action="store_true")
parser.add_argument("--fixture", choices=["fixed", "strings-low", "strings-wide"], default="fixed")
parser.add_argument("--rows", type=int, default=65536)
parser.add_argument("--warmups", type=int, default=2)
parser.add_argument("--repeats", type=int, default=5)
parser.add_argument("--cap-bytes", type=int, default=128 * 1024 * 1024)
parser.add_argument("--heap", default="1g")
args = parser.parse_args()

source = Path(__file__).resolve().with_name("CacheBridgeBench.java")
args.build_dir.mkdir(parents=True, exist_ok=True)
classpath = str(args.cudf_jar.resolve()) + os.pathsep + args.vortex_classpath.read_text().strip()
subprocess.run(["javac", "-encoding", "UTF-8", "--release", "17", "-cp", classpath,
                "-d", str(args.build_dir), str(source)], check=True)
if args.compile_only:
    sys.exit(0)
if args.native_lib is None:
    parser.error("--native-lib is required unless --compile-only")
native = args.native_lib.resolve(strict=True)
resource = args.build_dir / "native" / "linux-amd64" / "libvortex_jni.so"
resource.parent.mkdir(parents=True, exist_ok=True)
shutil.copyfile(native, resource)
hasher = hashlib.sha256()
with resource.open("rb") as library:
    for chunk in iter(lambda: library.read(1024 * 1024), b""):
        hasher.update(chunk)
digest = hasher.hexdigest()
print("Native resource: " + str(native) + " SHA256=" + digest, file=sys.stderr, flush=True)
print("Scope: isolated codec proxy; not Spark PCBS or heap-pressure validation",
      file=sys.stderr, flush=True)
subprocess.run([
    "java", "-ea", "-Xmx" + args.heap,
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "-cp", str(args.build_dir) + os.pathsep + classpath, "CacheBridgeBench",
    args.fixture, str(args.rows), str(args.warmups), str(args.repeats), str(args.cap_bytes)
], check=True)
