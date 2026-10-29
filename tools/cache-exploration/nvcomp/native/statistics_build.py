#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Build isolated CUDA packed statistics; no cuDF headers, libraries or GPU execution."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cuda", type=Path, default=Path("/usr/local/cuda"))
    parser.add_argument("--host-compiler", type=Path,
                        default=Path("/opt/rh/gcc-toolset-13/root/usr/bin/g++"))
    parser.add_argument("--architecture", type=int, default=86)
    args = parser.parse_args()
    if args.architecture <= 0:
        raise ValueError("CUDA architecture must be positive")
    cuda = args.cuda.resolve(strict=True)
    compiler = args.host_compiler.resolve(strict=True)
    nvcc = (cuda / "bin/nvcc").resolve(strict=True)
    javac = Path(shutil.which("javac")).resolve(strict=True)
    java_home = javac.parent.parent
    source = Path(__file__).resolve().parent
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    library = output / "libpacked_statistics.so"
    dependencies = output / "statistics.d"
    command = [
        str(nvcc), "-std=c++17", "-O2", "--shared", "--cudart", "shared",
        "--compiler-bindir", str(compiler),
        "-Xcompiler=-fPIC,-Wall,-Wextra,-Werror",
        "-gencode=arch=compute_{0},code=[sm_{0},compute_{0}]".format(args.architecture),
        "-I" + str(java_home / "include"), "-I" + str(java_home / "include/linux"),
        "-MD", "-MF", str(dependencies), str(source / "packed_statistics.cu"),
        "-Xlinker=-rpath," + str(cuda / "lib64"), "-o", str(library)]
    subprocess.run(command, check=True)
    dynamic = subprocess.check_output(["readelf", "-d", str(library)], text=True)
    if "libcudart.so" not in dynamic or "libcudf" in dynamic:
        raise RuntimeError("Statistics extension must link shared CUDA runtime without cuDF")
    header_text = dependencies.read_text().replace("\\\n", " ")
    headers = shlex.split(header_text.split(":", 1)[1])
    files = {Path(path).resolve(strict=True) for path in headers}
    files.update([source / "statistics_build.py", source / "packed_statistics.cu",
                  library, compiler, nvcc, (cuda / "lib64/libcudart.so").resolve(strict=True)])
    manifest = {
        "scope": "Experimental raw-pointer CUDA+CUB statistics; no cuDF ABI",
        "command": command, "architecture": args.architecture,
        "cuda": str(cuda), "java_home": str(java_home),
        "compiler_versions": {
            "nvcc": subprocess.check_output([str(nvcc), "--version"], text=True),
            "host": subprocess.check_output([str(compiler), "--version"], text=True)},
        "dynamic_dependencies": dynamic,
        "files": [{"path": str(path), "sha256": digest(path)} for path in sorted(files)],
        "gpu_work_executed": False}
    provenance = output / "statistics-build.json"
    provenance.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"library": str(library), "manifest": str(provenance)}))


if __name__ == "__main__":
    main()
