#!/usr/bin/env python3.9
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Build the isolated CPU metadata bridge against the pinned runtime library."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import zipfile


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024**2), b""):
            result.update(block)
    return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cudf-jar", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    jar = args.cudf_jar.resolve(strict=True)
    source = Path(__file__).resolve().parent
    # Share only the immutable, hash-checked large runtime across experiment builds.
    shared = Path("/data/tmp/nvcomp-host-libraries") / digest(jar)
    shared.mkdir(parents=True, exist_ok=True)
    library = shared / "libcudf.so"
    with zipfile.ZipFile(jar) as archive:
        names = [name for name in archive.namelist() if Path(name).name == "libcudf.so"]
        if len(names) != 1:
            raise RuntimeError("Expected one pinned libcudf.so in the JNI artifact")
        checksum = hashlib.sha256()
        temporary = shared / "libcudf.so.tmp"
        if library.exists():
            with archive.open(names[0]) as stream:
                for block in iter(lambda: stream.read(8 * 1024**2), b""):
                    checksum.update(block)
            if digest(library) != checksum.hexdigest():
                raise RuntimeError("Shared cuDF runtime differs from the pinned artifact")
        else:
            with archive.open(names[0]) as stream, temporary.open("xb") as target:
                for block in iter(lambda: stream.read(8 * 1024**2), b""):
                    checksum.update(block)
                    target.write(block)
            temporary.rename(library)
    jni = Path("/home/roberte/src/spark-rapids-jni")
    rmm = jni / "target/libcudf/cmake-build/_deps/rmm-src/cpp"
    includes = [
        Path(shutil.which("javac")).resolve().parent.parent / "include",
        Path(shutil.which("javac")).resolve().parent.parent / "include/linux",
        jni / "thirdparty/cudf/cpp/include", rmm / "include",
        jni / "target/libcudf-install/include",
        jni / "target/libcudf-install/include/rapids", Path("/usr/local/cuda/include")]
    native = output / "libhost_packed.so"
    dependencies = output / "headers.d"
    command = ["g++", "-std=c++20", "-O2", "-fPIC", "-shared", "-Wall", "-Wextra",
               "-Werror", "-MMD", "-MF", str(dependencies)]
    command += ["-I" + str(path) for path in includes]
    command += [str(source / "host_packed_jni.cpp"),
                str(rmm / "src/cuda_stream_view.cpp"), "-L" + str(shared),
                "-L/usr/local/cuda/lib64", "-Wl,-rpath," + str(shared),
                "-Wl,-rpath,/usr/local/cuda/lib64", "-lcudf", "-ldl", "-lcudart",
                "-o", str(native)]
    subprocess.run(command, check=True)
    # A single depfile from two sources records the last translation unit only;
    # ask the compiler separately for the bridge's complete header inventory.
    header_command = ["g++", "-std=c++20", "-MM"] + [
        "-I" + str(path) for path in includes] + [str(source / "host_packed_jni.cpp")]
    header_text = subprocess.check_output(header_command, text=True)
    headers = header_text.replace("\\\n", " ").split(":", 1)[1].split()
    rmm_headers = dependencies.read_text().replace('\\\n', " ").split(":", 1)[1].split()
    files = {Path(path).resolve() for path in headers + rmm_headers}
    files.update([jar, library, native, source / "host_build.py",
                  rmm / "src/cuda_stream_view.cpp"])
    manifest = {
        "scope": "Experimental host metadata only; no CPU row conversion or GPU execution",
        "command": command, "header_command": header_command,
        "cudf_library": str(library), "cudf_jar": str(jar),
        "files": [{"path": str(path), "sha256": digest(path)} for path in sorted(files)]}
    (output / "host-build.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"library": str(native), "manifest": str(output / "host-build.json")}))


if __name__ == "__main__":
    main()
