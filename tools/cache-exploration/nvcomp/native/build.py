#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
"""Compile the standalone bridge without launching CUDA work or modifying Maven targets."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--output', type=Path, required=True)
parser.add_argument('--sdk', type=Path,
                    default=Path('/home/roberte/miniconda3/envs/spark-accel'))
parser.add_argument('--cuda', type=Path, default=Path('/usr/local/cuda'))
parser.add_argument('--cudf-jar', type=Path, default=Path(
    '/home/roberte/.m2/repository/com/nvidia/cudf-spark-jni/26.12.0-SNAPSHOT/'
    'cudf-spark-jni-26.12.0-SNAPSHOT-cuda12.jar'))
args = parser.parse_args()
args.output.mkdir(parents=True, exist_ok=False)
classes = args.output / 'classes'
classes.mkdir()
java_home = Path(shutil.which('javac')).resolve().parent.parent
source = Path(__file__).resolve().parent
sdk_library = (args.sdk / 'lib/libnvcomp.so').resolve(strict=True)
library = args.output / 'libcascaded_exploration.so'
commands = [
    ['g++', '-std=c++17', '-O2', '-fPIC', '-shared', '-Wall', '-Wextra', '-Werror',
     '-I' + str(java_home / 'include'), '-I' + str(java_home / 'include/linux'),
     '-I' + str(args.cuda / 'include'), '-I' + str(args.sdk / 'include'),
     '-DCASCADED_SDK_LIBRARY="' + str(sdk_library) + '"',
     str(source / 'cascaded_jni.cpp'), str(source / 'byte_codec_jni.cpp'), '-L' + str(args.cuda / 'lib64'),
     '-Wl,-rpath,' + str(args.cuda / 'lib64'), '-lcudart', '-ldl', '-o', str(library)],
    ['javac', '-Xlint:all', '-cp', str(args.cudf_jar), '-d', str(classes),
     str(source.parent / 'CascadedCodec.java'), str(source.parent / 'ByteCodec.java'),
     str(source / 'CascadedSmoke.java'), str(source / 'ByteCodecSmoke.java')]
]
for command in commands:
    subprocess.run(command, check=True)
files = [sdk_library, args.cudf_jar, source / 'cascaded_jni.cpp', source / 'byte_codec_jni.cpp',
         source.parent / 'ByteCodec.java', source / 'CascadedSmoke.java', source / 'ByteCodecSmoke.java',
         source.parent / 'CascadedCodec.java', source / 'build.py', library]
files += sorted((args.sdk / 'include/nvcomp').glob('*.h'))
provenance = {'commands': commands, 'sdk_library': str(sdk_library),
              'java_home': str(java_home), 'cuda': str(args.cuda.resolve()),
              'files': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()}
                        for p in files],
              'gpu_work_executed': False}
(args.output / 'build-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
print(json.dumps({'library': str(library), 'classes': str(classes),
                  'provenance': str(args.output / 'build-provenance.json')}))
