# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Compile a small JNI correctness gate; GPU execution requires explicit --execute."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from prepare_native import sha

HERE = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', required=True, type=Path)
    parser.add_argument('--prerequisites', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    build = json.loads((args.build / 'build-result.json').read_text())
    prerequisite_file = args.prerequisites / 'prerequisites.json'
    prerequisite = json.loads(prerequisite_file.read_text())
    assert build['success'] and sha(prerequisite_file) == build['prerequisites_sha256']
    for role in ['native_library', 'java_jar', 'runtime_library']:
        assert sha(Path(build[role])) == build[role + '_sha256'], role
    fat = Path(prerequisite['fat_jar'])
    assert sha(fat) == prerequisite['fat_jar_sha256']
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    classes = out / 'classes'
    classes.mkdir()
    source = HERE / 'ParquetMultiFileReaderGpuCheck.java'
    slf4j = Path('/home/roberte/src/spark_3.5.5/dist/jars/slf4j-api-2.0.7.jar')
    assert slf4j.is_file(), 'missing pinned Spark SLF4J API dependency'
    classpath = ':'.join([str(classes), build['java_jar'], str(fat), str(slf4j)])
    compile_command = ['javac', '--release', '11', '-cp', classpath,
                       '-d', str(classes), str(source)]
    command = ['java', '-ea', '-Xmx2g',
               '-Dai.rapids.cudf.preserve-dependencies=true',
               '-Djava.library.path=' + str(Path(build['native_library']).parent),
               '-cp', classpath, 'ai.rapids.cudf.ParquetMultiFileReaderGpuCheck',
               str(out / 'data'), build['runtime_library_sha256']]
    result = {
        'research_only': True, 'gpu_executed': False, 'success': False,
        'source': str(source), 'source_sha256': sha(source),
        'runner_sha256': sha(Path(__file__)),
        'build_result_sha256': sha(args.build / 'build-result.json'),
        'compile_command': compile_command, 'java_command': command,
        'runtime_classpath_dependencies': [{'path': str(slf4j), 'sha256': sha(slf4j)}],
        'pool_bytes': 15 << 30,
        'exact_native_dependency_lock': build['exact_native_dependency_lock'],
    }
    try:
        with (out / 'compile.log').open('x') as log:
            subprocess.run(compile_command, stdout=log, stderr=subprocess.STDOUT,
                           check=True, timeout=60)
        result['compiled'] = True
        if args.execute:
            env = dict(os.environ)
            env['LD_LIBRARY_PATH'] = str(Path(build['runtime_library']).parent) + \
                ':/usr/local/cuda/lib64:' + env.get('LD_LIBRARY_PATH', '')
            result['LD_LIBRARY_PATH'] = env['LD_LIBRARY_PATH']
            result['gpu_executed'] = True
            with (out / 'gpu.log').open('x') as log:
                completed = subprocess.run(command, stdout=log, stderr=subprocess.STDOUT,
                                           timeout=180, env=env)
            result['exit_code'] = completed.returncode
            assert completed.returncode == 0, 'GPU check failed: see gpu.log'
            markers = [line.split('NATIVE_GPU_CHECK ', 1)[1] for line in
                       (out / 'gpu.log').read_text().splitlines()
                       if line.startswith('NATIVE_GPU_CHECK ')]
            assert len(markers) == 1, 'expected exactly one gate result'
            gate = json.loads(markers[0])
            assert gate['success'] and gate['independent_files'] == 2 and gate['rows'] == 6
            assert gate['pool_bytes'] == 15 << 30
            assert gate['live_result_bytes'] > gate['baseline_bytes']
            assert not list((out / 'data').glob('*.parquet')), 'fixture wrote Parquet to disk'
            for name in ['encoded_in_memory', 'projection', 'nulls', 'timestamp', 'malformed_cleanup',
                         'pool_limit_rejected']:
                assert gate[name] is True, name
            assert gate['mapped_cudf_sha256'] == build['runtime_library_sha256'], \
                'mapped cuDF differs from the ABI-checked actual runtime'
            result['gpu_checks'] = gate
            result['success'] = True
        else:
            result['status'] = 'COMPILED_ONLY'
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
