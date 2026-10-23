# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Compile a generated-consumer ownership gate. GPU execution needs --execute."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(2 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    manifest_path = args.artifacts / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    assert manifest['research_only'] and manifest['spark_version'] == '3.5.5'
    roles = {entry['role']: Path(entry['path']) for entry in manifest['files']}
    for path, expected in manifest['sources'].items():
        assert sha(path) == expected, 'artifact source changed: ' + path
    for entry in manifest['files']:
        assert sha(entry['path']) == entry['sha256'], entry['role']
    jars = sorted(Path('/home/roberte/src/spark_3.5.5/dist/jars').glob('*.jar'))
    assert jars
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    classes = out / 'classes'
    classes.mkdir()
    # Deliberately omit old plugin-compile-classes: the consumer must come from this plugin.
    classpath = ':'.join(map(str, [classes, roles['codec_overlay'],
        roles['codec_fixture'], roles['native_reader_jar'], roles['plugin']] + jars))
    source = HERE / 'ParquetConsumerTaskCleanupCheck.java'
    compile_command = ['javac', '--release', '11', '-cp', classpath, '-d',
                       str(classes), str(source)]
    command = ['java', '-ea', '-Xmx2g',
               '-Djava.library.path=' + str(roles['native_library'].parent),
               '-cp', classpath, 'org.apache.spark.ParquetConsumerTaskCleanupCheck',
               str(roles['plugin'])]
    result = {'research_only': True, 'success': False, 'gpu_executed': False,
              'source_sha256': sha(source), 'runner_sha256': sha(Path(__file__)),
              'artifact_manifest_sha256': sha(manifest_path),
              'compile_command': compile_command, 'java_command': command,
              'pool_bytes': 15 << 30}
    try:
        with (out / 'compile.log').open('x') as log:
            subprocess.run(compile_command, stdout=log, stderr=subprocess.STDOUT,
                           timeout=60, check=True)
        result['compiled'] = True
        if args.execute:
            env = dict(os.environ)
            native_runtime = Path(manifest['runtime_baseline']['runtime_library'])
            env['LD_LIBRARY_PATH'] = str(native_runtime.parent) + \
                ':/usr/local/cuda/lib64:' + env.get('LD_LIBRARY_PATH', '')
            result['LD_LIBRARY_PATH'] = env['LD_LIBRARY_PATH']
            result['gpu_executed'] = True
            with (out / 'gpu.log').open('x') as log:
                completed = subprocess.run(command, env=env, stdout=log,
                    stderr=subprocess.STDOUT, timeout=180)
            result['exit_code'] = completed.returncode
            assert completed.returncode == 0, 'see gpu.log'
            markers = [line.split('OWNERSHIP_GPU_CHECK ', 1)[1] for line in
                (out / 'gpu.log').read_text().splitlines()
                if line.startswith('OWNERSHIP_GPU_CHECK ')]
            assert len(markers) == 1
            gate = json.loads(markers[0])
            for field in ['success', 'more_records_remained', 'task_completed',
                          'callback_refcounts_zero', 'second_close_idempotent',
                          'encoded_in_memory']:
                assert gate[field] is True, field
            assert gate['consumed_records'] == 1 and gate['callback_buffers'] > 0
            assert gate['rmm_baseline_bytes'] == 0
            assert str(roles['plugin']) in gate['consumer_origin']
            result['checks'] = gate
            result['success'] = True
        else:
            result['status'] = 'COMPILED_ONLY'
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'out': str(out), 'compiled': result.get('compiled'),
                      'gpu_executed': result['gpu_executed'], 'success': result['success']}))


if __name__ == '__main__':
    main()
