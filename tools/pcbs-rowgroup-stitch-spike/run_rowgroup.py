# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""One isolated row-group prototype JVM. No automatic matrix or production changes."""
import argparse
import csv
import fcntl
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
import zipfile

HERE = Path(__file__).resolve().parent
REAL = HERE.parent / 'pcbs-real-data-comparison'
sys.path.insert(0, str(REAL))
from canonical_source import check, digest
from run_controlled import process_tree, signal_owned
from verify_rowgroup import verify

STOCK = Path('/home/roberte/src/spark_3.5.5')


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def artifact_inputs(path, mode):
    spec = json.loads(path.read_text())
    assert spec['research_only'] and spec['spark_version'] == '3.5.5'
    assert spec['global_batch_bytes'] == spec['decode_target_bytes'] == 2**30
    assert spec['rowgroup_target_rows'] == 65536 and spec['rowgroup_target_bytes'] == 8*2**20
    assert spec['record_safety_limit_bytes'] == 256*2**20
    assert spec['compression'] in ('NONE', 'SNAPPY')
    roles = {}
    frozen = {str(path.resolve()): digest(path)}
    for entry in spec['files']:
        role = entry['role']
        assert role not in roles, 'duplicate artifact role: ' + role
        artifact = Path(entry['path']).resolve()
        assert digest(artifact) == entry['sha256'], 'artifact changed: ' + str(artifact)
        roles[role] = str(artifact)
        frozen[str(artifact)] = entry['sha256']
    required = {'plugin', 'codec_fixture', 'native_reader_jar', 'native_library'}
    assert required <= set(roles), 'missing prototype artifact roles'
    assert mode == 'java' or 'codec_overlay' in roles
    for source, sha in spec['sources'].items():
        assert digest(source) == sha, 'prototype source changed: ' + source
        frozen[str(Path(source).resolve())] = sha
    baseline = spec['runtime_baseline']
    runtime_library = Path(baseline['runtime_library']).resolve()
    assert digest(runtime_library) == baseline['runtime_library_sha256'], 'native runtime changed'
    frozen[str(runtime_library)] = baseline['runtime_library_sha256']
    with zipfile.ZipFile(roles['native_reader_jar']) as jar:
        assert 'ai/rapids/cudf/Table.class' not in jar.namelist(), 'native helper overrides Table'
    return spec, roles, frozen


def launch_command(out, bench, canonical, spec, roles, mode, storage):
    codec = mode == 'codec'
    cache_serializer = ('org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer'
                        if codec else 'com.nvidia.spark.ParquetCachedBatchSerializer')
    base_roles = ('plugin', 'codec_fixture', 'native_reader_jar')
    jars = [roles[r] for r in base_roles] + [roles[r] for r in sorted(roles)
            if r.endswith('_jar') and r not in base_roles]
    cp = ':'.join(([roles['codec_overlay']] if codec else []) + jars)
    options = ('-Xms48g -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 '
               '-XX:-UseContainerSupport -XX:-HeapDumpOnOutOfMemoryError '
               '-Djava.library.path=' + str(Path(roles['native_library']).parent) +
               ' -Xlog:gc*:file=' + str(out / 'gc.log') + ':time,level,tags')
    conf = {
        'spark.sql.adaptive.enabled': 'false',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': (out / 'events').as_uri(),
        'spark.local.dir': str(out / 'local'),
        'spark.driver.extraJavaOptions': options,
        'spark.executor.extraClassPath': cp,
        'spark.sql.optimizer.canChangeCachedPlanOutputPartitioning': 'false',
        'spark.plugins': 'com.nvidia.spark.SQLPlugin',
        'spark.sql.cache.serializer': cache_serializer,
        'spark.sql.inMemoryColumnarStorage.compressed': str(spec['compression'] == 'SNAPPY').lower(),
        'spark.serializer.objectStreamReset': '100',
        'spark.storage.unrollMemoryCheckPeriod': '16',
        'spark.rapids.sql.enabled': 'true',
        'spark.rapids.sql.metrics.level': 'DEBUG',
        'spark.rapids.sql.exec.InMemoryTableScanExec': 'true',
        'spark.rapids.sql.batchSizeBytes': '1073741824',
        'spark.rapids.sql.concurrentGpuTasks': '2',
        'spark.rapids.sql.maxConcurrentGpuTasks': '2',
        'spark.rapids.memory.gpu.allocSize': '15g',
        'spark.rapids.memory.gpu.maxAllocSize': '15g',
        'spark.rapids.memory.host.offHeapLimit.enabled': 'true',
        'spark.rapids.memory.host.offHeapLimit.size': '40g',
        'spark.rapids.memory.pinnedPool.size': '2g'}
    cmd = [str(STOCK / 'bin/spark-submit'), '--properties-file', str(out / 'empty.properties'),
           '--class', 'RealCacheBench', '--master', 'local[8]', '--driver-memory', '48g',
           '--driver-class-path', cp, '--jars', ','.join(jars)]
    for key, value in conf.items():
        cmd += ['--conf', key + '=' + value]
    return cmd, conf, cache_serializer


def run_owned(cmd, env, out, timeout):
    stop = threading.Event()
    observed_pids = set()
    errors = [0]
    started = time.monotonic()
    with (out / 'run.log').open('x') as log:
        child = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True)
        (out / 'child.pid').write_text(str(child.pid))
        def sample():
            with (out / 'process-memory.csv').open('x', newline='') as stream:
                writer = csv.writer(stream, lineterminator='\n')
                writer.writerow(['epoch_s', 'pid', 'VmRSS_KiB', 'VmHWM_KiB', 'error'])
                while not stop.is_set():
                    try:
                        command = Path('/proc/{}/cmdline'.format(child.pid)).read_bytes()
                        assert b'org.apache.spark.deploy.SparkSubmit' in command
                        assert command.split(b'\0', 1)[0].rsplit(b'/', 1)[-1] == b'java'
                        fields = {}
                        for line in Path('/proc/{}/status'.format(child.pid)).read_text().splitlines():
                            if line.startswith(('VmRSS:', 'VmHWM:')):
                                key, value, unit = line.split()
                                assert unit == 'kB'
                                fields[key.rstrip(':')] = int(value)
                        writer.writerow([time.time(), child.pid, fields['VmRSS'], fields['VmHWM'], ''])
                        observed_pids.add(child.pid)
                    except Exception as error:
                        errors[0] += 1
                        writer.writerow([time.time(), '', '', '', repr(error)])
                    stream.flush()
                    stop.wait(1)
        sampler = threading.Thread(target=sample)
        sampler.daemon = True
        sampler.start()
        timed_out = False
        try:
            code = child.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            owned = process_tree(child.pid)
            signal_owned(owned, signal.SIGTERM)
            try:
                code = child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                signal_owned(owned, signal.SIGKILL)
                code = child.wait(timeout=10)
        finally:
            stop.set()
            sampler.join(timeout=3)
    write(out / 'process.json', dict(exit_code=code, timeout=timed_out,
                                    wall_seconds=time.monotonic()-started))
    write(out / 'process-sampler-result.json', dict(selected_pids=sorted(observed_pids),
          error_samples=errors[0], note='Sampled JVM RSS and process-lifetime HWM; not native attribution.'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--bench', type=Path, required=True)
    parser.add_argument('--canonical', type=Path, required=True)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--artifacts', type=Path, required=True)
    parser.add_argument('--mode', choices=['java', 'codec'], required=True)
    parser.add_argument('--storage', choices=['MEMORY_AND_DISK', 'DISK_ONLY'],
                        default='MEMORY_AND_DISK')
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--timeout', type=int, default=7200)
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    assert args.timeout > 0
    out = args.out.resolve()
    assert not out.exists(), 'fresh output directory required'
    spec, roles, frozen = artifact_inputs(args.artifacts.resolve(), args.mode)
    source = check(args.manifest)
    rows = source['config']['rows_per_partition']
    assert (0 < rows <= 1000000 if args.smoke else rows == 50331648)
    assert {str(Path(f['path']).parent) for f in source['files']} == {str(args.canonical.resolve())}
    built = json.loads((args.bench.parent / 'build-result.json').read_text())
    assert built['success'] and digest(args.bench) == built['jar_sha256']
    assert digest(REAL / 'RealCacheBench.scala') == built['source_sha256']
    frozen.update({str(p.resolve()): digest(p) for p in [
        args.bench, args.manifest, args.bench.parent / 'build-result.json',
        REAL / 'RealCacheBench.scala', HERE / 'run_rowgroup.py', HERE / 'verify_rowgroup.py']})
    cmd, conf, serializer = launch_command(out, args.bench, args.canonical, spec, roles,
                                          args.mode, args.storage)
    cmd += [str(args.bench.resolve()), 'gpu-controlled', str(args.canonical.resolve()), str(rows)]
    library_path = ':'.join([str(Path(spec['runtime_baseline']['runtime_library']).parent),
                             '/usr/local/cuda/lib64'] +
                            ([os.environ['LD_LIBRARY_PATH']] if os.environ.get('LD_LIBRARY_PATH') else []))
    meta = dict(action='gpu-controlled', mode=args.mode, smoke=args.smoke,
                rows_per_partition=rows, storage=args.storage, argv=cmd,
                expected_conf=conf, cache_serializer=serializer,
                codec_overlay=roles.get('codec_overlay'), artifact_spec=spec,
                manifest=str(args.manifest.resolve()), frozen_hashes=frozen,
                expected_ld_library_path=library_path)
    if args.plan_only:
        print(json.dumps(meta, indent=2))
        return
    assert shutil.disk_usage(out.parent).free > rows*16*128 + 16*2**30
    with (out.parent / 'rowgroup-gpu.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        users = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid',
                                        '--format=csv,noheader,nounits'], universal_newlines=True)
        assert not users.strip(), 'GPU in use; park rather than overlap'
        out.mkdir(parents=True)
        (out / 'events').mkdir()
        (out / 'empty.properties').write_text('# No inherited spark defaults\n')
        write(out / 'command.json', meta)
        env = {k: v for k, v in os.environ.items() if not k.startswith(
            ('SPARK_', 'PYSPARK_', 'REAL_CACHE_')) and k not in (
                'CLASSPATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS')}
        env.update(REAL_CACHE_STORAGE_LEVEL=args.storage, REAL_CACHE_CONSUMER_TARGETS='1024',
                   LD_LIBRARY_PATH=library_path)
        run_owned(cmd, env, out, args.timeout)
        try:
            result = verify(out)
        except Exception as error:
            result = dict(outcome='VERIFIER_ERROR', issues=[repr(error)])
        write(out / 'result.json', result)
        if result['outcome'] not in ('PASS', 'SMOKE_PASS', 'HEAP_OOM'):
            raise SystemExit(1)


if __name__ == '__main__':
    main()
