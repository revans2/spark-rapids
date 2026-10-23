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
"""One fresh-JVM real-data cache experiment; explicit inputs, no automatic tuning."""
import argparse
import csv
import fcntl
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import sys
import threading
import time
import zipfile

HERE = Path(__file__).resolve().parent
REAL = HERE.parent / 'pcbs-real-data-comparison'
sys.path.insert(0, str(REAL))
from canonical_source import digest
from run_controlled import process_tree, signal_owned

STOCK = Path('/home/roberte/src/spark_3.5.5')
MODES = ('cpu-default', 'pcbs-java', 'rg-java', 'rg-codec')
CORE = str(STOCK / 'assembly/target/scala-2.12/jars/spark-core_2.12-3.5.5.jar')
SERIALIZERS = {
    'cpu-default': 'org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer',
    'pcbs-java': 'com.nvidia.spark.ParquetCachedBatchSerializer',
    'rg-java': 'com.nvidia.spark.ParquetCachedBatchSerializer',
    'rg-codec': 'org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer'}


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def checked_file(path, sha, frozen):
    path = Path(path).resolve()
    assert path.is_file() and digest(path) == sha, 'changed/missing file: ' + str(path)
    frozen[str(path)] = sha
    return str(path)


def input_files(path, frozen):
    """Freeze actual input bytes outside timed phases; never regenerate data."""
    path = Path(path).resolve()
    source = json.loads(path.read_text())
    assert source['partitions'] == 16 and source['total_rows'] > 0
    files = source['files']
    assert len(files) == 16 and [x['partition'] for x in files] == list(range(16))
    assert sum(x['rows'] for x in files) == source['total_rows']
    assert all(x['rows'] > 0 and x['row_groups'] > 0 for x in files)
    for entry in files:
        name = checked_file(entry['path'], entry['sha256'], frozen)
        assert Path(name).stat().st_size == entry['bytes'], 'input length differs'
    frozen[str(path)] = digest(path)
    return source


def artifacts(path, mode, frozen):
    path = Path(path).resolve()
    spec = json.loads(path.read_text())
    assert spec['research_only'] is True and spec['spark_version'] == '3.5.5'
    selected = spec['modes'][mode]
    roles = {}
    for item in selected['files']:
        assert item['role'] not in roles, 'duplicate artifact role'
        roles[item['role']] = checked_file(item['path'], item['sha256'], frozen)
    for name, sha in selected.get('sources', {}).items():
        checked_file(name, sha, frozen)
    assert 'plugin' not in roles if mode == 'cpu-default' else 'plugin' in roles
    assert ('codec_overlay' in roles) == (mode == 'rg-codec'), 'wrong SerializerManager overlay'
    if mode.startswith('rg-'):
        assert {'codec_fixture', 'native_reader_jar', 'native_library'} <= set(roles)
        with zipfile.ZipFile(roles['native_reader_jar']) as jar:
            assert 'ai/rapids/cudf/Table.class' not in jar.namelist()
    if mode == 'pcbs-java':
        assert 'codec_fixture' in roles and 'native_reader_jar' not in roles
        assert selected['record_method'] == 'original-cap1024'
    if mode != 'cpu-default':
        baseline = spec['runtime_baseline']
        checked_file(baseline['runtime_library'], baseline['runtime_library_sha256'], frozen)
    for name, sha in spec['runtime_jars'].items():
        checked_file(name, sha, frozen)
    for name, sha in spec['provenance'].items():
        checked_file(name, sha, frozen)
    frozen[str(path)] = digest(path)
    return spec, selected, roles



def freeze_artifacts(baseline_manifest, rowgroup_manifest, destination):
    """Build one explicit research manifest from the two approved, immutable families."""
    baseline_manifest, rowgroup_manifest = map(Path, (baseline_manifest, rowgroup_manifest))
    old = json.loads(baseline_manifest.read_text())
    rg = json.loads(rowgroup_manifest.read_text())
    assert old['research_only'] and rg['research_only']
    assert old['spark_version'] == rg['spark_version'] == '3.5.5'
    assert old['fixed_global_batch_bytes'] == rg['global_batch_bytes'] == 2**30
    variant = old['variants']['cap1024-only']
    assert variant['cap_mib'] == 1024 and variant['consumer_mib'] == 0
    assert variant['coalesce_after'] is False and variant['consumer_target_mib'] is None
    assert rg['compression'] == 'SNAPPY' and rg['decode_target_bytes'] == 2**30
    assert rg['rowgroup_target_rows'] == 65536 and rg['rowgroup_target_bytes'] == 8*2**20
    baseline_files = [
        dict(role='plugin', path=variant['plugin'], sha256=variant['plugin_sha256']),
        dict(role='codec_fixture', path=old['fixture'], sha256=old['fixture_sha256'])]
    old_sources = dict(old['sources'])
    old_sources.update(variant['sources'])
    # Freeze the full stock Spark runtime, not only the three observed classes.
    runtime_jars = dict(old['runtime_jars'])
    modes = {
        'cpu-default': dict(files=[], sources={}, record_method='Spark default column batches'),
        'pcbs-java': dict(files=baseline_files, sources=old_sources,
                          record_method='original-cap1024'),
        'rg-java': dict(files=[item for item in rg['files'] if item['role'] != 'codec_overlay'],
                        sources=rg['sources'], record_method='writer-rowgroup-extraction'),
        'rg-codec': dict(files=rg['files'], sources=rg['sources'],
                         record_method='writer-rowgroup-extraction')}
    spec = dict(research_only=True, spark_version='3.5.5', modes=modes,
                runtime_jars=runtime_jars, runtime_baseline=rg['runtime_baseline'],
                rowgroup_spec={key: rg[key] for key in (
                    'global_batch_bytes', 'decode_target_bytes', 'rowgroup_target_rows',
                    'rowgroup_target_bytes', 'record_safety_limit_bytes', 'compression')},
                provenance={str(path.resolve()): digest(path)
                            for path in (baseline_manifest, rowgroup_manifest)},
                baseline_note='cap1024-only contains observations and a redundant private '
                    '1GiB cap; at fixed global1GiB it preserves the original 0.995 budget. '
                    'No immediate scan coalescer override.')
    # Check all roles and sources before publishing a reusable manifest.
    temporary = Path(destination)
    assert not temporary.exists(), 'fresh artifact manifest required'
    frozen = {}
    for mode, selected in modes.items():
        for item in selected['files']:
            checked_file(item['path'], item['sha256'], frozen)
        for name, sha in selected['sources'].items():
            checked_file(name, sha, frozen)
    for name, sha in runtime_jars.items():
        checked_file(name, sha, frozen)
    runtime = spec['runtime_baseline']
    checked_file(runtime['runtime_library'], runtime['runtime_library_sha256'], frozen)
    write(destination, spec)
    return spec

def launch_command(out, bench, manifest, oracle, mode, heap_gib, storage, roles):
    cpu = mode == 'cpu-default'
    base_roles = ('plugin', 'codec_fixture', 'native_reader_jar')
    jars = [roles[key] for key in base_roles if key in roles]
    jars += [roles[key] for key in sorted(roles)
             if key not in base_roles + ('native_library', 'codec_overlay')]
    cp = ':'.join(([roles['codec_overlay']] if mode == 'rg-codec' else []) + jars)
    options = ('-Xms{0}g -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 '
               '-XX:-UseContainerSupport -XX:-HeapDumpOnOutOfMemoryError '
               '-Xlog:gc*:file={1}:time,level,tags').format(heap_gib, out / 'gc.log')
    if 'native_library' in roles:
        options += ' -Djava.library.path=' + str(Path(roles['native_library']).parent)
    conf = {
        'spark.sql.adaptive.enabled': 'false',
        'spark.sql.session.timeZone': 'UTC',
        'spark.sql.parquet.inferTimestampNTZ.enabled': 'false',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': (out / 'events').as_uri(),
        'spark.local.dir': str(out / 'local'),
        'spark.driver.extraJavaOptions': options,
        'spark.sql.optimizer.canChangeCachedPlanOutputPartitioning': 'false',
        'spark.sql.cache.serializer': SERIALIZERS[mode],
        'spark.serializer': 'org.apache.spark.serializer.JavaSerializer',
        'spark.serializer.objectStreamReset': '100',
        'spark.storage.unrollMemoryCheckPeriod': '16',
        'spark.rdd.compress': 'false',
        'spark.sql.inMemoryColumnarStorage.compressed': 'true',
        'spark.io.encryption.enabled': 'false',
        'spark.task.cpus': '1'}
    if not cpu:
        conf.update({
            'spark.plugins': 'com.nvidia.spark.SQLPlugin',
            'spark.executor.extraClassPath': cp,
            'spark.rapids.sql.enabled': 'true',
            'spark.rapids.sql.metrics.level': 'DEBUG',
            'spark.rapids.sql.exec.InMemoryTableScanExec': 'true',
            'spark.rapids.sql.batchSizeBytes': str(2**30),
            'spark.rapids.sql.concurrentGpuTasks': '2',
            'spark.rapids.sql.maxConcurrentGpuTasks': '2',
            'spark.rapids.memory.gpu.allocSize': '15g',
            'spark.rapids.memory.gpu.maxAllocSize': '15g',
            'spark.rapids.memory.host.offHeapLimit.enabled': 'true',
            'spark.rapids.memory.host.offHeapLimit.size': '40g',
            'spark.rapids.memory.pinnedPool.size': '2g'})
    command = [str(STOCK / 'bin/spark-submit'), '--properties-file',
               str(out / 'empty.properties'), '--class',
               'org.apache.spark.sql.pcbsworld.WorldCacheBench', '--master', 'local[8]',
               '--driver-memory', str(heap_gib) + 'g']
    if jars:
        command += ['--driver-class-path', cp, '--jars', ','.join(jars)]
    for key, value in conf.items():
        command += ['--conf', key + '=' + value]
    command += [str(bench), mode, str(manifest), str(oracle)]
    return command, conf


def process_sample(pid):
    """Read only the owned SparkSubmit JVM; HWM and /proc/io are process-global."""
    proc = Path('/proc/{}'.format(pid))
    command = (proc / 'cmdline').read_bytes()
    assert b'org.apache.spark.deploy.SparkSubmit' in command
    assert command.split(b'\0', 1)[0].rsplit(b'/', 1)[-1] == b'java'
    stat = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
    result = {'pid': pid, 'start_time_ticks': int(stat[19])}
    for line in (proc / 'status').read_text().splitlines():
        if line.startswith(('VmRSS:', 'VmHWM:')):
            key, value, unit = line.split()
            assert unit == 'kB'
            result[key.rstrip(':') + '_KiB'] = int(value)
    for line in (proc / 'io').read_text().splitlines():
        key, value = line.split(':', 1)
        if key in ('read_bytes', 'write_bytes', 'rchar', 'wchar'):
            result[key] = int(value)
    assert all(key in result for key in ('VmRSS_KiB', 'VmHWM_KiB',
                                        'read_bytes', 'write_bytes', 'rchar', 'wchar'))
    return result


def run_owned(command, env, out, timeout):
    stop = threading.Event()
    observations = set()
    errors = [0]
    started = time.monotonic()
    with (out / 'run.log').open('x') as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, start_new_session=True)
        (out / 'child.pid').write_text(str(child.pid))

        def sample():
            columns = ['epoch_s', 'pid', 'start_time_ticks', 'VmRSS_KiB', 'VmHWM_KiB',
                       'read_bytes', 'write_bytes', 'rchar', 'wchar', 'error']
            identity = None
            with (out / 'process-memory-io.csv').open('x', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=columns, lineterminator='\n')
                writer.writeheader()
                while not stop.is_set():
                    row = {'epoch_s': time.time()}
                    try:
                        observed = process_sample(child.pid)
                        current = (observed['pid'], observed['start_time_ticks'])
                        if identity is None:
                            identity = current
                        assert identity == current, 'owned JVM PID reused'
                        observations.add(current)
                        row.update(observed)
                    except Exception as error:
                        errors[0] += 1
                        row['error'] = repr(error)
                    writer.writerow(row)
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
            assert not sampler.is_alive(), 'sampler did not terminate'
    write(out / 'process.json', dict(exit_code=code, timeout=timed_out,
                                    wall_seconds=time.monotonic() - started))
    write(out / 'process-sampler-result.json', dict(
        selected_identities=[{'pid': p, 'start_time_ticks': t} for p, t in sorted(observations)],
        error_samples=errors[0],
        note='RSS sampled; HWM and IO counters process-global. No cold-disk or native attribution.'))


def quiescent(out, use_gpu, grace=30):
    """No killing: a persistent owned JVM or GPU user parks the experiment."""
    deadline = time.monotonic() + grace
    while True:
        live = []
        for proc in Path('/proc').iterdir():
            if not proc.name.isdigit():
                continue
            try:
                command = (proc / 'cmdline').read_bytes()
            except FileNotFoundError:
                continue
            if (command.split(b'\0', 1)[0].rsplit(b'/', 1)[-1] == b'java' and
                    str(out).encode() in command):
                live.append(int(proc.name))
        gpu = subprocess.check_output(
            ['nvidia-smi', '--query-compute-apps=pid', '--format=csv,noheader,nounits'],
            universal_newlines=True, timeout=max(0.001, min(5, deadline-time.monotonic()))
        ).strip() if use_gpu else ''
        assert time.monotonic() <= deadline, 'quiescence probe exceeded deadline'
        if not live and not gpu:
            return {'live_case_jvms': [], 'gpu_compute_pids': [], 'epoch_s': time.time()}
        assert time.monotonic() < deadline, 'persistent case JVM/GPU user: ' + repr((live, gpu))
        time.sleep(min(1, deadline-time.monotonic()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--bench', required=True, type=Path)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--oracle', required=True, type=Path)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--mode', required=True, choices=MODES)
    parser.add_argument('--heap-gib', required=True, type=int, choices=(8, 16, 48))
    parser.add_argument('--storage', required=True, choices=('DISK_ONLY', 'MEMORY_AND_DISK'))
    parser.add_argument('--timeout', type=int, default=10800)
    parser.add_argument('--plan-only', action='store_true')
    args = parser.parse_args()
    assert args.timeout > 0
    out = args.out.resolve()
    assert not out.exists(), 'fresh output directory required'
    frozen = {}
    source = input_files(args.manifest, frozen)
    oracle = json.loads(args.oracle.read_text())
    assert oracle['rows'] == source['total_rows'] and oracle['raw_value_bytes'] > 0
    assert len(oracle['hash_sums']) == 4 and len(oracle['null_counts']) > 0
    frozen[str(args.oracle.resolve())] = digest(args.oracle)
    spec, selected, roles = artifacts(args.artifacts, args.mode, frozen)
    built_path = args.bench.parent / 'build-result.json'
    built = json.loads(built_path.read_text())
    assert built['compile_only'] is True
    checked_file(args.bench, built['artifact_sha256'], frozen)
    assert built['source_sha256'] == digest(HERE / 'WorldCacheBench.scala'), 'rebuild harness'
    for path in (HERE / 'WorldCacheBench.scala', HERE / 'run_world.py', HERE / 'verify_world.py',
                 REAL / 'canonical_source.py', REAL / 'run_controlled.py', built_path):
        frozen[str(path.resolve())] = digest(path)
    command, conf = launch_command(out, args.bench.resolve(), args.manifest.resolve(),
                                    args.oracle.resolve(), args.mode, args.heap_gib,
                                    args.storage, roles)
    baseline = spec.get('runtime_baseline', {})
    library_path = ':'.join([str(Path(baseline['runtime_library']).parent),
                             '/usr/local/cuda/lib64']) if args.mode != 'cpu-default' else ''
    meta = dict(mode=args.mode, heap_gib=args.heap_gib, storage=args.storage,
                manifest=str(args.manifest.resolve()), oracle=str(args.oracle.resolve()),
                artifact_spec=spec, selected_artifacts=selected, roles=roles,
                argv=command, expected_conf=conf, frozen_hashes=frozen,
                expected_ld_library_path=library_path)
    if args.plan_only:
        print(json.dumps(meta, indent=2))
        return
    out.parent.mkdir(parents=True, exist_ok=True)
    assert shutil.disk_usage(out.parent).free > oracle['raw_value_bytes'] * 2 + 16 * 2**30, (
        'insufficient free disk for conservative raw-data cache/spill allowance')
    # A shared lock serializes CPU and GPU arms. It does not terminate other work.
    with (out.parent / 'world-benchmark.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        quiescent(out, use_gpu=True)
        out.mkdir()
        (out / 'events').mkdir()
        (out / 'empty.properties').write_text('# No inherited Spark defaults\n')
        write(out / 'command.json', meta)
        env = {k: v for k, v in os.environ.items() if not k.startswith(
            ('SPARK_', 'PYSPARK_', 'REAL_CACHE_', 'WORLD_CACHE_')) and k not in (
                'CLASSPATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS',
                'LD_LIBRARY_PATH')}
        env['WORLD_CACHE_STORAGE_LEVEL'] = args.storage
        if library_path:
            env['LD_LIBRARY_PATH'] = library_path
        run_owned(command, env, out, args.timeout)
        # Fresh verifier process preserves a separate raw-evidence reparse.
        try:
            result = json.loads(subprocess.check_output(
                [sys.executable, str(HERE / 'verify_world.py'), str(out)],
                universal_newlines=True))
        except Exception as error:
            result = {'outcome': 'ERROR', 'issues': ['verifier failed: ' + repr(error)]}
        write(out / 'result.json', result)
        write(out / 'quiescence.json', quiescent(out, use_gpu=True))
        if result['outcome'] not in ('PASS', 'HEAP_OOM', 'GC_LIMIT_OOM'):
            raise SystemExit(1)


if __name__ == '__main__':
    main()
