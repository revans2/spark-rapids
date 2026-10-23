# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Run one explicit controlled research case in a fresh JVM, never an automatic sweep."""
import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
import time

from canonical_source import check, digest
from run_bench import HERE, STOCK, write
from verify_controlled import verify


def validate_artifacts(args):
    manifest = json.loads(args.artifacts.read_text())
    assert manifest['research_only'] and manifest['spark_version'] == '3.5.5'
    assert manifest['fixed_global_batch_bytes'] == 2**30
    variant = manifest['variants'][args.variant]
    record = variant.get('record_mib', variant.get('cap_mib'))
    assert record == args.record_mib, 'record cap differs from generated artifact'
    assert variant['consumer_mib'] == 'dynamic', 'controlled cases require dynamic consumer artifact'
    overlay = Path(manifest['spark_codec_overlay']).resolve()
    fixture = Path(manifest['fixture']).resolve()
    plugin = Path(variant['plugin']).resolve()
    for path, expected in [(overlay, manifest['spark_codec_overlay_sha256']),
                           (fixture, manifest['fixture_sha256']),
                           (plugin, variant['plugin_sha256'])]:
        assert digest(path) == expected, 'changed artifact: ' + str(path)
    for name, expected in variant.get('sources', {}).items():
        assert digest(name) == expected, 'changed variant source: ' + name
    assert variant['serializer_independent_plugin'], 'Java/codec must use identical plugin bytecode'
    for name, expected in manifest.get('sources', {}).items():
        assert digest(name) == expected, 'changed frozen source: ' + name
    jars = [path.resolve() for path in args.gpu_jar]
    assert len(jars) == len(set(jars)), 'duplicate runtime jar'
    assert fixture in jars and plugin in jars, 'include selected plugin and codec fixture jars'
    other_plugins = {Path(v['plugin']).resolve() for v in manifest['variants'].values()
                     if Path(v['plugin']).resolve() != plugin}
    assert not set(jars) & other_plugins, 'conflicting plugin variants'
    for name, expected in manifest['runtime_jars'].items():
        assert digest(name) == expected, 'changed stock Spark runtime jar: ' + name
    built = json.loads((args.bench.parent / 'build-result.json').read_text())
    assert built['success'] and built['jar_sha256'] == digest(args.bench)
    assert built['source_sha256'] == digest(HERE / 'RealCacheBench.scala'), 'rebuild harness'
    return overlay, jars, manifest, variant


def process_tree(root):
    """PID/start-time pairs for owned descendants, including a profiler's separate target group."""
    found = {}
    pending = [root]
    while pending:
        pid = pending.pop()
        if pid in found:
            continue
        try:
            stat = Path('/proc/{}/stat'.format(pid)).read_text().rsplit(')', 1)[1].split()
            found[pid] = int(stat[19])
            children = Path('/proc/{}/task/{}/children'.format(pid, pid)).read_text()
            pending.extend(int(value) for value in children.split())
        except (OSError, ValueError, IndexError):
            continue
    return found


def signal_owned(processes, sig):
    for pid, start in processes.items():
        try:
            stat = Path('/proc/{}/stat'.format(pid)).read_text().rsplit(')', 1)[1].split()
            if int(stat[19]) == start:  # Never signal a reused PID.
                os.kill(pid, sig)
        except (OSError, ValueError, IndexError):
            continue


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--bench', required=True, type=Path)
    parser.add_argument('--canonical', required=True, type=Path)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--variant', required=True)
    parser.add_argument('--gpu-jar', required=True, action='append', type=Path)
    parser.add_argument('--serializer', required=True, choices=['java', 'codec'])
    parser.add_argument('--reset', required=True, type=int, choices=[100, 1])
    parser.add_argument('--period', required=True, type=int, choices=[16, 1])
    parser.add_argument('--storage', required=True, choices=['DISK_ONLY', 'MEMORY_AND_DISK'])
    parser.add_argument('--record-mib', required=True, type=int, choices=[64, 128, 256, 512, 1024])
    parser.add_argument('--consumer-mib', type=int, choices=[0, 256, 1024], default=1024)
    parser.add_argument('--consumer-targets', choices=['0,256,1024', '1024,256,0'],
                        help='Compare all three consumer targets on the same populated cache')
    parser.add_argument('--rows-per-partition', type=int, default=50331648)
    parser.add_argument('--timeout', type=int, default=7200)
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--profile', choices=['none', 'jfr', 'nsys'], default='none',
                        help='Separate diagnostic run; never combine its timings with primary runs')
    parser.add_argument('--sample-gpu', action='store_true',
                        help='Optional separate nvidia-smi utilization/memory/power sampler at 1 Hz')
    args = parser.parse_args()
    assert args.rows_per_partition > 0 and args.timeout > 0
    assert (args.rows_per_partition <= 1000000 if args.smoke else
            args.rows_per_partition == 50331648), 'only fixed 96 GiB full cases or explicit smoke'
    overlay, jars, artifacts, variant = validate_artifacts(args)
    source = check(args.manifest)  # Identical full hashing/page-cache preparation outside the JVM.
    assert source['config']['rows_per_partition'] == args.rows_per_partition
    assert {str(Path(e['path']).parent) for e in source['files']} == {str(args.canonical.resolve())}
    out = args.out.resolve()
    assert not out.exists(), 'fresh output directory required'
    assert shutil.disk_usage(out.parent).free > args.rows_per_partition * 16 * 128 + 16*2**30
    out.mkdir(parents=True)
    events_dir = out / 'events'
    events_dir.mkdir()
    options = ('-Xms48g -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 '
               '-XX:-UseContainerSupport -XX:-HeapDumpOnOutOfMemoryError '
               '-Xlog:gc*:file=' + str(out / 'gc.log') + ':time,level,tags')
    if args.profile == 'jfr':
        options += (' -XX:StartFlightRecording=name=allocation,settings=profile,dumponexit=true,'
                    'maxsize=512m,filename=' + str(out / 'allocation.jfr'))
    cache_serializer = ('com.nvidia.spark.ParquetCachedBatchSerializer' if args.serializer == 'java'
                        else 'org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer')
    runtime_cp = ':'.join(map(str, [overlay] + jars))
    conf = {
        'spark.sql.adaptive.enabled': 'false',
        'spark.eventLog.enabled': 'true',
        'spark.eventLog.dir': events_dir.as_uri(),
        'spark.local.dir': str(out / 'local'),
        'spark.driver.extraJavaOptions': options,
        'spark.sql.optimizer.canChangeCachedPlanOutputPartitioning': 'false',
        'spark.plugins': 'com.nvidia.spark.SQLPlugin',
        'spark.sql.cache.serializer': cache_serializer,
        'spark.executor.extraClassPath': runtime_cp,
        'spark.serializer.objectStreamReset': str(args.reset),
        'spark.storage.unrollMemoryCheckPeriod': str(args.period),
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
    properties = out / 'empty-spark.properties'
    properties.write_text('# Intentionally empty; no inherited spark-defaults.conf\n')
    cmd = [str(STOCK / 'bin/spark-submit'), '--properties-file', str(properties),
           '--class', 'RealCacheBench', '--master', 'local[8]', '--driver-memory', '48g',
           '--driver-class-path', runtime_cp, '--jars', ','.join(map(str, jars))]
    for key, value in conf.items():
        cmd += ['--conf', key + '=' + value]
    cmd += [str(args.bench.resolve()), 'gpu-controlled', str(args.canonical.resolve()),
            str(args.rows_per_partition)]
    application_cmd = list(cmd)
    if args.profile == 'nsys':
        nsys = Path('/opt/nvidia/nsight-systems/current/bin/nsys')
        assert nsys.is_file(), 'Nsight Systems executable unavailable'
        cmd = [str(nsys), 'profile', '--trace=cuda,nvtx', '--sample=none', '--cpuctxsw=none',
               '--force-overwrite=false', '--kill=sigterm', '--wait=all',
               '--output=' + str(out / 'gpu-trace')] + cmd
    env = dict(os.environ)
    removed = []
    for key in list(env):
        if key.startswith(('SPARK_', 'PYSPARK_', 'REAL_CACHE_')) or key in (
                'CLASSPATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'):
            removed.append(key)
            del env[key]
    env['REAL_CACHE_STORAGE_LEVEL'] = args.storage
    targets = ([int(v) for v in args.consumer_targets.split(',')] if args.consumer_targets
               else [args.consumer_mib])
    env['REAL_CACHE_CONSUMER_TARGETS'] = ','.join(map(str, targets))
    meta = {
        'action': 'gpu-controlled', 'smoke': args.smoke, 'argv': cmd,
        'application_argv': application_cmd, 'profile': args.profile,
        'profile_note': 'Diagnostic timings are separate from the unprofiled primary matrix.',
        'sample_gpu': args.sample_gpu, 'procfs_clock_ticks_per_second': os.sysconf('SC_CLK_TCK'),
        'removed_environment_keys': removed, 'rows_per_partition': args.rows_per_partition,
        'serializer': args.serializer, 'reset': args.reset, 'period': args.period,
        'storage_level': args.storage, 'record_mib': args.record_mib,
        'consumer_mib': args.consumer_mib, 'consumer_targets': targets, 'variant': args.variant,
        'manifest': str(args.manifest.resolve()), 'manifest_sha256': digest(args.manifest),
        'artifacts': str(args.artifacts.resolve()), 'artifacts_sha256': digest(args.artifacts),
        'variant_definition': variant, 'bench_jar': str(args.bench.resolve()),
        'bench_sha256': digest(args.bench), 'source_sha256': digest(HERE / 'RealCacheBench.scala'),
        'build_manifest_sha256': digest(args.bench.parent / 'build-result.json'),
        'gpu_jars': [{'path': str(p), 'sha256': digest(p)} for p in jars],
        'spark_overlay': {'path': str(overlay), 'sha256': digest(overlay)},
        'input_check': 'full SHA256 before JVM; OS page cache is not cold',
        'expected_conf': conf}
    write(out / 'command.json', meta)
    stop = threading.Event()
    owned = {}
    owned_lock = threading.Lock()
    process_samples = {'valid_samples': 0, 'error_samples': 0, 'selected_pids': set()}

    def sample(pid):
        with (out / 'process-memory.csv').open('x', newline='') as stream:
            writer = csv.writer(stream, lineterminator='\n')
            writer.writerow(['epoch_s', 'pid', 'VmRSS_KiB', 'VmHWM_KiB', 'utime_ticks',
                             'stime_ticks', 'read_bytes', 'write_bytes', 'rchar', 'wchar', 'error'])
            while not stop.is_set():
                try:
                    tree = process_tree(pid)
                    with owned_lock:
                        owned.update(tree)
                    target_pid = pid
                    if args.profile == 'nsys':
                        candidates = [p for p in tree if b'org.apache.spark.deploy.SparkSubmit' in
                                      Path('/proc/{}/cmdline'.format(p)).read_bytes()]
                        if len(candidates) != 1:
                            raise RuntimeError('SparkSubmit JVM not uniquely visible under profiler')
                        target_pid = candidates[0]
                    command_line = Path('/proc/{}/cmdline'.format(target_pid)).read_bytes()
                    executable = command_line.split(b'\0', 1)[0].rsplit(b'/', 1)[-1]
                    if executable != b'java' or b'org.apache.spark.deploy.SparkSubmit' not in command_line:
                        raise RuntimeError('selected process is not yet the SparkSubmit JVM')
                    fields = {}
                    for line in Path('/proc/{}/status'.format(target_pid)).read_text().splitlines():
                        if line.startswith(('VmRSS:', 'VmHWM:')):
                            name, value, _ = line.split()
                            fields[name.rstrip(':')] = int(value)
                    # comm can contain spaces/parentheses; fields after its final ')' start at state.
                    stat = Path('/proc/{}/stat'.format(target_pid)).read_text().rsplit(')', 1)[1].split()
                    io = {}
                    for line in Path('/proc/{}/io'.format(target_pid)).read_text().splitlines():
                        key, value = line.split(':', 1)
                        io[key] = int(value)
                    writer.writerow([time.time(), target_pid, fields.get('VmRSS'), fields.get('VmHWM'),
                                     int(stat[11]), int(stat[12]), io['read_bytes'],
                                     io['write_bytes'], io['rchar'], io['wchar'], ''])
                    process_samples['valid_samples'] += 1
                    process_samples['selected_pids'].add(target_pid)
                except Exception as error:
                    process_samples['error_samples'] += 1
                    writer.writerow([time.time()] + [''] * 9 + [repr(error)])
                stream.flush()
                stop.wait(1)

    gpu_sampler = None
    gpu_stream = None
    gpu_error = None
    if args.sample_gpu:
        gpu_stream = (out / 'gpu-samples.csv').open('x')
        gpu_cmd = ['nvidia-smi',
                   '--query-gpu=timestamp,uuid,utilization.gpu,utilization.memory,memory.used,power.draw',
                   '--format=csv,nounits', '--loop=1']
        write(out / 'gpu-sampler-command.json', {'argv': gpu_cmd,
              'note': 'Device-wide samples include other processes; optional instrumentation.'})
        try:
            gpu_sampler = subprocess.Popen(gpu_cmd, stdout=gpu_stream,
                                           stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                                           start_new_session=True)
        except OSError as error:
            gpu_error = repr(error)
    started = time.monotonic()
    timed_out = False
    with (out / 'run.log').open('x') as log:
        child = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, env=env, start_new_session=True)
        (out / 'child.pid').write_text(str(child.pid))
        sampler = threading.Thread(target=sample, args=(child.pid,), daemon=True)
        sampler.start()
        try:
            code = child.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            with owned_lock:
                owned.update(process_tree(child.pid))
                to_stop = dict(owned)
            signal_owned(to_stop, signal.SIGTERM)
            try:
                code = child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                code = None
            # Refresh surviving owned trees for children born during the graceful-stop interval.
            with owned_lock:
                for pid, start in list(owned.items()):
                    current = process_tree(pid)
                    if current.get(pid) == start:
                        owned.update(current)
                to_stop = dict(owned)
            # Nsight may use a separate target group; do not rely on wrapper killpg alone.
            signal_owned(to_stop, signal.SIGKILL)
            if code is None:
                code = child.wait(timeout=10)
        finally:
            stop.set()
            sampler.join(timeout=3)
    process_samples['selected_pids'] = sorted(process_samples['selected_pids'])
    process_samples['verified_samples_available'] = process_samples['valid_samples'] > 0
    process_samples['note'] = ('Physical I/O and CPU/RSS sampling unavailable: no actual JVM samples.'
                               if not process_samples['valid_samples'] else
                               'read_bytes/write_bytes are physical I/O; rchar/wchar are syscall '
                               'traffic. Neither equals cache payload bytes.')
    write(out / 'process-sampler-result.json', process_samples)
    gpu_code = None
    gpu_running_at_stop = False
    if gpu_sampler is not None:
        gpu_running_at_stop = gpu_sampler.poll() is None
        if gpu_running_at_stop:
            gpu_sampler.terminate()
        try:
            gpu_code = gpu_sampler.wait(timeout=5)
        except subprocess.TimeoutExpired:
            gpu_sampler.kill()
            gpu_code = gpu_sampler.wait()
            gpu_error = 'sampler did not terminate promptly'
        if not gpu_running_at_stop:
            gpu_error = 'sampler exited before benchmark completed'
    if gpu_stream is not None:
        gpu_stream.close()
    write(out / 'gpu-sampler-result.json', {'requested': args.sample_gpu,
          'running_at_benchmark_end': gpu_running_at_stop, 'exit_code': gpu_code,
          'error': gpu_error})
    write(out / 'process.json', {'exit_code': code, 'timeout': timed_out,
                                'wall_seconds': time.monotonic()-started})
    profile_files = ([out / 'allocation.jfr'] if args.profile == 'jfr' else
                     sorted(out.glob('gpu-trace*')) if args.profile == 'nsys' else [])
    profile_files = [p for p in profile_files if p.is_file()]
    expected_profile = (out / 'allocation.jfr' if args.profile == 'jfr' else
                        out / 'gpu-trace.nsys-rep')
    profile_error = None
    if args.profile != 'none':
        if not expected_profile.is_file() or expected_profile.stat().st_size == 0:
            profile_error = 'expected nonempty profile artifact missing'
        elif code != 0 or timed_out:
            profile_error = 'profiled process failed or timed out; inspect application/profiler log'
    write(out / 'profile-result.json', {'profile': args.profile, 'exit_code': code,
          'timeout': timed_out, 'error': profile_error,
          'raw_files': [{'path': str(p), 'bytes': p.stat().st_size} for p in profile_files],
          'note': 'Diagnostic timings are not primary measurements. JFR maxsize is a recording '
                  'retention target; individual chunks can exceed the requested bound.'})
    try:
        result = verify(out)
    except Exception as error:
        result = {'outcome': 'VERIFIER_ERROR', 'issues': [repr(error)]}
    write(out / 'result.json', result)
    if profile_error or result['outcome'] not in ('PASS', 'SMOKE_PASS', 'HEAP_OOM'):
        raise SystemExit(1)


if __name__ == '__main__':
    main()
