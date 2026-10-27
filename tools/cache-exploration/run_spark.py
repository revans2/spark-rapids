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

"""Launch one local Spark cache experiment with captured commands and logs."""
import argparse
import hashlib
import json
import os
import signal
from pathlib import Path
import subprocess
import time

from measure_process import gpu_sample, heap_sample, peak, proc_tree, residency


def digest(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            value.update(chunk)
    return value.hexdigest()


def verified_provenance(path):
    value = json.loads(path.read_text())
    artifacts = [value['application']] + value['plugin_dependencies']
    for entry in artifacts:
        if digest(entry['path']) != entry['sha256']:
            raise ValueError('Build artifact changed: ' + entry['path'])
    for entry in value['child_classpath_entries']:
        location = Path(entry['path'])
        if entry['kind'] == 'file':
            if digest(location) != entry['sha256']:
                raise ValueError('Child dependency changed: ' + str(location))
        else:
            files = sorted(str(item.relative_to(location)) for item in location.rglob('*') if item.is_file())
            if files != sorted(item['relative_path'] for item in entry['files']):
                raise ValueError('Child directory contents changed: ' + str(location))
            for item in entry['files']:
                if digest(location / item['relative_path']) != item['sha256']:
                    raise ValueError('Child class/resource changed: ' + str(location / item['relative_path']))
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-dir', type=Path, default=Path('/data/tmp/vortex-spark-prototype'))
    parser.add_argument('--spark-home', type=Path, default=Path('/home/roberte/src/spark_3.5.5'))
    parser.add_argument('--format', choices=['default', 'pcbs', 'vortex'], required=True)
    parser.add_argument('--write-mode', choices=['cpu', 'gpu'], default='cpu')
    parser.add_argument('--read-modes', default='cpu,gpu')
    parser.add_argument('--fixture', choices=['fixed', 'fixed-wide', 'strings-low', 'strings-wide', 'interval'],
                        default='strings-wide')
    size = parser.add_mutually_exclusive_group()
    size.add_argument('--rows', type=int)
    size.add_argument('--task-size-mib', type=int, choices=[256, 512, 1024, 2048],
                      help='Approved fixed-wide per-task scale case; requires two waves')
    parser.add_argument('--partitions', type=int, default=4)
    parser.add_argument('--warmups', type=int, default=2)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--tasks', type=int, default=2)
    parser.add_argument('--heap', choices=['512m', '1g', '2g', '16g'], default='1g',
                        help='Explicit JVM heap budget; default 1g, primary benchmark baseline 16g')
    parser.add_argument('--offheap-limit', choices=['256m', '512m', '1g', '2g', '4g'], default='4g',
                        help='RAPIDS-managed host off-heap cap; excludes Arrow/Rust allocations')
    parser.add_argument('--rss-abort-bytes', type=int, default=4 * 1024 ** 3,
                        help='Sampled early-stop guard in bytes; default 4 GiB, maximum 32 GiB')
    parser.add_argument('--measure-memory', action='store_true',
                        help='Sample whole-job heap/RSS/PSS/GPU; collection overhead affects timings')
    parser.add_argument('--storage', choices=['dataframe-default', 'serialized-memory-and-disk',
                                             'serialized-memory-only'], default='dataframe-default')
    parser.add_argument('--load-plugin', action='store_true',
                        help='Load RAPIDS even if every requested execution mode is CPU')
    parser.add_argument('--payload-audit', action='store_true',
                        help='Run payload audit AFTER timing/capacity snapshots')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--timeout', type=float, default=300)
    args = parser.parse_args()
    if args.rss_abort_bytes < 1 or args.rss_abort_bytes > 32 * 1024 ** 3:
        parser.error('RSS guard must be positive and no greater than 32 GiB')
    if args.tasks < 1 or args.tasks > 4 or args.timeout <= 0:
        parser.error('tasks must be 1..4 and timeout positive')
    if args.task_size_mib is not None:
        if args.fixture != 'fixed-wide' or args.partitions != 2 * args.tasks:
            parser.error('scale cases require fixed-wide and partitions equal to twice tasks')
        args.rows = args.task_size_mib * 1024 ** 2 // 256 * args.partitions
    elif args.rows is None:
        args.rows = 524288
    if args.rows < 1 or (args.task_size_mib is None and args.rows > 524288) \
            or args.partitions < 1 or args.partitions > 16:
        parser.error('use an approved task-size scale case for larger inputs')
    prototype = (args.build_dir / 'vortex-spark-exploration.jar').resolve(strict=True)
    dependencies = (args.build_dir / 'plugin-jars.txt').read_text().strip()
    if not dependencies:
        parser.error('prototype build directory has no plugin dependency list')
    source = Path(__file__).resolve().with_name('spark_cache_bench.py')
    needs_gpu = args.write_mode == 'gpu' or 'gpu' in args.read_modes.split(',')
    plugin = needs_gpu or args.load_plugin
    provenance = verified_provenance(args.build_dir / 'provenance.json')
    if prototype != Path(provenance['application']['path']).resolve():
        parser.error('Launched prototype differs from verified application')
    launched_dependencies = [Path(item).resolve() for item in dependencies.split(',')]
    verified_dependencies = [Path(item['path']).resolve() for item in provenance['plugin_dependencies']]
    if launched_dependencies != verified_dependencies:
        parser.error('Launched dependency order/list differs from verified provenance')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / 'build-provenance.json').write_text(json.dumps(provenance, indent=2) + '\n')
    snapshot = args.output_dir / 'spark_cache_bench.py'
    snapshot.write_bytes(source.read_bytes())
    command = [str(args.spark_home / 'bin' / 'spark-submit'), '--master', 'local[' + str(args.tasks) + ']',
               '--driver-memory', args.heap, '--driver-java-options',
               '--add-opens=java.base/java.nio=ALL-UNNAMED', '--jars',
               dependencies + ',' + str(prototype),
               '--conf', 'spark.rapids.memory.host.offHeapLimit.enabled=true',
               '--conf', 'spark.rapids.memory.host.offHeapLimit.size=' + args.offheap_limit,
               '--conf', 'spark.rapids.sql.enabled=' + str(args.write_mode == 'gpu').lower(),
               '--conf', 'spark.executor.cores=' + str(args.tasks)]
    if plugin:
        command += ['--conf', 'spark.plugins=com.nvidia.spark.SQLPlugin']
    command += [str(snapshot), '--format', args.format, '--write-mode', args.write_mode,
                '--read-modes', args.read_modes, '--fixture', args.fixture, '--rows', str(args.rows),
                '--partitions', str(args.partitions), '--warmups', str(args.warmups),
                '--repeats', str(args.repeats), '--storage', args.storage,
                '--output', str(args.output_dir / 'result.json')]
    if args.task_size_mib is not None:
        command += ['--task-size-mib', str(args.task_size_mib)]
    if args.payload_audit:
        command += ['--payload-audit-class', 'org.apache.spark.sql.cacheexploration.CachePayloadAudit']
    environment = os.environ.copy()
    environment.update(SPARK_HOME=str(args.spark_home), SPARK_LOCAL_IP='127.0.0.1')
    # Use the sandbox-selected Python, not a separate mounted Conda environment.
    environment.setdefault('PYSPARK_PYTHON', os.sys.executable)
    environment.setdefault('PYSPARK_DRIVER_PYTHON', os.sys.executable)
    if not plugin:
        environment['CUDA_VISIBLE_DEVICES'] = ''
    manifest = {'command': command, 'gpu_plugin_loaded': plugin,
                'cuda_visibility': environment.get('CUDA_VISIBLE_DEVICES', 'inherited'),
                'source_sha256': digest(snapshot), 'source_snapshot': str(snapshot),
                'launcher_sha256': digest(Path(__file__)),
                'task_size_mib': args.task_size_mib, 'rows': args.rows,
                'partitions': args.partitions, 'task_slots': args.tasks,
                'prototype_sha256': hashlib.sha256(prototype.read_bytes()).hexdigest(),
                'build_provenance_snapshot': str(args.output_dir / 'build-provenance.json'),
                'managed_offheap_limit': args.offheap_limit,
                'managed_offheap_limit_scope': 'Only RAPIDS-managed allocations after allocator initialization; excludes Arrow/Rust',
                'sampled_rss_abort_threshold_bytes': args.rss_abort_bytes,
                'memory_instrumented': args.measure_memory,
                'scope': 'One fresh local JVM/cache; population is cold, reuse has explicit warmups. '
                         'JVM heap and RAPIDS-managed host memory are capped; Arrow/Rust are outside '
                         'the plugin cap. Dataset/concurrency bounds apply independently. A sampled '
                         f'{args.rss_abort_bytes} byte process-tree RSS early-stop guard monitors this '
                         'experiment; samples may miss brief peaks.'}
    (args.output_dir / 'command.json').write_text(json.dumps(manifest, indent=2) + '\n')
    with (args.output_dir / 'stdout.txt').open('w') as stdout, \
            (args.output_dir / 'stderr.txt').open('w') as stderr:
        try:
            process = subprocess.Popen(command, env=environment, stdout=stdout, stderr=stderr,
                                       start_new_session=True)
            manifest['root_pid'] = process.pid
            try:
                samples = []
                started = time.monotonic()
                with (args.output_dir / 'memory-samples.jsonl').open('w') as memory_log:
                    while process.poll() is None:
                        if time.monotonic() - started > args.timeout:
                            raise subprocess.TimeoutExpired(command, args.timeout)
                        pids = proc_tree(process.pid)
                        sample = {'elapsed_seconds': time.monotonic() - started, 'pids': sorted(pids),
                                  'process_tree': residency(pids)}
                        rss = sample['process_tree']['rss_bytes']
                        if rss is not None and rss > args.rss_abort_bytes:
                            sample['rss_guard_triggered'] = True
                            memory_log.write(json.dumps(sample) + '\n')
                            memory_log.flush()
                            manifest['safety_stop_sample'] = sample
                            (args.output_dir / 'memory.json').write_text(json.dumps({
                                'status': 'RSS_GUARD', 'trigger_sample': sample,
                                'threshold_bytes': args.rss_abort_bytes}, indent=2) + '\n')
                            raise RuntimeError('Conservative RSS guard reached; stopping experiment')
                        if args.measure_memory:
                            sample['heap'] = heap_sample(process.pid)
                            sample['gpu'] = gpu_sample()
                        samples.append(sample)
                        memory_log.write(json.dumps(sample) + '\n')
                        memory_log.flush()
                        time.sleep(0.25 if not args.measure_memory else 0.5)
                returncode = process.wait()
                memory = {'sampled_peak_tree_rss_bytes': peak(samples, ['process_tree', 'rss_bytes']),
                          'sampled_peak_tree_pss_bytes': peak(samples, ['process_tree', 'pss_bytes']),
                          'sampled_peak_heap_used_bytes': peak(samples, ['heap', 'used_bytes']),
                          'sample_count': len(samples),
                          'scope': 'Sampled whole JVM/process tree including startup/reference actions/audit. '
                                   'May miss brief allocations; RSS is not native allocation accounting. '
                                   'GPU results stay by reported PID to expose namespace attribution.'}
                (args.output_dir / 'memory.json').write_text(json.dumps(memory, indent=2) + '\n')
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
                raise
            manifest['returncode'] = returncode
        except subprocess.TimeoutExpired:
            manifest['status'] = 'TIMEOUT'
            raise
        except BaseException as error:
            manifest['status'] = 'ABORTED'
            manifest['error'] = repr(error)
            raise
        finally:
            (args.output_dir / 'command.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print(json.dumps({'returncode': returncode, 'result': str(args.output_dir / 'result.json')}))
    raise SystemExit(returncode if returncode >= 0 else 128 - returncode)


if __name__ == '__main__':
    main()
