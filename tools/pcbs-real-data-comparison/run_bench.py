# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""One explicit build/materialization/fresh-JVM case; no automatic sweep."""
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

from canonical_source import check, digest, freeze
from verify_bench import verify

HERE = Path(__file__).resolve().parent
STOCK = Path('/home/roberte/src/spark_3.5.5')


def write(path, value):
    with Path(path).open('x') as stream:
        json.dump(value, stream, indent=2)
        stream.write('\n')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['build', 'canonical', 'cpu', 'gpu32split', 'gpu32combine'])
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--bench', type=Path)
    parser.add_argument('--canonical', type=Path)
    parser.add_argument('--manifest', type=Path)
    parser.add_argument('--rows-per-partition', type=int, default=50331648)
    parser.add_argument('--heap', default='48g', choices=['48g'])
    parser.add_argument('--gpu-jar', action='append', default=[], type=Path)
    parser.add_argument('--spark-overlay', type=Path)
    parser.add_argument('--timeout', type=int, default=7200)
    parser.add_argument('--smoke', action='store_true')
    parser.add_argument('--disk-roundtrip', action='store_true',
                        help='Smoke-only DISK_ONLY gate, never a full comparison')
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    if args.action == 'build':
        assert not args.gpu_jar and not args.spark_overlay
        classes = out / 'classes'
        classes.mkdir()
        jars = sorted((STOCK / 'assembly/target/scala-2.12/jars').glob('*.jar'))
        assert jars
        cp = ':'.join(str(p) for p in jars)
        jar = out / 'real-cache-bench.jar'
        commands = [['java', '-cp', cp, 'scala.tools.nsc.Main', '-classpath', cp,
                     '-d', str(classes), str(HERE / 'RealCacheBench.scala')],
                    ['jar', 'cf', str(jar), '-C', str(classes), '.']]
        write(out / 'build-command.json', {'commands': commands,
              'source_sha256': digest(HERE / 'RealCacheBench.scala'),
              'classpath': [str(p) for p in jars]})
        with (out / 'build.log').open('x') as log:
            for cmd in commands:
                subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        write(out / 'build-result.json', {'success': True, 'jar': str(jar),
              'jar_sha256': digest(jar), 'source_sha256': digest(HERE / 'RealCacheBench.scala')})
        return
    assert args.bench and args.bench.is_file() and args.canonical
    build_manifest = args.bench.parent / 'build-result.json'
    built = json.loads(build_manifest.read_text())
    assert built['success'] and built['jar_sha256'] == digest(args.bench)
    assert built['source_sha256'] == digest(HERE / 'RealCacheBench.scala'), 'Rebuild changed harness'
    assert args.rows_per_partition > 0
    assert not args.smoke or args.rows_per_partition <= 1000000
    assert not args.disk_roundtrip or (args.smoke and args.action != 'canonical')
    canonical = args.canonical.resolve()
    gpu = args.action.startswith('gpu')
    assert bool(args.gpu_jar) == gpu and bool(args.spark_overlay) == gpu
    if args.action == 'canonical':
        assert not canonical.exists() and not args.manifest
        assert shutil.disk_usage(out).free > args.rows_per_partition * 16 * 128 * 2 + 16*2**30
    else:
        assert args.manifest and args.manifest.is_file()
        manifest = check(args.manifest)  # Full hashes outside timed JVM; warms OS page cache.
        assert manifest['config']['rows_per_partition'] == args.rows_per_partition
        assert {str(Path(e['path']).parent) for e in manifest['files']} == {str(canonical)}
        assert shutil.disk_usage(out).free > args.rows_per_partition * 16 * 128 + 16*2**30
    event_dir = out / 'events'
    event_dir.mkdir()
    options = '-Xms48g -XX:+UseParallelGC -XX:ParallelGCThreads=4 -XX:ConcGCThreads=2 ' \
        '-XX:-UseContainerSupport -XX:-HeapDumpOnOutOfMemoryError ' \
        '-Xlog:gc*:file=' + str(out / 'gc.log') + ':time,level,tags'
    conf = {'spark.sql.adaptive.enabled': 'false', 'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': event_dir.as_uri(), 'spark.local.dir': str(out / 'local'),
            'spark.driver.extraJavaOptions': options,
            'spark.sql.optimizer.canChangeCachedPlanOutputPartitioning': 'false'}
    if gpu:
        conf.update({'spark.plugins': 'com.nvidia.spark.SQLPlugin',
                     'spark.sql.cache.serializer':
                     'org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer',
                     'spark.executor.extraClassPath': ':'.join(
                         [str(args.spark_overlay.resolve())] +
                         [str(p.resolve()) for p in args.gpu_jar]),
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
                     'spark.rapids.memory.pinnedPool.size': '2g'})
    properties = out / 'empty-spark.properties'
    properties.write_text('# Intentionally empty: no inherited spark-defaults.conf\n')
    cmd = [str(STOCK / 'bin/spark-submit'), '--properties-file', str(properties),
           '--class', 'RealCacheBench', '--master', 'local[8]', '--driver-memory', args.heap]
    if gpu:
        cmd += ['--driver-class-path', ':'.join(
                    [str(args.spark_overlay.resolve())] + [str(p.resolve()) for p in args.gpu_jar]),
                '--jars', ','.join(str(p.resolve()) for p in args.gpu_jar)]
    for key, value in conf.items():
        cmd += ['--conf', key + '=' + value]
    cmd += [str(args.bench.resolve()), args.action, str(canonical), str(args.rows_per_partition)]
    env = dict(os.environ)
    removed = []
    for key in list(env):
        if key.startswith(('SPARK_', 'PYSPARK_')) or key in (
                'CLASSPATH', 'JAVA_TOOL_OPTIONS', '_JAVA_OPTIONS', 'JDK_JAVA_OPTIONS'):
            removed.append(key)
            del env[key]
    env.pop('REAL_CACHE_DISK_SMOKE', None)
    if args.disk_roundtrip:
        env['REAL_CACHE_DISK_SMOKE'] = 'true'
    meta = {'action': args.action, 'smoke': args.smoke,
            'disk_roundtrip': args.disk_roundtrip, 'argv': cmd, 'removed_environment_keys': removed,
            'rows_per_partition': args.rows_per_partition,
            'manifest': str(args.manifest.resolve()) if args.manifest else None,
            'manifest_sha256': digest(args.manifest) if args.manifest else None,
            'bench_jar': str(args.bench.resolve()), 'bench_sha256': digest(args.bench),
            'source_sha256': digest(HERE / 'RealCacheBench.scala'),
            'build_manifest_sha256': digest(build_manifest),
            'gpu_jars': [{'path': str(p.resolve()), 'sha256': digest(p)} for p in args.gpu_jar],
            'spark_overlay': {'path': str(args.spark_overlay.resolve()),
                              'sha256': digest(args.spark_overlay)} if gpu else None,
            'input_check': 'full file SHA256 before JVM; OS page cache is not cold',
            'storage_level': 'DISK_ONLY' if args.disk_roundtrip else 'MEMORY_AND_DISK'}
    write(out / 'command.json', meta)
    stop = threading.Event()

    def sample(pid):
        with (out / 'process-memory.csv').open('x', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(['epoch_s', 'VmRSS_KiB', 'VmHWM_KiB', 'error'])
            while not stop.is_set():
                try:
                    fields = {}
                    for line in Path('/proc/{}/status'.format(pid)).read_text().splitlines():
                        if line.startswith(('VmRSS:', 'VmHWM:')):
                            name, value, _ = line.split()
                            fields[name.rstrip(':')] = int(value)
                    writer.writerow([time.time(), fields.get('VmRSS'), fields.get('VmHWM'), ''])
                except Exception as error:
                    writer.writerow([time.time(), '', '', repr(error)])
                stream.flush()
                stop.wait(1)
    started = time.monotonic()
    timeout = False
    with (out / 'run.log').open('x') as log:
        child = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT,
                                 stdin=subprocess.DEVNULL, env=env, start_new_session=True)
        (out / 'child.pid').write_text(str(child.pid))
        sampler = threading.Thread(target=sample, args=(child.pid,), daemon=True)
        sampler.start()
        try:
            code = child.wait(timeout=args.timeout)
        except subprocess.TimeoutExpired:
            timeout = True
            os.killpg(child.pid, signal.SIGKILL)
            code = child.wait()
        finally:
            stop.set()
            sampler.join(timeout=3)
    write(out / 'process.json', {'exit_code': code, 'timeout': timeout,
                                'wall_seconds': time.monotonic()-started})
    if args.action == 'canonical':
        assert code == 0 and not timeout, 'Canonical generation failed; inspect retained log'
        freeze(out / 'run.log', out / 'canonical-manifest.json')
    else:
        result = verify(out)
        write(out / 'result.json', result)
        if result['outcome'] not in ('PASS', 'SMOKE_PASS'):
            raise SystemExit(1)


if __name__ == '__main__':
    main()
