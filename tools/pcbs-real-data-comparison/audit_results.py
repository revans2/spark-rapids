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
"""Independent raw evidence audit. Runs no Spark application and changes no evidence."""
import argparse
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

_HASHES = {}


def digest(path):
    path = Path(path)
    key = (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
    if key not in _HASHES:
        value = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
                value.update(block)
        _HASHES[key] = value.hexdigest()
    return _HASHES[key]


def peak_overlap(intervals):
    changes = [(start, 1) for start, end in intervals] + [
        (end, -1) for start, end in intervals]
    current = peak = 0
    for timestamp, change in sorted(changes, key=lambda item: (item[0], item[1])):
        current += change
        peak = max(peak, current)
    return peak


def gc_records(paths):
    # Summary lines, not gc,start/heap/phase lines: one duration per completed collection.
    pattern = re.compile(
        r'^\[([^]]+)\].*GC\((\d+)\) Pause (Young|Full).* '
        r'(\d+)([KMG])->(\d+)([KMG])\((\d+)([KMG])\) ([\d.]+)ms$')
    units = {'K': 1024, 'M': 1024**2, 'G': 1024**3}
    result = {}
    for path in paths:
        for line in path.read_text(errors='replace').splitlines():
            found = pattern.match(line)
            if found:
                stamp, identity, kind, before, bu, after, au, capacity, cu, ms = found.groups()
                result[int(identity)] = {
                    'id': int(identity), 'kind': kind,
                    'end_epoch_ms': datetime.strptime(stamp, '%Y-%m-%dT%H:%M:%S.%f%z').timestamp() * 1000,
                    'duration_ms': float(ms),
                    'before_bytes_rounded': int(before) * units[bu],
                    'after_bytes_rounded': int(after) * units[au],
                    'capacity_bytes_rounded': int(capacity) * units[cu]}
    return sorted(result.values(), key=lambda value: value['id'])


def audit(case):
    case = Path(case).resolve()
    meta = json.loads((case / 'command.json').read_text())
    result = json.loads((case / 'result.json').read_text())
    process = json.loads((case / 'process.json').read_text())
    markers = [json.loads(line[len('REAL_CACHE '):])
               for line in (case / 'run.log').read_text(errors='replace').splitlines()
               if line.startswith('REAL_CACHE ')]
    issues = []

    def require(condition, message):
        if not condition:
            issues.append(message)

    config = next(item for item in markers if item['event'] == 'config')
    phases = [item for item in markers if item['event'] == 'phase']
    storage = [item for item in markers if item['event'] == 'storage']
    populations = [item for item in markers if item['event'] == 'partition']
    reads = [item for item in markers if item['event'] == 'read']
    complete = any(item['event'] == 'complete' and item['success'] for item in markers)
    require(result['process'] == process, 'recorded process differs from raw process')
    require(result['phases'] == phases and result['storage'] == storage,
            'recorded phases/storage differ from raw markers')
    require(result['complete'] == complete, 'completion differs from raw markers')
    require(result['encoded_buffer_bytes'] == sum(
        item['encoded_buffer_bytes'] for item in populations), 'encoded bytes differ')
    require(result['records'] == sum(item['records'] for item in populations),
            'record count differs')
    require(config['master'] == 'local[8]' and config['parts'] == 16, 'layout controls')
    require(any(value.lower() in ('-xmx48g', '-xmx49152m')
                for value in config['jvm_arguments']), '48g launch missing')
    require('-XX:+UseParallelGC' in config['jvm_arguments'], 'ParallelGC launch missing')
    require(meta['storage_level'] == ('DISK_ONLY' if meta.get('disk_roundtrip')
                                    else 'MEMORY_AND_DISK'), 'storage metadata mismatch')
    require(digest(meta['bench_jar']) == meta['bench_sha256'], 'bench artifact changed')
    build_path = Path(meta['bench_jar']).parent / 'build-result.json'
    build = json.loads(build_path.read_text())
    require(build['success'] and build['jar_sha256'] == meta['bench_sha256'] and
            build['source_sha256'] == meta['source_sha256'],
            'bench/source provenance differs from successful build manifest')
    if 'build_manifest_sha256' in meta:
        require(digest(build_path) == meta['build_manifest_sha256'], 'build manifest changed')
    preparation = []
    for artifact in meta['gpu_jars']:
        prepared_path = Path(artifact['path']).parent / 'manifest.json'
        prepared = json.loads(prepared_path.read_text())
        require(prepared['fixture_sha256'] == digest(prepared['fixture']),
                'prepared codec fixture changed')
        require(prepared['spark_codec_overlay_sha256'] == meta['spark_overlay']['sha256'] and
                Path(prepared['spark_codec_overlay']).resolve() ==
                Path(meta['spark_overlay']['path']).resolve(), 'prepared Spark overlay mismatch')
        for source_path, source_sha in prepared['sources'].items():
            require(digest(source_path) == source_sha, 'prepared fixture source changed')
        variants = [value for value in prepared['variants'].values()
                    if Path(value['plugin']).resolve() == Path(artifact['path']).resolve()]
        for variant in variants:
            require(variant['plugin_sha256'] == artifact['sha256'],
                    'plugin SHA differs from preparation manifest')
            require(variant['cap_mib'] == 32 and variant['coalesce_after'] ==
                    (meta['action'] == 'gpu32combine'), 'prepared variant policy mismatch')
            for source_path, source_sha in variant['sources'].items():
                require(digest(source_path) == source_sha, 'prepared variant source changed')
        require(variants or Path(prepared['fixture']).resolve() ==
                Path(artifact['path']).resolve(), 'runtime jar absent from preparation manifest')
        preparation.append({'manifest': str(prepared_path), 'sha256': digest(prepared_path)})
    for artifact in meta['gpu_jars'] + ([meta['spark_overlay']] if meta['spark_overlay'] else []):
        require(digest(artifact['path']) == artifact['sha256'], 'runtime artifact changed')
    manifest = json.loads(Path(meta['manifest']).read_text())
    require(digest(meta['manifest']) == meta['manifest_sha256'], 'canonical manifest changed')
    source = [item for item in markers if item['event'] == 'canonical_file']
    require([(item['path'], item['rows'], item['bytes'], item['schema']) for item in source] ==
            [(item['path'], item['rows'], item['bytes'], item['schema'])
             for item in manifest['files']], 'canonical footer evidence differs')
    for item in manifest['files']:
        require(digest(item['path']) == item['sha256'], 'canonical content changed')
    expected = meta['rows_per_partition'] * 16
    for read in reads:
        require(read['rows'] == expected and len(read['sums']) == 16,
                'read count or width differs')
        require(all(abs(a-b) <= max(1.0, abs(b))*1e-10
                    for a, b in zip(read['sums'], manifest['oracle']['sums'])),
                'read differs from independently materialized canonical oracle')
    event_files = sorted(path for path in (case / 'events').rglob('*') if path.is_file())
    events = []
    for path in event_files:
        for line in path.read_text().splitlines():
            events.append(json.loads(line))
    event_conf = next((item['Spark Properties'] for item in events
                       if item['Event'] == 'SparkListenerEnvironmentUpdate'), {})
    require(event_conf.get('spark.master') == 'local[8]', 'event-log master mismatch')
    for key in ('spark.plugins', 'spark.driver.extraClassPath',
                'spark.executor.extraClassPath', 'spark.rapids.sql.batchSizeBytes',
                'spark.rapids.sql.concurrentGpuTasks', 'spark.rapids.sql.maxConcurrentGpuTasks'):
        require(event_conf.get(key) == config['conf'].get(key),
                'runtime/classpath configuration differs from event log: ' + key)
    for key, default in (('spark.memory.fraction', '0.6'),
                         ('spark.storage.unrollMemoryCheckPeriod', '16'),
                         ('spark.serializer.objectStreamReset', '100')):
        require(config['conf'].get(key, default) == event_conf.get(key, default) == default,
                'nondefault memory/serialization control: ' + key)
    ended = [item for item in events if item['Event'] == 'SparkListenerTaskEnd']
    intervals = [(item['Task Info']['Launch Time'], item['Task Info']['Finish Time'])
                 for item in ended]
    failures = [item for item in ended
                if item.get('Task End Reason', {}).get('Reason') != 'Success']
    application_end = sum(item['Event'] == 'SparkListenerApplicationEnd' for item in events)
    executors = [item for item in events if item['Event'] == 'SparkListenerExecutorAdded']
    require(executors and all(item['Executor Info']['Total Cores'] == 8 for item in executors),
            'event-log executor does not provide eight CPU slots')
    successful = result['outcome'] in ('PASS', 'SMOKE_PASS')
    if result['outcome'] == 'HEAP_OOM':
        require('java.lang.OutOfMemoryError: Java heap space' in
                (case / 'run.log').read_text(errors='replace') and
                process['exit_code'] != 0 and not process['timeout'],
                'heap-OOM classification lacks strict raw evidence')
    if result['outcome'] not in ('PASS', 'SMOKE_PASS', 'HEAP_OOM'):
        require(False, 'unclassified application failure; audit is not an acceptance pass')
    if successful:
        require(complete and process['exit_code'] == 0 and not process['timeout'],
                'successful classification lacks clean completion')
        require(application_end == 1 and not failures, 'task failures or missing application end')
        require([(item['phase'], item['iteration']) for item in phases] ==
                [('fill', 0)] + [('read', i) for i in range(4)], 'raw measured phase sequence')
        require(len(populations) == 16 and all(item['rows'] == meta['rows_per_partition']
                                             for item in populations), 'raw fill incomplete')
        require(len(storage) == 5 and all(item['cached_partitions'] == 16 for item in storage),
                'BlockManager did not retain every partition')
    if meta['action'] == 'cpu':
        require(not meta['gpu_jars'] and not meta['spark_overlay'], 'CPU has overlay/plugin jars')
        require(all('spark-core_2.12-3.5.5.jar' in origin
                    for origin in config['runtime_origins'].values()), 'CPU nonstock core origin')
        require(config['cpu_batch_rows'] == '10000' and config['cpu_compression'] == 'true',
                'CPU nondefault batching/compression')
    gc_files = sorted(case.glob('gc.log*'))
    gc_text = '\n'.join(path.read_text(errors='replace') for path in gc_files)
    require('Heap Max Capacity: 48G' in gc_text and 'Parallel Workers: 4' in gc_text,
            'GC log does not confirm actual 48g/four-worker controls')
    collections = gc_records(gc_files)
    phase_audit = []
    for phase in phases:
        start, end = phase['start_epoch_ms'], phase['end_epoch_ms']
        selected = [item for item in collections if start <= item['end_epoch_ms'] <= end]
        tasks = [(a, b) for a, b in intervals if a >= start and b <= end]
        full = [item for item in selected if item['kind'] == 'Full']
        young = [item for item in selected if item['kind'] == 'Young']
        if successful:
            require(len(tasks) >= 16, 'phase lacks 16 completed partition tasks')
        phase_audit.append({
            'phase': phase['phase'], 'iteration': phase['iteration'],
            'seconds': phase['seconds'], 'completed_tasks': len(tasks),
            'peak_scheduled_task_interval_overlap': peak_overlap(tasks),
            'young_count': len(young), 'young_ms': sum(item['duration_ms'] for item in young),
            'full_count': len(full), 'full_ms': sum(item['duration_ms'] for item in full),
            'max_post_full_gc_occupancy_bytes_rounded': max(
                [item['after_bytes_rounded'] for item in full], default=None),
            'max_pre_gc_occupancy_bytes_rounded': max(
                [item['before_bytes_rounded'] for item in selected], default=None),
            'sampled_heap_occupancy_peak': phase['sampled_heap_peak'],
            'bean_gc_ms': phase['gc_ms'], 'bean_gc_count': phase['gc_count']})
    hashed = [case / 'run.log', case / 'process.json', case / 'command.json',
              case / 'result.json'] + event_files + gc_files
    return {
        'case': str(case), 'valid': not issues, 'issues': issues,
        'classification': result['outcome'], 'logical_rows': expected,
        'logical_value_bytes': expected * 128,
        'configured_heap': '48g', 'usable_heap_max': config['heap_max'],
        'runtime_origins': config['runtime_origins'],
        'canonical_manifest_sha256': meta['manifest_sha256'],
        'build_manifest_sha256': digest(build_path),
        'source_sha256': build['source_sha256'], 'gpu_preparation': preparation,
        'encoded_buffer_bytes': result['encoded_buffer_bytes'],
        'records': result['records'], 'retention': storage,
        'application_end_count': application_end, 'task_ends': len(ended),
        'failed_task_ends': len(failures),
        'executor_cpu_slots': [item['Executor Info']['Total Cores'] for item in executors],
        'peak_scheduled_task_interval_overlap': peak_overlap(intervals),
        'phases': phase_audit, 'completed_gc_summaries': len(collections),
        'all_observed_young_gc_ms': sum(item['duration_ms'] for item in collections
                                       if item['kind'] == 'Young'),
        'all_observed_full_gc_ms': sum(item['duration_ms'] for item in collections
                                      if item['kind'] == 'Full'),
        'all_observed_max_post_full_gc_occupancy_bytes_rounded': max(
            [item['after_bytes_rounded'] for item in collections if item['kind'] == 'Full'],
            default=None),
        'raw_sha256': {str(path): digest(path) for path in hashed},
        'notes': [
            'GC boundaries use collection-end wall timestamps; boundary overlap may differ from MXBean deltas.',
            'Post-full-GC occupancy includes retained cache and JVM objects; it is not an isolated payload live set.',
            'No full collection in a phase means post-full-GC occupancy is unavailable, not zero.',
            'Task launch/finish intervals include scheduling queues and can overlap beyond CPU slots; they do not measure simultaneous execution or GPU concurrency.',
            'Canonical content hashes are checked outside measured application phases.']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cases', nargs='+')
    parser.add_argument('--output', type=Path,
                        help='Exclusively create a retained JSON audit; never overwrite')
    args = parser.parse_args()
    reports = [audit(case) for case in args.cases]
    if args.output:
        with args.output.open('x') as stream:
            json.dump(reports, stream, indent=2)
            stream.write('\n')
    print(json.dumps(reports, indent=2))
    raise SystemExit(0 if all(report['valid'] for report in reports) else 1)
