# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Verify controlled real-PCBS cases without discarding failed or incomplete observations."""
import argparse
import json
import math
from pathlib import Path
import statistics
import shlex
from urllib.parse import urlparse

from canonical_source import digest, events
from audit_results import gc_records


def verify(case):
    case = Path(case)
    meta = json.loads((case / 'command.json').read_text())
    status = json.loads((case / 'process.json').read_text())
    data = events(case / 'run.log')
    raw = (case / 'run.log').read_text(errors='replace')
    issues = []

    def require(value, message):
        if not value:
            issues.append(message)

    config = next((e for e in data if e['event'] == 'config'), {})
    conf = config.get('conf', {})
    expected_rows = meta['rows_per_partition'] * 16
    expected_serializer = ('com.nvidia.spark.ParquetCachedBatchSerializer'
                           if meta['serializer'] == 'java' else
                           'org.apache.spark.sql.pcbsrealdata.ResearchParquetCachedBatchSerializer')
    require(config.get('action') == 'gpu-controlled', 'wrong fixture action')
    require(config.get('master') == 'local[8]' and config.get('parts') == 16,
            'task/partition controls changed')
    require(config.get('spark_version') == '3.5.5', 'Spark version changed')
    require(config.get('rows') == expected_rows and
            config.get('logical_value_bytes') == expected_rows * 128, 'input volume changed')
    require(config.get('serializer_class') == 'org.apache.spark.serializer.JavaSerializer',
            'generic Java serializer changed')
    require(config.get('cache_serializer') == expected_serializer, 'cache serializer mismatch')
    require(conf.get('spark.memory.fraction', '0.6') == '0.6', 'memory fraction changed')
    require(conf.get('spark.serializer.objectStreamReset') == str(meta['reset']), 'reset changed')
    require(conf.get('spark.storage.unrollMemoryCheckPeriod') == str(meta['period']), 'unroll changed')
    jvm = config.get('jvm_arguments', [])
    for key, value in meta['expected_conf'].items():
        if key == 'spark.driver.extraJavaOptions':
            # Spark prepends its module-opening defaults to this property.
            require(all(token in jvm for token in shlex.split(value)),
                    'requested JVM option missing from effective runtime arguments')
        else:
            require(conf.get(key) == value, 'effective conf mismatch: ' + key)
    require(any(v.lower() in ('-xmx48g', '-xmx49152m') for v in jvm), 'heap maximum changed')
    require('-XX:+UseParallelGC' in jvm and '-XX:ParallelGCThreads=4' in jvm, 'GC controls changed')
    require(0 < config.get('heap_max', 0) <= 48 * 2**30, 'invalid usable heap maximum')
    origins = config.get('runtime_origins', {})
    overlay = meta['spark_overlay']['path']
    stock_core = '/home/roberte/src/spark_3.5.5/assembly/target/scala-2.12/jars/' + \
        'spark-core_2.12-3.5.5.jar'
    expected_origins = {
        'org.apache.spark.storage.BlockManager': stock_core,
        'org.apache.spark.storage.memory.MemoryStore': stock_core,
        'org.apache.spark.serializer.SerializerManager': overlay}
    require(set(origins) == set(expected_origins) and all(
        urlparse(origins.get(name, '')).path == path for name, path in expected_origins.items()),
        'stock BlockManager/MemoryStore or research SerializerManager origin mismatch')
    for key in ['manifest', 'artifacts']:
        require(digest(meta[key]) == meta[key + '_sha256'], key + ' changed')
    require(digest(meta['bench_jar']) == meta['bench_sha256'], 'harness jar changed')
    for entry in meta['gpu_jars'] + [meta['spark_overlay']]:
        require(digest(entry['path']) == entry['sha256'], 'runtime artifact changed: ' + entry['path'])
    manifest = json.loads(Path(meta['manifest']).read_text())
    require(manifest['oracle']['rows'] == expected_rows, 'canonical row total differs')
    files = [e for e in data if e['event'] == 'canonical_file']
    require([(e['path'], e['rows'], e['bytes'], e['schema']) for e in files] ==
            [(e['path'], e['rows'], e['bytes'], e['schema']) for e in manifest['files']],
            'canonical file/footer evidence differs')
    plan = next((e for e in data if e['event'] == 'cache_plan'), {})
    if plan:
        require(plan.get('serializer') == expected_serializer, 'cache builder serializer differs')
        require(plan.get('partitions') == 16 and plan.get('replication') == 1, 'cache layout changed')
        flags = [True, False, False, False] if meta['storage_level'] == 'DISK_ONLY' else [
            True, True, False, True]
        require(plan.get('storage_flags') == flags, 'cache storage flags changed')
        require('Gpu' in plan.get('plan', ''), 'cache producer is not GPU')
    populations = [e for e in data if e['event'] == 'partition']
    shape_valid = (len(populations) == 16 and
                   {e['partition'] for e in populations} == set(range(16)) and
                   all(e['rows'] == meta['rows_per_partition'] and e['records'] > 0 and
                       e['encoded_buffer_bytes'] > 0 for e in populations))
    phases = [e for e in data if e['event'] == 'phase']
    reads = [e for e in data if e['event'] == 'read']
    scans = [e for e in data if e['event'] == 'scan']
    storage = [e for e in data if e['event'] == 'storage']
    snapshots = [e for e in data if e['event'] == 'codec']
    targets = meta.get('consumer_targets') or [meta['consumer_mib']]
    count = len(targets) * 4
    signatures = [('fill', 0)] + [('read', i) for i in range(count)]
    complete = any(e.get('event') == 'complete' and e.get('success') for e in data)
    deltas = []
    for previous, current in zip(snapshots, snapshots[1:]):
        before, after = previous['snapshot'], current['snapshot']
        values = {k: after[k] - before.get(k, 0) for k in after
                  if k in before and 'max' not in k and 'min' not in k}
        # Extrema reset before each read: copy them, never subtract snapshots.
        values.update({k: v for k, v in after.items() if 'max' in k or 'min' in k})
        values.update(iteration=current['iteration'], phase=current['phase'],
                      consumer_target_mib=current.get('consumer_target_mib'),
                      gpu_decoded_max_bytes=after.get('gpu_decoded_max_bytes', 0),
                      gpu_combined_max_bytes=after.get('gpu_combined_max_bytes', 0),
                      gpu_combined_min_bytes=after.get('gpu_combined_min_bytes', 0))
        deltas.append(values)
    successful = complete and status['exit_code'] == 0 and not status['timeout']
    if successful:
        require(shape_valid, 'cache population incomplete')
        require([(p['phase'], p['iteration']) for p in phases] == signatures and
                all(p['success'] for p in phases), 'measured phases incomplete')
        require([r['iteration'] for r in reads] == list(range(count)), 'read sequence differs')
        require([s['iteration'] for s in scans] == list(range(count)) and
                all(s['rows'] == expected_rows and s['columns'] == 16 and
                    'GpuInMemoryTableScan' in s['class'] for s in scans),
                'full GPU cache scans missing')
        require([(s['phase'], s['iteration']) for s in storage] == signatures,
                'residency checkpoints incomplete')
        require(all(s['cached_partitions'] == 16 and s['memory_bytes'] + s['disk_bytes'] > 0
                    for s in storage), 'cache residency incomplete')
        if meta['storage_level'] == 'DISK_ONLY':
            require(all(s['memory_bytes'] == 0 and s['disk_bytes'] > 0 for s in storage),
                    'disk-only residency differs')
        oracle = manifest['oracle']['sums']
        for read in reads:
            require(read['rows'] == expected_rows and len(read['sums']) == 16,
                    'aggregate rows/columns differ')
            require(all(math.isfinite(a) and abs(a-b) <= max(1.0, abs(b))*1e-10
                        for a, b in zip(read['sums'], oracle)), 'canonical sums differ')
            require('Gpu' in read['plan'], 'read query GPU plan missing')
        require(any(e['event'] == 'unpersist' and e['success'] for e in data),
                'blocking unpersist cleanup missing')
        require([(s['phase'], s['iteration']) for s in snapshots] == signatures,
                'codec/batch checkpoints missing')
        require([d['iteration'] for d in deltas] == list(range(count)),
                'per-read counters missing')
        if snapshots:
            fill = snapshots[0]['snapshot']
            require(fill.get('gpu_produced_rows') == expected_rows,
                    'producer attempt rows differ from exact cache population')
            if not meta['smoke']:
                # Whole input batches are packed below the configured target; there is
                # no minimum fullness contract. Freeze the audited canonical producer
                # layout instead, so record splitting cannot silently change its input.
                expected_producer = {
                    'gpu_produced_batches': 112,
                    'gpu_produced_rows': 805306368,
                    'gpu_produced_bytes': 103079215104,
                    'gpu_produced_min_bytes': 806860032,
                    'gpu_produced_max_bytes': 939265152}
                require(all(fill.get(key) == value for key, value in expected_producer.items()),
                        'canonical producer layout changed across controlled cases')
                producer_goals = [line.strip() for line in plan.get('plan', '').splitlines()
                                  if 'GpuCoalesceBatches' in line]
                require(len(producer_goals) == 16 and all(
                    'targetsize(1073741824)' in line for line in producer_goals),
                    'producer plan lost a fixed 1 GiB coalescing target')
        for delta in deltas:
            require(delta.get('gpu_produced_rows') == 0, 'cache unexpectedly recomputed on read')
            require(delta.get('gpu_decoded_rows') == expected_rows,
                    'decoded attempt rows differ; investigate retries')
            require(all(v >= 0 for k, v in delta.items()
                        if k not in ('iteration', 'consumer_target_mib') and
                        isinstance(v, (int, float))), 'attempt counters decreased')
            target = targets[delta['iteration'] // 4]
            if meta.get('consumer_targets'):
                require(delta['consumer_target_mib'] == target, 'consumer target marker differs')
            require(delta.get('gpu_combined_rows') == (expected_rows if target else 0),
                    'post-cache coalescer rows differ')
            allowance = (meta['record_mib'] + max(2, meta['record_mib'] * 0.02)) * 2**20
            decoded_max = delta.get('gpu_decoded_max_bytes', 0)
            require(0 < decoded_max <= allowance, 'per-read decoded maximum exceeds record allowance')
            combined_max = delta.get('gpu_combined_max_bytes', 0)
            if target:
                expected_max = max(target * 2**20, decoded_max)
                require(0 < combined_max <= expected_max + max(2 * 2**20, expected_max * 0.02),
                        'per-read coalesced maximum exceeds target/input allowance')
                if not meta['smoke']:
                    require(combined_max >= expected_max * 0.85,
                            'per-read coalesced maximum not near target or larger input')
            else:
                require(combined_max == 0, 'combined output recorded with consumer coalescing off')
        if snapshots:
            last = snapshots[-1]['snapshot']
            if meta['serializer'] == 'codec':
                if not meta['smoke'] or meta['storage_level'] == 'DISK_ONLY':
                    require(last['codec_write_records'] > 0 and last['codec_read_records'] > 0,
                            'codec byte path not exercised')
            else:
                require(last['codec_write_records'] == 0 and last['codec_read_records'] == 0,
                        'Java control unexpectedly used custom codec')
            if not meta['smoke']:
                allowance = (meta['record_mib'] + max(2, meta['record_mib'] * 0.02)) * 2**20
                require(max(p['max_encoded_bytes'] for p in populations) <= allowance,
                        'encoded record exceeds private split budget allowance')
                require(0 < last['gpu_decoded_max_bytes'] <= allowance,
                        'decoded batch exceeds private split budget allowance')
        coalescers = [e for e in data if e['event'] == 'coalescer' and
                      e['class'].endswith('.GpuCoalesceBatches') and
                      any('GpuInMemoryTableScan' in c for c in e['children'])]
        expected_coalesced = [i for i in range(count) if targets[i // 4]]
        require([e['iteration'] for e in coalescers] == expected_coalesced,
                'standard coalescer placement differs')
    event_log = []
    for path in sorted((case / 'events').rglob('*')):
        if path.is_file() and not path.name.startswith('.'):
            event_log.extend(json.loads(line) for line in path.read_text().splitlines())
    task_ends = [e for e in event_log if e['Event'] == 'SparkListenerTaskEnd']
    task_failures = [e for e in task_ends
                     if e.get('Task End Reason', {}).get('Reason') != 'Success']
    if successful:
        require(not task_failures, 'task failures/retries occurred')
        require(sum(e['Event'] == 'SparkListenerApplicationEnd' for e in event_log) == 1,
                'application end missing')
    collections = gc_records(sorted(case.glob('gc.log*')))
    phase_heap = []
    for phase in phases:
        recorded = [e['before_bytes_rounded'] for e in collections
                    if phase['start_epoch_ms'] <= e['end_epoch_ms'] <= phase['end_epoch_ms']]
        sampled = phase.get('sampled_heap_peak', 0)
        phase_heap.append({'phase': phase['phase'], 'iteration': phase['iteration'],
                           'sampled_peak_bytes': sampled,
                           'logged_pre_gc_peak_bytes': max(recorded or [0]),
                           'observed_peak_bytes': max([sampled] + recorded)})
    measured = [p for p in phases if p['phase'] == 'read' and p['iteration'] % 4 > 0 and
                p['success']]
    by_target = []
    for group, target in enumerate(targets):
        times = [p['seconds'] for p in measured if p['iteration'] // 4 == group]
        by_target.append({'consumer_target_mib': target, 'measured_read_seconds': times,
                          'median_read_seconds': statistics.median(times)
                          if successful and len(times) == 3 else None})
    oom = 'java.lang.OutOfMemoryError: Java heap space' in raw
    if successful and not issues:
        outcome = 'SMOKE_PASS' if meta['smoke'] else 'PASS'
    elif status['timeout']:
        outcome = 'TIMEOUT'
    elif oom and status['exit_code'] != 0 and not issues:
        outcome = 'HEAP_OOM'
    else:
        outcome = 'ERROR'
    starts = [e for e in data if e['event'] == 'phase_start']
    return {
        'action': 'gpu-controlled', 'outcome': outcome, 'issues': issues, 'process': status,
        'serializer': meta['serializer'], 'storage_level': meta['storage_level'],
        'reset': meta['reset'], 'period': meta['period'], 'record_mib': meta['record_mib'],
        'consumer_targets': targets, 'complete': complete, 'shape_valid': shape_valid,
        'failure_phase': starts[-1]['phase'] if starts and not complete else None,
        'rows': expected_rows, 'records': sum(p['records'] for p in populations),
        'partitions': populations,
        'gpu_sampler': json.loads((case / 'gpu-sampler-result.json').read_text())
        if (case / 'gpu-sampler-result.json').is_file() else None,
        'encoded_buffer_bytes': sum(p['encoded_buffer_bytes'] for p in populations),
        'max_encoded_record_bytes': max([p['max_encoded_bytes'] for p in populations] or [0]),
        'fill_seconds': next((p['seconds'] for p in phases
                              if p['phase'] == 'fill' and p['success']), None),
        'read_groups': by_target, 'phases': phases, 'storage': storage,
        'phase_heap': phase_heap,
        'observed_peak_heap_bytes': max([p['observed_peak_bytes'] for p in phase_heap] or [0]),
        'codec_snapshots': snapshots, 'gpu_read_deltas': deltas,
        'process_sampling': json.loads((case / 'process-sampler-result.json').read_text())
        if (case / 'process-sampler-result.json').exists() else {'verified_samples_available': False},
        'gpu_sampling': json.loads((case / 'gpu-sampler-result.json').read_text())
        if (case / 'gpu-sampler-result.json').exists() else {'error': 'sampler status missing'},
        'manifest_sha256': meta['manifest_sha256'],
        'occupancy_note': 'Sampled maximum includes uncollected garbage; not post-GC live heap.',
        'counter_note': 'Attempt totals must equal exact read rows; retries are not silently accepted.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.case), indent=2))
