# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Read-only raw-log verification; negative outcomes remain explicit results."""
import argparse
import json
import math
import statistics
from pathlib import Path

from canonical_source import digest, events


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
    action = meta['action']
    require(config.get('action') == action, 'action/config mismatch')
    require(config.get('master') == 'local[8]', 'master mismatch')
    require(config.get('parts') == 16, 'partition configuration mismatch')
    require(config.get('serializer_class') == 'org.apache.spark.serializer.JavaSerializer',
            'default JavaSerializer missing')
    expected_rows = meta['rows_per_partition'] * 16
    require(config.get('rows') == expected_rows, 'configured row count mismatch')
    require(config.get('logical_value_bytes') == expected_rows * 128, 'logical bytes mismatch')
    require(config.get('spark_version') == '3.5.5', 'runtime version mismatch')
    jvm = config.get('jvm_arguments', [])
    require(any(value.lower() in ('-xmx48g', '-xmx49152m') for value in jvm),
            'configured maximum heap mismatch')
    require('-XX:+UseParallelGC' in jvm and '-XX:ParallelGCThreads=4' in jvm,
            'GC controls mismatch')
    require(0 < config.get('heap_max', 0) <= 48 * 2**30,
            'invalid usable heap maximum; ParallelGC survivor space is excluded')
    conf = config.get('conf', {})
    plan = next((e for e in data if e['event'] == 'cache_plan'), {})
    if plan:
        require(plan.get('storage_flags') ==
                ([True, False, False, False] if meta.get('disk_roundtrip') else
                 [True, True, False, True]) and
                plan.get('replication') == 1, 'MEMORY_AND_DISK storage mismatch')
    if action.startswith('gpu'):
        for key, value in {
            'spark.rapids.sql.batchSizeBytes': '1073741824',
            'spark.rapids.sql.concurrentGpuTasks': '2',
            'spark.rapids.sql.maxConcurrentGpuTasks': '2',
            'spark.rapids.memory.gpu.allocSize': '15g',
            'spark.rapids.memory.gpu.maxAllocSize': '15g',
            'spark.rapids.memory.host.offHeapLimit.enabled': 'true',
            'spark.rapids.memory.host.offHeapLimit.size': '40g',
            'spark.rapids.memory.pinnedPool.size': '2g'}.items():
            require(conf.get(key) == value, 'GPU effective configuration mismatch: ' + key)
    for key, default in [('spark.memory.fraction', '0.6'),
                         ('spark.storage.unrollMemoryCheckPeriod', '16'),
                         ('spark.serializer.objectStreamReset', '100')]:
        require(conf.get(key, default) == default, 'nondefault ' + key)
    if action == 'cpu':
        require(config.get('cache_serializer') ==
                'org.apache.spark.sql.execution.columnar.DefaultCachedBatchSerializer',
                'CPU cache serializer mismatch')
        require(config.get('cpu_batch_rows') == '10000', 'CPU batch size not default')
        require(config.get('cpu_compression') == 'true', 'CPU compression not default')
        require(not conf.get('spark.plugins'), 'CPU plugin configured')
        origins = config.get('runtime_origins', {})
        require(len(origins) == 3 and all('/spark_3.5.5/' in value and
                'spark-core_2.12-3.5.5.jar' in value for value in origins.values()),
                'CPU core classes did not come from stock Spark runtime')
        require(not meta['gpu_jars'] and not meta['spark_overlay'], 'CPU nonstock jars')
        require(not any('rapids' in k.lower() for k in conf), 'CPU RAPIDS configuration')
    population = [e for e in data if e['event'] == 'partition']
    full_shape = len(population) == 16 and {e['partition'] for e in population} == set(range(16))
    full_shape = full_shape and all(e['rows'] == meta['rows_per_partition'] and
                                  e['records'] > 0 and e['encoded_buffer_bytes'] > 0
                                  for e in population)
    reads = [e for e in data if e['event'] == 'read']
    phases = [e for e in data if e['event'] == 'phase']
    scans = [e for e in data if e['event'] == 'scan']
    storage = [e for e in data if e['event'] == 'storage']
    snapshots = [e for e in data if e['event'] == 'codec']
    deltas = []
    for previous, current in zip(snapshots, snapshots[1:]):
        before, after = previous['snapshot'], current['snapshot']
        values = {key: after[key]-before.get(key, 0) for key in after
                  if key in before and not ('max' in key or 'min' in key)}
        values.update(iteration=current['iteration'], phase=current['phase'])
        for prefix in ['gpu_decoded', 'gpu_combined']:
            batches = values.get(prefix + '_batches', 0)
            values[prefix + '_mean_bytes'] = (values.get(prefix + '_bytes', 0) / batches
                                             if batches else None)
        deltas.append(values)
    complete = any(e.get('event') == 'complete' and e.get('success') for e in data)
    starts = [e for e in data if e['event'] == 'phase_start']
    failure_phase = starts[-1]['phase'] if not complete and starts else None
    oom = 'java.lang.OutOfMemoryError: Java heap space' in raw
    manifest = json.loads(Path(meta['manifest']).read_text())
    require(digest(meta['manifest']) == meta['manifest_sha256'], 'manifest changed')
    require(config.get('rows') == manifest['oracle']['rows'], 'canonical rows mismatch')
    files = [e for e in data if e['event'] == 'canonical_file']
    require([(e['path'], e['rows'], e['bytes'], e['schema']) for e in files] ==
            [(e['path'], e['rows'], e['bytes'], e['schema']) for e in manifest['files']],
            'source file/footer mismatch')
    if complete and status['exit_code'] == 0 and not status['timeout']:
        require(full_shape, 'population shape invalid')
        require([(e['phase'], e['iteration']) for e in phases] ==
                [('fill', 0)] + [('read', i) for i in range(4)], 'timing phases invalid')
        require(all(e['success'] for e in phases), 'failed measured phase')
        require([e['iteration'] for e in reads] == list(range(4)), 'read IDs invalid')
        require([e['iteration'] for e in scans] == list(range(4)) and
                all(e['rows'] == expected_rows and e['columns'] == 16
                                      for e in scans), 'full cache scan evidence invalid')
        require([(e['phase'], e['iteration']) for e in storage] ==
                [('fill', 0)] + [('read', i) for i in range(4)], 'storage checkpoints invalid')
        require(all(e['cached_partitions'] == 16 and
                    e['memory_bytes'] + e['disk_bytes'] > 0 for e in storage),
                'retained cache incomplete')
        oracle = manifest['oracle']['sums']
        for read in reads:
            require(read['rows'] == expected_rows and len(read['sums']) == 16,
                    'SQL count/width invalid')
            require(all(math.isfinite(a) and abs(a-b) <= max(1.0, abs(b))*1e-10
                        for a, b in zip(read['sums'], oracle)), 'canonical aggregate mismatch')
        require(any(e['event'] == 'unpersist' and e['success'] for e in data), 'cleanup missing')
        if action.startswith('gpu'):
            require([(e['phase'], e['iteration']) for e in snapshots] ==
                    [('fill', 0)] + [('read', i) for i in range(4)], 'codec checkpoints missing')
            if snapshots:
                require(snapshots[-1]['snapshot']['gpu_decoded_rows'] >= expected_rows * 4,
                        'GPU decoding did not consume full reads')
                require((meta.get('smoke') and not meta.get('disk_roundtrip')) or
                        (snapshots[-1]['snapshot']['codec_write_records'] > 0 and
                         snapshots[-1]['snapshot']['codec_read_records'] > 0),
                        'research byte codec not exercised; a memory-only hit is insufficient')
                last = snapshots[-1]['snapshot']
                require([d['iteration'] for d in deltas] == list(range(4)),
                        'GPU per-read snapshot deltas missing')
                require(all(d.get('gpu_decoded_rows') == expected_rows for d in deltas),
                        'GPU decode attempt rows differ from exact completed read; inspect retries')
                require(all(all(v >= 0 for k, v in d.items() if isinstance(v, (int, float)))
                            for d in deltas), 'GPU attempt counters decreased')
                if not meta.get('smoke'):
                    require(max(e['max_encoded_bytes'] for e in population) <= 34 * 2**20,
                            'actual encoded record exceeds fixed-fixture 34MiB allowance')
                    require(0 < last['gpu_decoded_max_bytes'] <= 34 * 2**20,
                            'actual decoded record exceeds fixed-fixture 34MiB allowance')
                if action == 'gpu32combine':
                    require(all(d.get('gpu_combined_rows') == expected_rows for d in deltas),
                            'post-cache coalescer rows differ from exact completed read')
                    if not meta.get('smoke'):
                        require(900 * 2**20 <= last['gpu_combined_max_bytes'] <=
                                2**30 + 34 * 2**20,
                                'actual combined output not near1GiB within fixture allowance')
                    require(last['gpu_combined_batches'] > 0 and
                            (meta.get('smoke') or
                             last['gpu_combined_batches'] < last['gpu_decoded_batches']),
                            'standard coalescer did not reduce decoded batch count')
                    coalescers = [e for e in data if e['event'] == 'coalescer' and
                                  e['class'].endswith('.GpuCoalesceBatches') and
                                  any('GpuInMemoryTableScan' in child for child in e['children'])]
                    require([e['iteration'] for e in coalescers] == list(range(4)),
                            'standard coalescer is not immediately above cache scan')
                else:
                    require(last['gpu_combined_rows'] == 0,
                            'unexpected immediate post-cache coalescer in split-only case')
    outcome = 'PASS' if complete and status['exit_code'] == 0 and not status['timeout'] and not issues else (
        'HEAP_OOM' if oom and status['exit_code'] != 0 and not status['timeout'] else 'ERROR')
    if outcome == 'PASS' and meta.get('smoke'):
        outcome = 'SMOKE_PASS'
    timed = [e for e in phases if e['phase'] == 'read' and e['iteration'] > 0 and e['success']]
    return {'action': action, 'outcome': outcome, 'issues': issues, 'process': status,
            'failure_phase': failure_phase, 'shape_valid': full_shape,
            'complete': complete, 'rows': expected_rows,
            'encoded_buffer_bytes': sum(e['encoded_buffer_bytes'] for e in population),
            'records': sum(e['records'] for e in population),
            'fill_seconds': next((e['seconds'] for e in phases if e['phase'] == 'fill'), None),
            'measured_read_seconds': [e['seconds'] for e in timed],
            'median_read_seconds': statistics.median([e['seconds'] for e in timed])
            if outcome == 'PASS' and len(timed) == 3 else None,
            'phases': phases, 'storage': storage, 'codec_snapshots': snapshots,
            'gpu_read_deltas': deltas,
            'max_encoded_record_bytes': max([e['max_encoded_bytes'] for e in population] or [0]),
            'gpu_max_decoded_bytes': snapshots[-1]['snapshot'].get('gpu_decoded_max_bytes')
            if snapshots else None,
            'gpu_max_combined_bytes': snapshots[-1]['snapshot'].get('gpu_combined_max_bytes')
            if snapshots else None,
            'counter_note': 'Attempt totals; exact read deltas required; extra retry work is reported as a mismatch, not silently accepted.',
            'occupancy_note': 'sampled heap occupancy is not post-GC live heap',
            'manifest_sha256': meta['manifest_sha256']}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('case')
    args = parser.parse_args()
    print(json.dumps(verify(args.case), indent=2))
