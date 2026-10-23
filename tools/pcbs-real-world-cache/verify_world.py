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
"""Reparse one immutable world-cache case; storage, heap and IO have separate meanings."""
import argparse
import csv
import json
import math
from pathlib import Path
import shlex
import statistics
import sys
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
REAL = HERE.parent / 'pcbs-real-data-comparison'
sys.path.insert(0, str(REAL))
from canonical_source import digest
from audit_results import gc_records

STOCK_CORE = '/home/roberte/src/spark_3.5.5/assembly/target/scala-2.12/jars/spark-core_2.12-3.5.5.jar'
PREFIX = 'WORLD_CACHE '
IO_KEYS = ('read_bytes', 'write_bytes', 'rchar', 'wchar')


def markers(path):
    return [json.loads(line[len(PREFIX):]) for line in Path(path).read_text().splitlines()
            if line.startswith(PREFIX)]


def memory_io(case, phases):
    metadata = case / 'process-sampler-result.json'
    sample_meta = json.loads(metadata.read_text()) if metadata.is_file() else {}
    identities = {(x['pid'], x['start_time_ticks'])
                  for x in sample_meta.get('selected_identities', [])}
    values, rejected = [], 0
    path = case / 'process-memory-io.csv'
    if identities and path.is_file():
        with path.open(newline='') as stream:
            for row in csv.DictReader(stream):
                try:
                    identity = (int(row['pid']), int(row['start_time_ticks']))
                    epoch = float(row['epoch_s']) * 1000
                    rss, hwm = int(row['VmRSS_KiB']) * 1024, int(row['VmHWM_KiB']) * 1024
                    io = {key: int(row[key]) for key in IO_KEYS}
                    assert not row.get('error') and identity in identities
                    assert math.isfinite(epoch) and min([rss, hwm] + list(io.values())) >= 0
                    values.append(dict(epoch_ms=epoch, rss=rss, hwm=hwm,
                                       identity=identity, io=io))
                except (ValueError, KeyError, TypeError, AssertionError):
                    rejected += 1
    values.sort(key=lambda x: x['epoch_ms'])
    collections = gc_records(sorted(case.glob('gc.log*')))
    observations = []
    for phase in phases:
        start, end = phase['start_epoch_ms'], phase['end_epoch_ms']
        selected = [v for v in values if start <= v['epoch_ms'] < end]
        before_gc = [r['before_bytes_rounded'] for r in collections
                     if start <= r['end_epoch_ms'] <= end]
        sampled = phase.get('sampled_heap_peak')
        heap = max(([sampled] if sampled is not None else []) + before_gc, default=None)
        # Only wholly in-phase sample intervals are attributed. Boundary gaps remain unmeasured.
        io_delta = None
        if len(selected) >= 2 and len({v['identity'] for v in selected}) == 1:
            first, last = selected[0], selected[-1]
            delta = {key: last['io'][key] - first['io'][key] for key in IO_KEYS}
            if all(value >= 0 for value in delta.values()):
                io_delta = dict(delta, sample_start_epoch_ms=first['epoch_ms'],
                                sample_end_epoch_ms=last['epoch_ms'])
        observations.append(dict(
            phase=phase['phase'], iteration=phase['iteration'],
            sampled_heap_peak_bytes=sampled, observed_heap_peak_bytes=heap,
            sampled_rss_peak_bytes=max((v['rss'] for v in selected), default=None),
            sampled_io_delta=io_delta,
            gc_ms=phase.get('gc_ms'), gc_count=phase.get('gc_count')))
    return observations, dict(valid_samples=len(values), rejected_samples=rejected,
                              selected_identities=sample_meta.get('selected_identities', []),
                              sampling_error_samples=sample_meta.get('error_samples'),
                              process_observed_hwm_bytes=max((v['hwm'] for v in values), default=None))


def task_results(case):
    result = []
    files = [p for p in (case / 'events').rglob('*') if p.is_file() and not p.name.startswith('.')]
    for path in sorted(files):
        with path.open() as stream:
            for line in stream:
                event = json.loads(line)
                if event.get('Event') == 'SparkListenerTaskEnd':
                    result.append(event)
    return result


def verify(case):
    case = Path(case)
    meta = json.loads((case / 'command.json').read_text())
    process = json.loads((case / 'process.json').read_text())
    manifest = json.loads(Path(meta['manifest']).read_text())
    oracle = json.loads(Path(meta['oracle']).read_text())
    data = markers(case / 'run.log')
    raw = (case / 'run.log').read_text(errors='replace')
    issues = []

    def require(condition, message):
        if not condition:
            issues.append(message)

    config_events = [x for x in data if x['event'] == 'config']
    require(len(config_events) == 1, 'one runtime config required')
    config = config_events[0] if config_events else {}
    expected_rows = manifest['total_rows']
    mode = meta['mode']
    cpu = mode == 'cpu-default'
    codec = mode == 'rg-codec'
    rowgroup = mode.startswith('rg-')
    heap_bytes = meta['heap_gib'] * 2**30
    signature = [('fill', 0)] + [('read', i) for i in range(4)]
    conf = config.get('conf', {})
    jvm = config.get('jvm_arguments', [])
    require(config.get('mode') == mode, 'harness mode differs')
    require(config.get('rows') == expected_rows == oracle['rows'], 'row count control differs')
    require(config.get('parts') == 16 and config.get('master') == 'local[8]', 'layout/slots differ')
    require(config.get('spark_version') == '3.5.5', 'Spark version differs')
    require(config.get('schema_json') == oracle['schema_json'], 'schema differs from oracle')
    require(config.get('parquet_infer_timestamp_ntz') == 'false' and
            config.get('session_timezone') == 'UTC', 'timestamp inference/timezone differs')
    require(config.get('raw_value_bytes') == oracle['raw_value_bytes'] > 0,
            'semantic-byte denominator differs')
    require(config.get('serializer_class') == 'org.apache.spark.serializer.JavaSerializer',
            'generic serializer differs')
    require(config.get('cache_serializer') == meta['expected_conf']['spark.sql.cache.serializer'],
            'cache serializer differs')
    require(any(arg.lower() in ('-xmx{}g'.format(meta['heap_gib']),
                                '-xmx{}m'.format(meta['heap_gib'] * 1024)) for arg in jvm),
            'configured maximum heap differs')
    require(0 < config.get('heap_max', 0) <= heap_bytes, 'actual maximum heap differs')
    for key, expected in meta['expected_conf'].items():
        if key == 'spark.driver.extraJavaOptions':
            require(all(value in jvm for value in shlex.split(expected)), 'JVM option missing')
        else:
            require(conf.get(key) == expected, 'runtime control differs: ' + key)
    require(conf.get('spark.memory.fraction', '0.6') == '0.6', 'memory fraction changed')
    if cpu:
        require(not conf.get('spark.plugins'), 'CPU reference loaded a plugin')
        require(not any(key.startswith('spark.rapids.') for key in conf), 'CPU RAPIDS config leaked')
    origins = {key: urlparse(value).path for key, value in config.get('runtime_origins', {}).items()}
    expected_origins = {name: STOCK_CORE for name in (
        'org.apache.spark.storage.BlockManager', 'org.apache.spark.storage.memory.MemoryStore',
        'org.apache.spark.serializer.SerializerManager')}
    if codec:
        expected_origins['org.apache.spark.serializer.SerializerManager'] = meta['roles']['codec_overlay']
    require(origins == expected_origins, 'Spark class origins differ')
    for path, expected in meta['frozen_hashes'].items():
        require(digest(path) == expected, 'frozen input/artifact changed: ' + path)

    require(config.get('input_manifest_path') == meta['manifest'] and
            config.get('input_manifest_sha256') == digest(meta['manifest']),
            'runtime source manifest differs')
    require(config.get('oracle_path') == meta['oracle'], 'runtime oracle path differs')
    require(str(config.get('cpu_batch_rows')) == '10000', 'default CPU cache batch rows changed')
    footers = [x for x in data if x['event'] == 'canonical_file']
    require([(x.get('partition'), x.get('path'), x.get('rows'), x.get('bytes'),
              x.get('row_groups')) for x in footers] ==
            [(x['partition'], x['path'], x['rows'], x['bytes'], x['row_groups'])
             for x in manifest['files']], 'runtime input footer layout differs')

    plans = [x for x in data if x['event'] == 'cache_plan']
    require(len(plans) == 1, 'one cache plan required')
    plan = plans[0] if plans else {}
    flags = [True, True, False, True] if meta['storage'] == 'MEMORY_AND_DISK' else [
        True, False, False, False]
    require(plan.get('serializer') == meta['expected_conf']['spark.sql.cache.serializer'],
            'cache builder serializer differs')
    require(plan.get('storage_flags') == flags and plan.get('partitions') == 16 and
            plan.get('replication') == 1, 'persistence controls differ')
    serializer_origin = urlparse(plan.get('serializer_origin', '')).path
    expected_serializer_origin = (
        '/home/roberte/src/spark_3.5.5/assembly/target/scala-2.12/jars/spark-sql_2.12-3.5.5.jar'
        if cpu else meta['roles']['codec_fixture'] if codec else meta['roles']['plugin'])
    require(serializer_origin == expected_serializer_origin, 'cache serializer class origin differs')
    populations = [x for x in data if x['event'] == 'partition']
    phases = [x for x in data if x['event'] == 'phase']
    starts = [x for x in data if x['event'] == 'phase_start']
    reads = [x for x in data if x['event'] == 'read']
    scans = [x for x in data if x['event'] == 'scan']
    gpu_paths = [x for x in data if x['event'] == 'gpu_path']
    storage = [x for x in data if x['event'] == 'storage']
    snapshots = [x for x in data if x['event'] == 'codec']
    complete = any(x['event'] == 'complete' and x.get('success') for x in data)
    successful = complete and process['exit_code'] == 0 and not process['timeout']
    # Even failed runs must never turn an observed wrong answer into an expected OOM.
    for read in reads:
        require(all(read.get(key) == oracle[key] for key in
                    ('rows', 'hash_sums', 'null_counts', 'value_bytes', 'raw_value_bytes', 'schema_json')),
                'all-column content/profile oracle mismatch')
    for phase in phases:
        require(phase['end_epoch_ms'] >= phase['start_epoch_ms'] and phase['seconds'] >= 0,
                'invalid phase timing')
        require(0 <= phase.get('sampled_heap_peak', -1) <= heap_bytes, 'invalid sampled heap')
    part_rows = {x['partition']: x['rows'] for x in manifest['files']}
    shape = (len(populations) == 16 and
             {x['partition'] for x in populations} == set(range(16)) and
             all(x['rows'] == part_rows[x['partition']] and x['records'] > 0 and
                 x['encoded_buffer_bytes'] > 0 for x in populations))
    deltas = []
    for before, after in zip(snapshots, snapshots[1:]):
        a, b = before['snapshot'], after['snapshot']
        delta = {key: value - a.get(key, 0) for key, value in b.items()
                 if isinstance(value, (int, float)) and 'max' not in key and 'min' not in key}
        delta.update({key: value for key, value in b.items() if 'max' in key or 'min' in key})
        deltas.append(dict(delta, iteration=after['iteration']))

    if successful:
        require(shape, 'cache population incomplete')
        require(all(sum(x['record_histogram_counts']) == x['records'] and
                    0 < x['min_encoded_bytes'] <= x['max_encoded_bytes']
                    for x in populations), 'cache record distribution inconsistent')
        if cpu:
            require(all(x['class'] == 'org.apache.spark.sql.execution.columnar.DefaultCachedBatch'
                        and x['max_batch_rows'] <= 10000 for x in populations),
                    'CPU default cache format/batching differs')

        require([(x['phase'], x['iteration']) for x in phases] == signature and
                all(x['success'] for x in phases), 'phase sequence incomplete')
        require([(x['phase'], x['iteration']) for x in starts] == signature,
                'phase starts incomplete')
        require([x['iteration'] for x in reads] == list(range(4)), 'read sequence incomplete')
        require([x['iteration'] for x in scans] == list(range(4)), 'scan sequence incomplete')
        if cpu:
            require(not gpu_paths, 'CPU reference emitted GPU-path markers')
        else:
            require([x['iteration'] for x in gpu_paths] == list(range(4)),
                    'four full-row GPU path checks required')
            for marker in gpu_paths:
                iteration = marker['iteration']
                operators = [x for x in data if x['event'] == 'operator' and
                             x.get('cached') and x['iteration'] == iteration]
                nodes = {node['node_id']: node for node in operators}
                require(len(nodes) == len(operators) and bool(nodes),
                        'GPU plan operator graph missing/duplicate IDs')
                children = {child for node in operators for child in node['children']}
                roots = [node for node in operators if node['node_id'] not in children]
                plan_scans = [node for node in operators
                              if 'InMemoryTableScan' in node['class']]
                require(len(roots) == 1 and len(plan_scans) == 1 and
                        children <= set(nodes), 'GPU plan graph incomplete')
                aggregate_classes = [node['class'] for node in operators
                                     if node['class'].rsplit('.', 1)[-1].endswith('AggregateExec')]
                path = None
                def to_scan(node, ancestors):
                    if node['node_id'] in ancestors:
                        return None
                    if node in plan_scans:
                        return [node]
                    for child in node['children']:
                        if child in nodes:
                            tail = to_scan(nodes[child], ancestors | {node['node_id']})
                            if tail is not None:
                                return [node] + tail
                    return None
                if len(roots) == 1 and len(plan_scans) == 1:
                    path = to_scan(roots[0], set())
                full_row_classes = []
                if path:
                    reductions = [i for i, node in enumerate(path)
                                  if node['class'].rsplit('.', 1)[-1].endswith('AggregateExec')]
                    start = reductions[-1] + 1 if reductions else 0
                    full_row_classes = [node['class'] for node in path[start:]]
                expected_aggregate = marker.get('aggregate_classes', [])
                expected_full = marker.get('full_row_classes', [])
                require(marker.get('success') is True and bool(aggregate_classes) and
                        bool(full_row_classes) and
                        aggregate_classes == expected_aggregate and
                        full_row_classes == expected_full and all(
                            name.rsplit('.', 1)[-1].startswith('Gpu')
                            for name in aggregate_classes + full_row_classes),
                        'full-row/aggregate GPU path differs from recorded operator plan')
        for scan in scans:
            require(scan.get('rows') == expected_rows and scan.get('columns') == config.get('columns'),
                    'full all-column cache scan missing')
            require(('GpuInMemoryTableScan' not in scan.get('class', '') if cpu else
                     'GpuInMemoryTableScan' in scan.get('class', '')),
                    'unexpected CPU/GPU cache scan path')
        require([(x['phase'], x['iteration']) for x in storage] == signature,
                'storage checkpoints incomplete')
        for checkpoint in storage:
            blocks = checkpoint.get('local_blocks', [])
            require(len(blocks) == 16 and {b['partition'] for b in blocks} == set(range(16)),
                    'local block status layout missing')
            require(all(b['present'] and b['memory_bytes'] + b['disk_bytes'] > 0 for b in blocks),
                    'cache block absent after successful phase')
            require(checkpoint.get('memory_bytes') == sum(b['memory_bytes'] for b in blocks) and
                    checkpoint.get('disk_bytes') == sum(b['disk_bytes'] for b in blocks),
                    'local block totals differ')
            require(checkpoint.get('cached_partitions') == 16, 'cached partition count differs')
            require(all(b['block_id'] == 'rdd_{}_{}'.format(plan.get('cache_rdd_id'), b['partition'])
                        for b in blocks), 'block identifier differs')
            require(checkpoint.get('disk_file_bytes') ==
                    sum(b['disk_file_bytes'] for b in blocks), 'physical disk totals differ')
            require(all(b['disk_bytes'] == b['disk_file_bytes'] for b in blocks),
                    'unencrypted stream count differs from actual cache file length')

            require(all((b['disk_file_bytes'] > 0 if b['disk_file_exists'] else
                         b['disk_file_bytes'] == 0) for b in blocks), 'physical disk evidence inconsistent')
            if meta['storage'] == 'DISK_ONLY':
                require(all(b['memory_bytes'] == 0 and b['disk_file_exists'] for b in blocks),
                        'DISK_ONLY residency differs')
        require(any(x['event'] == 'unpersist' and x.get('success') for x in data),
                'blocking cleanup missing')
        if cpu:
            require(not snapshots, 'CPU reference loaded GPU observer')
        else:
            require([(x['phase'], x['iteration']) for x in snapshots] == signature,
                    'instrumentation checkpoints missing')
            require(all(urlparse(x.get('observer_origin', '')).path == meta['roles']['codec_fixture']
                        for x in snapshots), 'observer artifact differs')
            if snapshots:
                fill = snapshots[0]['snapshot']
                require(fill.get('gpu_produced_rows') == expected_rows,
                        'GPU producer missing; possible CPU PCBS fallback')
                require(fill.get('gpu_produced_batches', 0) > 0 and
                        fill.get('gpu_produced_bytes', 0) > 0, 'GPU producer layout missing')
                if rowgroup:
                    require(fill.get('rg_encode_successes') == fill.get('gpu_produced_batches'),
                            'input pre-split/repeated successful encoding')
                    require(fill.get('rg_encoded_rows') == fill.get('rg_stored_rows') == expected_rows,
                            'row-group stored rows differ')
                    require(fill.get('rg_stored_records') == sum(x['records'] for x in populations) and
                            fill.get('rg_stored_bytes') == sum(x['encoded_buffer_bytes'] for x in populations),
                            'row-group storage counters differ')
                    require(fill.get('rg_encode_attempts', 0) >= fill.get('rg_encode_successes', 0),
                            'encode retry counters inconsistent')
                    require(0 < fill.get('rg_stored_max_bytes', 0) <=
                            meta['artifact_spec']['rowgroup_spec']['record_safety_limit_bytes'],
                            'coarse encoded-record safety guard exceeded (not an 8MiB assertion)')
                    require(fill.get('rg_stored_max_bytes') == max(x['max_encoded_bytes']
                                                                  for x in populations),
                            'row-group maximum differs')
                if codec:
                    written = fill.get('codec_write_records', -1)
                    read_back = fill.get('codec_read_records', -1)
                    written_bytes = fill.get('codec_write_bytes', -1)
                    read_back_bytes = fill.get('codec_read_bytes', -1)
                    if meta['storage'] == 'DISK_ONLY':
                        # A successful put is read back by getOrElseUpdate/getLocalValues.
                        # Codec counters count payload bytes, excluding the wire header.
                        records = sum(x['records'] for x in populations)
                        payload = sum(x['encoded_buffer_bytes'] for x in populations)
                        require(written == read_back == records and
                                written_bytes == read_back_bytes == payload,
                                'DISK_ONLY codec fill write/readback differs from stored payload')
                    else:
                        require(0 <= read_back <= written and
                                0 <= read_back_bytes <= written_bytes,
                                'MEMORY_AND_DISK codec fill readback exceeds writes')
                else:
                    require(fill.get('codec_read_records', 0) == 0 and
                            fill.get('codec_read_bytes', 0) == 0,
                            'Java fill unexpectedly used custom codec readback')
            require(len(deltas) == 4, 'per-read counters missing')
            for delta in deltas:
                require(delta.get('gpu_produced_rows') == 0, 'cache unexpectedly recomputed')
                require(delta.get('gpu_decoded_rows') == expected_rows, 'GPU decoded rows differ')
                if rowgroup:
                    require(delta.get('rg_decode_rows') == expected_rows,
                            'native multi-source decoded rows differ')
                    require(0 < delta.get('rg_decode_successes', 0) <= delta.get('rg_decode_files', 0),
                            'native multi-source evidence missing')
                    require(delta.get('rg_decode_attempts', 0) >= delta.get('rg_decode_successes', 0),
                            'decode retry counters inconsistent')
                    require(0 < delta.get('rg_bundle_max_encoded_bytes', 0) <= 2**30 and
                            0 < delta.get('rg_bundle_max_estimated_decoded_bytes', 0) <= 2**30,
                            'encoded/estimated bundle admission guard exceeded')
                    require(delta.get('rg_decode_device_bytes') == delta.get('gpu_decoded_bytes'),
                            'decoded device byte counters differ')
                if not codec:
                    require(delta.get('codec_read_records', 0) == 0, 'Java arm used custom codec')
            if not codec:
                require(all(x['snapshot'].get('codec_write_records', 0) == 0 for x in snapshots),
                        'Java arm wrote custom codec')
            if codec and meta['storage'] == 'DISK_ONLY' and snapshots:
                require(snapshots[-1]['snapshot'].get('codec_write_records', 0) > 0 and
                        snapshots[-1]['snapshot'].get('codec_read_records', 0) > 0,
                        'DISK_ONLY custom codec byte path inactive')

    tasks = task_results(case)
    if successful:
        require(bool(tasks), 'task event evidence missing')
        require(all(x.get('Task End Reason', {}).get('Reason') == 'Success' for x in tasks),
                'task failure/retry occurred')
    phase_observations, sampling = memory_io(case, phases)
    if successful:
        require(sampling['valid_samples'] > 0, 'owned JVM RSS/IO sample evidence missing')
    native_failure = any(text in raw for text in (
        '# A fatal error has been detected', 'SIGSEGV', 'CUDA_ERROR_ILLEGAL_ADDRESS',
        'Fatal CUDA error', 'OutOfMemoryError: Direct buffer memory'))
    heap_oom = 'java.lang.OutOfMemoryError: Java heap space' in raw
    gc_oom = 'java.lang.OutOfMemoryError: GC overhead limit exceeded' in raw
    measured_failure = (not successful and process['exit_code'] != 0 and not process['timeout']
                        and not native_failure and not issues)
    outcome = ('PASS' if successful and not issues else
               'HEAP_OOM' if measured_failure and heap_oom else
               'GC_LIMIT_OOM' if measured_failure and gc_oom else 'ERROR')

    encoded_bytes = sum(x['encoded_buffer_bytes'] for x in populations) if shape else None
    storage_summary = []
    for checkpoint in storage:
        blocks = checkpoint.get('local_blocks', [])
        all_on_disk = (len(blocks) == 16 and
                       all(b.get('disk_file_exists') and b.get('disk_file_bytes', 0) > 0
                           for b in blocks))
        physical = sum(b.get('disk_file_bytes', 0) for b in blocks)
        storage_summary.append(dict(
            phase=checkpoint['phase'], iteration=checkpoint['iteration'],
            memory_store_estimated_bytes=checkpoint['memory_bytes'],
            disk_status_bytes=checkpoint['disk_bytes'],
            actual_cache_disk_file_bytes=physical, all_cache_blocks_on_disk=all_on_disk,
            serialized_file_to_raw_value_ratio=physical / float(oracle['raw_value_bytes'])
                if all_on_disk else None,
            disk_blocks=sum(bool(b.get('disk_file_exists')) for b in blocks)))
    measured = [x['seconds'] for x in phases if x['phase'] == 'read' and
                x['iteration'] > 0 and x['success']]
    return dict(outcome=outcome, issues=issues, mode=mode, heap_gib=meta['heap_gib'],
                storage_level=meta['storage'], rows=expected_rows,
                raw_value_bytes=oracle['raw_value_bytes'], complete=complete, process=process,
                shape_valid=shape, partitions=populations, phases=phases,
                fill_seconds=next((x['seconds'] for x in phases
                                   if x['phase'] == 'fill' and x['success']), None),
                warmup_seconds=next((x['seconds'] for x in phases
                                     if x['phase'] == 'read' and x['iteration'] == 0 and
                                     x['success']), None),
                measured_read_seconds=measured,
                median_read_seconds=statistics.median(measured)
                    if successful and len(measured) == 3 else None,
                payload_encoded_bytes=encoded_bytes,
                payload_to_raw_value_ratio=encoded_bytes / float(oracle['raw_value_bytes'])
                    if encoded_bytes is not None else None,
                storage_checkpoints=storage, storage_summary=storage_summary,
                phase_observations=phase_observations, process_sampling=sampling,
                actual_max_heap=config.get('heap_max'), snapshots=snapshots, read_deltas=deltas,
                tasks=len(tasks),
                failed_tasks=sum(x.get('Task End Reason', {}).get('Reason') != 'Success' for x in tasks),
                operators=[x for x in data if x['event'] == 'operator'],
                note='Observed heap occupancy, not live-set/minimum heap. Timed reads include hash work. '
                     'CPU and PCBS use different cache formats. IO deltas cover in-phase sample intervals; '
                     'read_bytes is not rchar. Partial disk residency has no whole-cache disk ratio.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.case), indent=2))
