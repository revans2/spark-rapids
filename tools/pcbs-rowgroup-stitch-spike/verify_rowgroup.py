# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Research-only verifier; row-group instrumentation is an explicit artifact contract."""
import argparse
import csv
import json
import math
from pathlib import Path
import shlex
import statistics
import sys
from urllib.parse import urlparse

REAL = Path(__file__).resolve().parent.parent / 'pcbs-real-data-comparison'
sys.path.insert(0, str(REAL))
from canonical_source import digest, events
from audit_results import gc_records

STOCK_CORE = '/home/roberte/src/spark_3.5.5/assembly/target/scala-2.12/jars/spark-core_2.12-3.5.5.jar'


def verify(case):
    case = Path(case)
    meta = json.loads((case / 'command.json').read_text())
    process = json.loads((case / 'process.json').read_text())
    data = events(case / 'run.log')
    raw = (case / 'run.log').read_text(errors='replace')
    issues = []

    def require(condition, message):
        if not condition:
            issues.append(message)

    config = next((x for x in data if x['event'] == 'config'), {})
    conf = config.get('conf', {})
    expected_rows = 16 * meta['rows_per_partition']
    signature = [('fill', 0)] + [('read', i) for i in range(4)]
    require(config.get('action') == 'gpu-controlled', 'wrong harness action')
    require(config.get('master') == 'local[8]' and config.get('parts') == 16,
            'task/partition controls differ')
    require(config.get('spark_version') == '3.5.5', 'Spark version differs')
    require(config.get('rows') == expected_rows and
            config.get('logical_value_bytes') == expected_rows * 128, 'input volume differs')
    require(config.get('cache_serializer') == meta['cache_serializer'], 'cache serializer differs')
    require(config.get('serializer_class') == 'org.apache.spark.serializer.JavaSerializer',
            'generic serializer differs')
    jvm = config.get('jvm_arguments', [])
    require(any(x.lower() in ('-xmx48g', '-xmx49152m') for x in jvm),
            'configured heap differs')
    require(0 < config.get('heap_max', 0) <= 48 * 2**30, 'invalid actual maximum heap')
    for key, value in meta['expected_conf'].items():
        if key == 'spark.driver.extraJavaOptions':
            require(all(x in jvm for x in shlex.split(value)), 'requested JVM option missing')
        else:
            require(conf.get(key) == value, 'runtime control differs: ' + key)
    require(conf.get('spark.memory.fraction', '0.6') == '0.6', 'memory fraction differs')
    origins = {k: urlparse(v).path for k, v in config.get('runtime_origins', {}).items()}
    expected_origins = {k: STOCK_CORE for k in [
        'org.apache.spark.storage.BlockManager', 'org.apache.spark.storage.memory.MemoryStore',
        'org.apache.spark.serializer.SerializerManager']}
    if meta['mode'] == 'codec':
        expected_origins['org.apache.spark.serializer.SerializerManager'] = meta['codec_overlay']
    require(origins == expected_origins, 'Spark class origins differ')
    for path, expected in meta['frozen_hashes'].items():
        require(digest(path) == expected, 'frozen input/artifact changed: ' + path)
    manifest = json.loads(Path(meta['manifest']).read_text())
    require(manifest['oracle']['rows'] == expected_rows, 'manifest row total differs')
    files = [e for e in data if e['event'] == 'canonical_file']
    require([(e['path'], e['rows'], e['bytes'], e['schema']) for e in files] ==
            [(e['path'], e['rows'], e['bytes'], e['schema']) for e in manifest['files']],
            'source footer layout differs')
    plan = next((e for e in data if e['event'] == 'cache_plan'), {})
    require(plan.get('serializer') == meta['cache_serializer'], 'cache builder serializer differs')
    flags = [True, True, False, True] if meta['storage'] == 'MEMORY_AND_DISK' else [
        True, False, False, False]
    require(plan.get('storage_flags') == flags and plan.get('partitions') == 16 and
            plan.get('replication') == 1, 'storage/layout controls differ')
    populations = [e for e in data if e['event'] == 'partition']
    shape = len(populations) == 16 and {e['partition'] for e in populations} == set(range(16))
    shape = shape and all(e['rows'] == meta['rows_per_partition'] and
                          e['records'] > 0 and e['encoded_buffer_bytes'] > 0 for e in populations)
    phases = [e for e in data if e['event'] == 'phase']
    reads = [e for e in data if e['event'] == 'read']
    scans = [e for e in data if e['event'] == 'scan']
    storage = [e for e in data if e['event'] == 'storage']
    snapshots = [e for e in data if e['event'] == 'codec']
    complete = any(e['event'] == 'complete' and e.get('success') for e in data)
    successful = complete and process['exit_code'] == 0 and not process['timeout']
    spec = meta['artifact_spec']
    metric_names = ['rg_encode_attempts', 'rg_encode_successes', 'rg_encoded_rows',
                    'rg_stored_records', 'rg_stored_rows', 'rg_stored_bytes', 'rg_stored_max_bytes',
                    'rg_stored_max_estimated_decoded_bytes',
                    'rg_decode_attempts', 'rg_decode_successes', 'rg_decode_files', 'rg_decode_rows',
                    'rg_decode_device_bytes', 'rg_decode_max_device_bytes',
                    'rg_bundle_max_estimated_decoded_bytes', 'rg_bundle_max_encoded_bytes']
    deltas = []
    for before, after in zip(snapshots, snapshots[1:]):
        a, b = before['snapshot'], after['snapshot']
        delta = {key: value - a.get(key, 0) for key, value in b.items()
                 if isinstance(value, (int, float)) and 'max' not in key and 'min' not in key}
        delta.update({key: value for key, value in b.items() if 'max' in key or 'min' in key})
        delta['iteration'] = after['iteration']
        deltas.append(delta)

    if successful:
        require(shape, 'cache population incomplete')
        require([(x['phase'], x['iteration']) for x in phases] == signature and
                all(x['success'] for x in phases), 'timing phases incomplete')
        require([x['iteration'] for x in reads] == list(range(4)), 'read sequence differs')
        require([x['iteration'] for x in scans] == list(range(4)) and all(
            x['rows'] == expected_rows and x['columns'] == 16 and
            'GpuInMemoryTableScan' in x['class'] for x in scans), 'full GPU scan missing')
        for read in reads:
            require(read['rows'] == expected_rows and len(read['sums']) == 16 and all(
                math.isfinite(a) and abs(a-b) <= max(1.0, abs(b)) * 1e-10
                for a, b in zip(read['sums'], manifest['oracle']['sums'])), 'oracle mismatch')
        require([(x['phase'], x['iteration']) for x in storage] == signature and all(
            x['cached_partitions'] == 16 and x['memory_bytes'] + x['disk_bytes'] > 0
            for x in storage), 'storage checkpoints incomplete')
        if meta['storage'] == 'DISK_ONLY':
            require(all(x['memory_bytes'] == 0 for x in storage), 'disk-only memory residency')
        require(any(x['event'] == 'unpersist' and x.get('success') for x in data),
                'blocking cleanup missing')
        require([(x['phase'], x['iteration']) for x in snapshots] == signature,
                'instrumentation checkpoints incomplete')
        if snapshots:
            fill = snapshots[0]['snapshot']
            require(fill.get('gpu_produced_rows') == expected_rows, 'producer rows differ')
            produced_bytes = fill.get('gpu_produced_bytes', 0)
            if meta['smoke']:
                # This small nullable fixture can retain 16 validity masks (two bytes/row).
                require(expected_rows * 128 <= produced_bytes <= expected_rows * 130,
                        'smoke producer bytes outside values plus validity-mask allowance')
            else:
                require(produced_bytes == expected_rows * 128, 'full producer bytes differ')
                require(fill.get('gpu_produced_batches') == 112,
                        'full producer layout changed from 112 original large batches')
                require(fill.get('gpu_produced_min_bytes') == 806860032 and
                        fill.get('gpu_produced_max_bytes') == 939265152,
                        'full producer minimum/maximum differs from historical fingerprint')
            # Do not invent a producer lower byte bound: exact full fingerprint controls layout.
            encoded_count = sum(x['records'] for x in populations)
            require(all(isinstance(fill.get(k), (int, float)) for k in metric_names),
                    'row-group instrumentation contract missing')
            require(fill.get('rg_encode_successes') == fill.get('gpu_produced_batches'),
                    'encoding was repeated or input was pre-split')
            require(fill.get('rg_encode_attempts', 0) >= fill.get('rg_encode_successes', 0),
                    'encoding attempt/success counters inconsistent')
            require(fill.get('rg_encoded_rows') == fill.get('rg_stored_rows') == expected_rows,
                    'encoded/stored rows differ')
            require(fill.get('rg_stored_records') == encoded_count, 'stored record count differs')
            require(fill.get('rg_stored_bytes') == sum(x['encoded_buffer_bytes'] for x in populations),
                    'stored byte counter differs')
            require(fill.get('rg_stored_max_bytes') == max(
                    (x['max_encoded_bytes'] for x in populations), default=0),
                    'stored maximum byte counter differs')
            require(all(x['max_encoded_bytes'] <= spec['record_safety_limit_bytes']
                        for x in populations),
                    'coarse record safety guard exceeded; not an 8 MiB target-size assertion')
            require(encoded_count > fill.get('gpu_produced_batches', 0) or meta['smoke'],
                    'large input did not produce separate row-group records')
        require(len(deltas) == 4, 'per-read instrumentation missing')
        for delta in deltas:
            require(delta.get('gpu_produced_rows') == 0, 'cache unexpectedly recomputed')
            require(delta.get('gpu_decoded_rows') == expected_rows, 'decoded rows/retries differ')
            calls = delta.get('rg_decode_successes', 0)
            file_count = delta.get('rg_decode_files', 0)
            require(delta.get('rg_decode_attempts', 0) >= calls,
                    'decode attempt/success counters inconsistent')
            require(0 < calls <= file_count, 'native multi-file decode evidence missing')
            if not meta['smoke']:
                require(calls < file_count, 'row groups were decoded separately')
                require(delta.get('gpu_decoded_max_bytes', 0) >= 850 * 2**20,
                        'combined GPU decode remained small')
            require(delta.get('rg_decode_rows') == expected_rows, 'decoded bundle rows differ')
            require(delta.get('rg_decode_device_bytes') == delta.get('gpu_decoded_bytes'),
                    'successful decoded device bytes differ')
            require(0 < delta.get('rg_bundle_max_estimated_decoded_bytes', 0) <=
                    spec['decode_target_bytes'], 'bundle estimated decoded-byte guard exceeded')
            require(0 < delta.get('rg_bundle_max_encoded_bytes', 0) <=
                    spec['decode_target_bytes'], 'bundle encoded-byte guard exceeded')
            require(all(value >= 0 for key, value in delta.items()
                        if key != 'iteration' and isinstance(value, (int, float))),
                    'cumulative counters decreased')
            require((delta.get('codec_read_records', 0) >= 0 if meta['mode'] == 'codec'
                     else delta.get('codec_read_records', 0) == 0),
                    'Java arm unexpectedly used custom codec')
        if meta['mode'] == 'java':
            require(all(x['snapshot'].get('codec_write_records') == 0 and
                        x['snapshot'].get('codec_read_records') == 0 for x in snapshots),
                    'Java arm unexpectedly exercised custom byte codec')
        if meta['mode'] == 'codec' and snapshots and (
                not meta['smoke'] or meta['storage'] == 'DISK_ONLY'):
            final = snapshots[-1]['snapshot']
            require(final.get('codec_write_records', 0) > 0 and
                    final.get('codec_read_records', 0) > 0, 'custom codec byte path not exercised')
    task_ends = []
    event_files = [p for p in (case / 'events').rglob('*')
                   if p.is_file() and not p.name.startswith('.')]
    for path in sorted(event_files):
        for line in path.read_text().splitlines():
            value = json.loads(line)
            if value['Event'] == 'SparkListenerTaskEnd':
                task_ends.append(value)
    if successful:
        require(bool(task_ends), 'task evidence missing')
        require(all(x.get('Task End Reason', {}).get('Reason') == 'Success' for x in task_ends),
                'task failure/retry occurred')
    collections = gc_records(sorted(case.glob('gc.log*')))
    heap = []
    for phase in phases:
        logged = [x['before_bytes_rounded'] for x in collections
                  if phase['start_epoch_ms'] <= x['end_epoch_ms'] <= phase['end_epoch_ms']]
        heap.append(dict(phase=phase['phase'], iteration=phase['iteration'],
                         sampled_peak_bytes=phase['sampled_heap_peak'],
                         observed_peak_bytes=max([phase['sampled_heap_peak']] + logged)))
    sampler_path = case / 'process-sampler-result.json'
    sampler = json.loads(sampler_path.read_text()) if sampler_path.exists() else {}
    verified_pids = set(sampler.get('selected_pids') or [])
    rss_samples = []
    rejected_samples = 0
    sample_path = case / 'process-memory.csv'
    if verified_pids and sample_path.exists():
        with sample_path.open(newline='') as stream:
            for row in csv.DictReader(stream):
                try:
                    pid = int(row['pid'])
                    epoch = float(row['epoch_s']) * 1000
                    rss = int(row['VmRSS_KiB']) * 1024
                    hwm = int(row['VmHWM_KiB']) * 1024
                    if row.get('error') or pid not in verified_pids or not math.isfinite(epoch) or min(rss, hwm) < 0:
                        raise ValueError('invalid JVM sample')
                    rss_samples.append(dict(epoch_ms=epoch, rss=rss, hwm=hwm))
                except (ValueError, KeyError, TypeError):
                    rejected_samples += 1
    for observed, phase in zip(heap, phases):
        values = [x['rss'] for x in rss_samples if
                  phase['start_epoch_ms'] <= x['epoch_ms'] < phase['end_epoch_ms']]
        observed['sampled_rss_peak_bytes'] = max(values) if values else None
    measured = [x['seconds'] for x in phases if x['phase'] == 'read' and
                x['iteration'] > 0 and x['success']]
    outcome = ('SMOKE_PASS' if meta['smoke'] else 'PASS') if successful and not issues else (
        'HEAP_OOM' if not successful and not issues and process['exit_code'] != 0 and
        not process['timeout'] and 'java.lang.OutOfMemoryError: Java heap space' in raw else 'ERROR')
    return dict(outcome=outcome, issues=issues, process=process, complete=complete,
                rows=expected_rows, shape_valid=shape, records=sum(x['records'] for x in populations),
                compression=spec['compression'],
                encoded_buffer_bytes=sum(x['encoded_buffer_bytes'] for x in populations),
                encoded_to_logical_value_ratio=sum(x['encoded_buffer_bytes'] for x in populations) /
                    float(expected_rows * 128),
                encoded_to_decoded_device_ratios=[
                    sum(x['encoded_buffer_bytes'] for x in populations) / float(d['rg_decode_device_bytes'])
                    if d.get('rg_decode_device_bytes', 0) else None for d in deltas],
                partitions=populations, fill_seconds=next(
                    (x['seconds'] for x in phases if x['phase'] == 'fill'), None),
                measured_read_seconds=measured,
                median_read_seconds=statistics.median(measured) if successful and len(measured) == 3 else None,
                phase_heap=heap, actual_max_heap=config.get('heap_max'), phases=phases,
                process_sampling=dict(selected_pids=sorted(verified_pids),
                    valid_samples=len(rss_samples), rejected_samples=rejected_samples),
                whole_process_observed_hwm_bytes=max((x['hwm'] for x in rss_samples), default=None),
                retry_work=[dict(iteration=d['iteration'],
                    decode_attempts=d.get('rg_decode_attempts'),
                    decode_successes=d.get('rg_decode_successes')) for d in deltas],
                storage=storage, snapshots=snapshots, read_deltas=deltas,
                observed_producer_bytes=snapshots[0]['snapshot'].get('gpu_produced_bytes')
                    if snapshots else None,
                note='Heap is observed occupancy; failed workload peaks are not required memory bounds.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('case', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.case), indent=2))
