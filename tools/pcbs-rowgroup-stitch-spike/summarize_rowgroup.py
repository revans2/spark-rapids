# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Summarize explicit completed 96 GiB cases; never scan a matrix or launch GPU work."""
import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import statistics

ROWS_PER_PARTITION = 50331648
PARTITIONS = 16
ROWS = ROWS_PER_PARTITION * PARTITIONS
LOGICAL_BYTES = ROWS * 128
MIB = 2**20
GIB = 2**30


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(MIB), b''):
            h.update(block)
    return h.hexdigest()


def config_prefix(path, budget=256*1024):
    """Read only the bounded startup prefix, stopping at the actual runtime config."""
    with path.open('rb') as stream:
        remaining = budget
        while remaining:
            line = stream.readline(remaining)
            if not line:
                break
            remaining -= len(line)
            if line.startswith(b'REAL_CACHE '):
                value = json.loads(line[len(b'REAL_CACHE '):])
                if value.get('event') == 'config':
                    return value
    raise ValueError('No runtime config in bounded prefix: ' + str(path))


def load_case(directory, historical=False):
    directory = Path(directory).resolve()
    # Do not read even startup logs until an explicit result and exited process are recorded.
    result = json.loads((directory / 'result.json').read_text())
    process = json.loads((directory / 'process.json').read_text())
    meta = json.loads((directory / 'command.json').read_text())
    if process != result['process'] or process.get('exit_code') is None or process.get('timeout'):
        raise ValueError('Case process completion is inconsistent: ' + str(directory))
    if result['outcome'] not in ('PASS', 'HEAP_OOM') or result.get('issues'):
        raise ValueError('Case is not a verified full result: ' + str(directory))
    if result['outcome'] == 'PASS' and (not result.get('complete') or process['exit_code'] != 0):
        raise ValueError('Successful case did not complete: ' + str(directory))
    if result['outcome'] == 'HEAP_OOM' and process['exit_code'] == 0:
        raise ValueError('OOM case has successful process status: ' + str(directory))
    if meta.get('smoke') or meta['rows_per_partition'] != ROWS_PER_PARTITION or result['rows'] != ROWS:
        raise ValueError('Only completed full canonical 96 GiB workloads are included')
    config = config_prefix(directory / 'run.log')
    conf = config['conf']
    if config['logical_value_bytes'] != LOGICAL_BYTES or config['rows'] != ROWS:
        raise ValueError('Actual runtime input volume differs')
    required_controls = {
        'spark.rapids.sql.batchSizeBytes': '1073741824',
        'spark.rapids.sql.concurrentGpuTasks': '2',
        'spark.rapids.sql.maxConcurrentGpuTasks': '2',
        'spark.rapids.memory.gpu.allocSize': '15g',
        'spark.rapids.memory.gpu.maxAllocSize': '15g',
        'spark.rapids.memory.host.offHeapLimit.size': '40g',
        'spark.rapids.memory.host.offHeapLimit.enabled': 'true',
        'spark.rapids.memory.pinnedPool.size': '2g'}
    if config['master'] != 'local[8]' or any(conf.get(k) != v for k, v in required_controls.items()):
        raise ValueError('Resource controls differ: ' + str(directory))
    if conf.get('spark.memory.fraction', '0.6') != '0.6':
        raise ValueError('Memory fraction differs')
    jvm = config['jvm_arguments']
    if not any(v.lower() in ('-xmx48g', '-xmx49152m') for v in jvm) or not (
            '-XX:+UseParallelGC' in jvm and '-XX:ParallelGCThreads=4' in jvm):
        raise ValueError('Heap/GC controls differ')
    manifest_path = Path(meta['manifest'])
    manifest_sha = digest(manifest_path)
    if historical:
        # Audited old controlled-case schema: serializer/storage_level and codec_snapshots.
        mode = meta['serializer']
        storage = meta['storage_level']
        snapshots = result['codec_snapshots']
        if meta['record_mib'] != 1024 or meta['consumer_targets'] != [1024]:
            raise ValueError('Historical reference must be the one-target 1 GiB serialization gate')
        if manifest_sha != meta['manifest_sha256']:
            raise ValueError('Historical source manifest changed')
        method = 'whole producer batch'
        record_target = 1024
        row_target = None
        family = 'historical'
        artifact_signature = None
    else:
        # New row-group schema: mode/storage, artifact_spec, snapshots, frozen_hashes.
        mode, storage = meta['mode'], meta['storage']
        snapshots = result['snapshots']
        spec = meta['artifact_spec']
        if manifest_sha != meta['frozen_hashes'][str(manifest_path.resolve())]:
            raise ValueError('Row-group source manifest changed')
        method = 'one standalone Parquet row group'
        record_target = spec['rowgroup_target_bytes'] / MIB
        row_target = spec['rowgroup_target_rows']
        if spec['global_batch_bytes'] != GIB or spec['decode_target_bytes'] != GIB:
            raise ValueError('Row-group producer/decode target differs')
        family = 'row-group'
        artifact_signature = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    if mode not in ('java', 'codec') or storage != 'MEMORY_AND_DISK':
        raise ValueError('This bounded comparison requires Java/codec MEMORY_AND_DISK cases')
    if not snapshots or snapshots[0]['phase'] != 'fill':
        raise ValueError('Fill producer fingerprint unavailable')
    producer = snapshots[0]['snapshot']
    fingerprint = (producer['gpu_produced_batches'], producer['gpu_produced_rows'],
                   producer['gpu_produced_bytes'], producer['gpu_produced_min_bytes'],
                   producer['gpu_produced_max_bytes'])
    if fingerprint != (112, ROWS, LOGICAL_BYTES, 806860032, 939265152):
        raise ValueError('Full producer fingerprint differs: ' + str(directory))
    populations = result['partitions']
    if (len(populations) != PARTITIONS or
            {p['partition'] for p in populations} != set(range(PARTITIONS)) or
            any(p['rows'] != ROWS_PER_PARTITION or p['records'] <= 0 for p in populations)):
        raise ValueError('Full cache population unavailable: ' + str(directory))
    if sum(p['records'] for p in populations) != result['records']:
        raise ValueError('Stored record totals differ')
    encoded = sum(p['encoded_buffer_bytes'] for p in populations)
    phases = result['phases']
    fills = [p for p in phases if p['phase'] == 'fill']
    if len(fills) != 1 or not fills[0]['success']:
        raise ValueError('Comparison needs a completed full cache fill')
    reads = [p for p in phases if p['phase'] == 'read']
    measured = [p['seconds'] for p in reads if p['iteration'] > 0 and p['success']]
    if result['outcome'] == 'PASS' and (len(measured) != 3 or
            [p['iteration'] for p in reads] != [0, 1, 2, 3] or not all(p['success'] for p in reads)):
        raise ValueError('Expected one warmup and three measured reads')
    warmups = [p['seconds'] for p in reads if p['iteration'] == 0 and p['success']]
    heap = result['phase_heap']
    def maximum(phase):
        values = [p['observed_peak_bytes'] for p in heap if p['phase'] == phase]
        return max(values) / GIB if values else None
    compressed = config.get('cpu_compression')
    if compressed not in ('true', 'false'):
        raise ValueError('Actual compression configuration unavailable')
    compression = 'SNAPPY' if compressed == 'true' else 'NONE'
    if not historical and compression != meta['artifact_spec']['compression']:
        raise ValueError('Actual compression differs from artifact description')
    return dict(case=str(directory), adapter=family, mode=mode, storage=storage,
                artifact_signature=artifact_signature,
                reset=int(conf['spark.serializer.objectStreamReset']),
                unroll=int(conf['spark.storage.unrollMemoryCheckPeriod']),
                source_manifest_sha256=manifest_sha, compression=compression,
                producer_target_mib=1024, decode_target_mib=1024,
                private_record_method=method, private_record_target_mib=record_target,
                private_record_target_rows=row_target,
                actual_record_min_mib=min(p['min_encoded_bytes'] for p in populations)/MIB,
                actual_record_max_mib=max(p['max_encoded_bytes'] for p in populations)/MIB,
                records=result['records'], fill_seconds=fills[0]['seconds'],
                warmup_seconds=warmups[0] if len(warmups) == 1 else None,
                measured_read_seconds=measured,
                median_read_seconds=statistics.median(measured)
                    if result['outcome'] == 'PASS' else None,
                failed_read_elapsed_seconds=[p['seconds'] for p in reads if not p['success']],
                peak_fill_heap_gib=maximum('fill'), peak_read_heap_gib=maximum('read'),
                encoded_to_logical_value_ratio=encoded/float(LOGICAL_BYTES),
                outcome=result['outcome'],
                evidence_sha256={name:digest(directory/name)
                                 for name in ['command.json','process.json','result.json']})


NOTES = [
    'All included cases populated the same 96 GiB logical values in 16 partitions, with local[8], '
    '48 GiB configured heap, ParallelGC four workers, 15 GiB GPU pool/concurrency two, '
    'host limit 40 GiB/pinned pool 2 GiB.',
    'The consumer is the same simple count plus sums over all 16 columns; this does not measure '
    'substantial downstream GPU work or a CPU-cache alternative.',
    'Java means ordinary Java object serialization of cached records; codec means the research '
    'custom cache-byte path. Spark BlockManager/MemoryStore remain stock in both.',
    'Historical runs have 1 GiB producer and cache-record targets. Row-group runs encode each '
    'large producer batch once, store smaller standalone row-group files, and combine files before GPU decoding.',
    'Historical private PCBS targets budget estimated GPU device bytes before Parquet encoding. '
    'New row-group targets are writer uncompressed-data byte and row targets. Neither is a hard '
    'encoded-size bound. Actual record ranges are encoded standalone Parquet file sizes, excluding '
    'Java stream framing. The combined GPU decode target remains 1 GiB.',
    'Compression ratio means Parquet payload bytes divided by logical value bytes; it includes '
    'file/footer overhead and is not the pure compression-codec ratio.',
    'Read timings exclude warmup. Grouped read ranges are the minimum/maximum of per-JVM '
    'three-read medians; individual observations remain in JSON.',
    'Peak heap is observed occupancy (including garbage), not live heap or minimum required memory. '
    'Read peaks include warmup; OOM peaks describe a failed, incomplete read and are not workload bounds.',
    'The table reports fresh JVM counts. The initial row-group pair has one JVM per mode; '
    'the prescribed historical reference set has three per setting. New timings are preliminary.',
    'Encoding layout, decode path and reset/unroll settings intentionally differ between some rows; '
    'those comparisons do not isolate a single cause. Only the two new modes share the same row-group artifacts.',
    'This reporter consumes existing verified results and bounded runtime config prefixes; it does '
    'not replace independent raw-result verification.'
]


def groups(cases):
    buckets = defaultdict(list)
    for case in cases:
        key = tuple(case[k] for k in ('adapter','mode','storage','reset','unroll','compression',
                    'private_record_method','private_record_target_mib','private_record_target_rows'))
        buckets[key].append(case)
    rows = []
    for key, items in buckets.items():
        row = {k:items[0][k] for k in ('adapter','mode','storage','reset','unroll','compression',
                    'private_record_method','private_record_target_mib','private_record_target_rows')}
        row.update(jvms=len(items), outcomes={name:sum(i['outcome']==name for i in items)
                    for name in sorted({i['outcome'] for i in items})},
                   cases=[i['case'] for i in items])
        for field in ['fill_seconds','warmup_seconds','median_read_seconds',
                      'peak_fill_heap_gib','peak_read_heap_gib','encoded_to_logical_value_ratio']:
            row[field] = [i[field] for i in items if i[field] is not None]
        row['actual_record_min_mib'] = min(i['actual_record_min_mib'] for i in items)
        row['actual_record_max_mib'] = max(i['actual_record_max_mib'] for i in items)
        rows.append(row)
    return rows


def interval(values, precision=2):
    if not values:
        return '—'
    low, high = min(values), max(values)
    fmt = '{:.' + str(precision) + 'f}'
    return fmt.format(low) if low == high else fmt.format(low)+'–'+fmt.format(high)


def markdown(rows):
    lines = ['# Row-group prototype and historical references', '',
             'One row per method/settings cohort. Seconds, MiB and GiB are labeled below.', '',
             '| Method / mode | JVMs / outcome | Storage | Compression | Reset / unroll | Producer / record target MiB | '
             'Actual records MiB | Fill s | Warmup s | Read median/JVM s | '
             'Peak fill / read heap GiB | Parquet / logical bytes |',
             '|---|---|---|---|---|---|---|---|---|---|---|---|']
    for r in rows:
        outcome = ', '.join(str(n)+' '+name for name,n in r['outcomes'].items())
        target = '1024 / '+str(r['private_record_target_mib'])
        peaks = interval(r['peak_fill_heap_gib'])+' / '+interval(r['peak_read_heap_gib'])
        if r['outcomes'].get('HEAP_OOM'):
            peaks += '*'
        lines.append('| {} / {} | {} / {} | {} | {} | {} / {} | {} | {}–{} | {} | {} | {} | {} | {} |'.format(
            r['adapter'], r['mode'], r['jvms'], outcome, r['storage'], r['compression'], r['reset'], r['unroll'],
            target, interval([r['actual_record_min_mib']]), interval([r['actual_record_max_mib']]),
            interval(r['fill_seconds']), interval(r['warmup_seconds']),
            interval(r['median_read_seconds']), peaks,
            interval(r['encoded_to_logical_value_ratio'], 6)))
    lines += ['', '*OOM read occupancy is a partial observation. Failed read durations are retained '
              'in JSON and are not reported as successful warmups or measured reads.', '',
              ''] + ['- '+note for note in NOTES] + ['']
    return '\n'.join(lines)


def summarize(new_cases, historical_cases):
    paths = [Path(p).resolve() for p in list(new_cases)+list(historical_cases)]
    if not paths or len(paths) != len(set(paths)):
        raise ValueError('Provide distinct explicit case paths')
    cases = [load_case(p) for p in new_cases] + [load_case(p, True) for p in historical_cases]
    if len({c['source_manifest_sha256'] for c in cases}) != 1:
        raise ValueError('Cases do not share the same canonical input manifest')
    if len({c['artifact_signature'] for c in cases if c['adapter'] == 'row-group'}) > 1:
        raise ValueError('New cases do not share the same row-group artifact manifest')
    return dict(notes=NOTES, cases=cases, cohorts=groups(cases))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', action='append', default=[], type=Path)
    parser.add_argument('--historical-case', action='append', default=[], type=Path)
    parser.add_argument('--out', required=True, type=Path, help='Fresh directory; never overwrite')
    args = parser.parse_args()
    if args.out.exists():
        raise ValueError('Output already exists')
    report = summarize(args.case, args.historical_case)
    args.out.mkdir(parents=True, exist_ok=False)
    with (args.out/'summary.json').open('x') as stream:
        stream.write(json.dumps(report, indent=2)+'\n')
    with (args.out/'summary.md').open('x') as stream:
        stream.write(markdown(report['cohorts']))
    print(str(args.out/'summary.md'))


if __name__ == '__main__':
    main()
