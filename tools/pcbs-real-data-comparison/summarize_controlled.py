# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Summarize controlled research results without reading data, logs or runtime artifacts.

Usage: python3 summarize_controlled.py MATRIX/results.json --out /data/.../summary
A sibling plan.json supplies not-yet-attempted cases. The input is read once;
rerun to refresh a live matrix. This script is a reporter, not a verifier.
"""
import argparse
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import statistics


def maximum(values):
    values = [v for v in values if v is not None]
    return max(values) if values else None


def total(records, key):
    values = [r.get(key) for r in records]
    return sum(values) if values and all(v is not None for v in values) else None


def observed_range(values):
    values = [v for v in values if v is not None]
    return {'min': min(values), 'max': max(values)} if values else None


def rows_for(entry, source):
    case, result = entry['case'], entry.get('result') or {}
    targets = result.get('consumer_targets') or [
        int(v) for v in str(case.get('targets', '')).split(',') if v]
    phases = result.get('phases', [])
    heap = {(p['phase'], p['iteration']): p.get('observed_peak_bytes')
            for p in result.get('phase_heap', [])}
    fill = [p for p in phases if p['phase'] == 'fill']
    snapshot = next((s['snapshot'] for s in result.get('codec_snapshots', [])
                     if s.get('phase') == 'fill'), {})
    producer = {k: v for k, v in snapshot.items() if k.startswith('gpu_produced_')}
    outcome = result.get('outcome', 'NOT_STARTED')
    for group, target in enumerate(targets):
        reads = [p for p in phases if p['phase'] == 'read' and
                 p['iteration'] // 4 == group and p['iteration'] % 4 > 0]
        good = [p for p in reads if p.get('success')]
        times = [p['seconds'] for p in good]
        # Keep partial observations, but never turn them into successful timings.
        median = statistics.median(times) if len(good) == 3 else None
        deltas = [d for d in result.get('gpu_read_deltas', [])
                  if d.get('phase') == 'read' and d['iteration'] // 4 == group and
                  d['iteration'] % 4 > 0]
        residencies = [s for s in result.get('storage', []) if s['phase'] == 'read'
                       and s['iteration'] // 4 == group and s['iteration'] % 4 > 0]
        codec = case.get('serializer') == 'codec'
        row = {k: case.get(k) for k in
               ('id', 'stage', 'storage', 'repetition', 'serializer', 'reset', 'period',
                'record_mib', 'targets')}
        row.update(
            source=str(source), manifest_sha256=result.get('manifest_sha256'),
            consumer_target_mib=target, consumer_schedule_set=','.join(
                str(v) for v in sorted(targets)), outcome=outcome,
            failure_phase=result.get('failure_phase'), issues=result.get('issues', []),
            fill_seconds=result.get('fill_seconds'), read_seconds=times,
            median_read_seconds=median,
            fill_heap_bytes=maximum(heap.get(('fill', p['iteration'])) for p in fill),
            read_heap_bytes=maximum(heap.get(('read', p['iteration'])) for p in reads),
            read_including_warmup_heap_bytes=maximum(
                heap.get(('read', p['iteration'])) for p in phases
                if p['phase'] == 'read' and p['iteration'] // 4 == group),
            warmup_heap_bytes=heap.get(('read', group * 4)),
            fill_gc_count=total(fill, 'gc_count'), fill_gc_ms=total(fill, 'gc_ms'),
            read_gc_count=total(reads, 'gc_count'), read_gc_ms=total(reads, 'gc_ms'),
            measured_read_observations=len(good),
            heap_scope='completed phases; incomplete workload, not a memory bound'
                       if outcome not in ('PASS', 'SMOKE_PASS') else 'completed workload',
            rows=result.get('rows'), records=result.get('records'),
            encoded_buffer_bytes=result.get('encoded_buffer_bytes') or None,
            max_encoded_record_bytes=result.get('max_encoded_record_bytes') or None,
            producer_fingerprint=producer,
            decoded_max_bytes=maximum(d.get('gpu_decoded_max_bytes') for d in deltas),
            combined_max_bytes=maximum(d.get('gpu_combined_max_bytes') for d in deltas),
            decoded_batches=[d.get('gpu_decoded_batches') for d in deltas],
            combined_batches=[d.get('gpu_combined_batches') for d in deltas],
            codec_fill_payload_bytes=snapshot.get('codec_write_bytes') if codec else None,
            codec_read_payload_bytes=[d.get('codec_read_bytes') for d in deltas]
                                     if codec else None,
            codec_read_records=[d.get('codec_read_records') for d in deltas]
                               if codec else None,
            serialization_note='custom-codec payload counters; exclude framing'
                               if codec else 'Java serialized byte counters unavailable',
            storage_checkpoints=residencies,
            memory_bytes_range=observed_range(s.get('memory_bytes') for s in residencies),
            disk_bytes_range=observed_range(s.get('disk_bytes') for s in residencies),
            warmup_storage=[s for s in result.get('storage', []) if s['phase'] == 'read'
                            and s['iteration'] == group * 4],
            process_sampling=result.get('process_sampling'),
            gpu_sampling=result.get('gpu_sampling'))
        for count in (1, 4):
            row['fill_plus_%d_reads_seconds' % count] = (
                row['fill_seconds'] + count * median
                if outcome == 'PASS' and row['fill_seconds'] is not None and
                median is not None else None)
        yield row


METRICS = (
    'fill_seconds', 'median_read_seconds', 'fill_heap_bytes', 'read_heap_bytes',
    'fill_gc_count', 'fill_gc_ms', 'read_gc_count', 'read_gc_ms',
    'read_including_warmup_heap_bytes', 'warmup_heap_bytes',
    'max_encoded_record_bytes', 'records', 'decoded_max_bytes', 'combined_max_bytes',
    'fill_plus_1_reads_seconds', 'fill_plus_4_reads_seconds')
GROUP = ('source', 'manifest_sha256', 'stage', 'storage', 'serializer', 'reset',
         'period', 'record_mib', 'consumer_schedule_set', 'consumer_target_mib')


def summarize(sources):
    rows, inputs, plans = [], [], []
    for source in sources:
        source = Path(source).resolve()
        entries = json.loads(source.read_text()) if source.exists() else []
        if not isinstance(entries, list):
            raise ValueError('Expected result array: ' + str(source))
        plan_path = source.with_name('plan.json')
        plan = json.loads(plan_path.read_text()) if plan_path.exists() else {}
        inputs.append({'source': str(source), 'entries': entries})
        plans.append({'source': str(plan_path), 'plan': plan})
        ids = [entry['case']['id'] for entry in entries]
        if len(ids) != len(set(ids)):
            raise ValueError('Duplicate case IDs: ' + str(source))
        complete = set(ids)
        entries = entries + [{'case': c, 'result': {
            'manifest_sha256': plan.get('canonical_manifest_sha256')}}
            for c in plan.get('cases', []) if c['id'] not in complete]
        for entry in entries:
            rows.extend(rows_for(entry, source))
    buckets = defaultdict(list)
    for row in rows:
        buckets[tuple(row.get(k) for k in GROUP)].append(row)
    groups = []
    for key, cases in buckets.items():
        successes = [r for r in cases if r['outcome'] == 'PASS']
        group = dict(zip(GROUP, key))
        group.update(outcomes=dict(Counter(r['outcome'] for r in cases)),
                     completed_fill_observed_range=observed_range(
                         r.get('fill_seconds') for r in cases),
                     pass_count=len(successes),
                     attempted_count=sum(r['outcome'] != 'NOT_STARTED' for r in cases),
                     case_ids=[r['id'] for r in cases],
                     successful_ranges={k: observed_range(r.get(k) for r in successes)
                                        for k in METRICS},
                     observed_heap_ranges_including_failures={
                         k: observed_range(r.get(k) for r in cases)
                         for k in ('fill_heap_bytes', 'read_heap_bytes')})
        groups.append(group)
    return {'notes': [
        'No cross-matrix aggregation. Groups retain stage, storage, consumer schedule, '
        'settings and manifest. Counterbalanced consumer order is pooled across JVMs, '
        'with the actual order retained per case. Sibling plan preserves control evidence.',
        'This reporter consumes verifier results; it does not verify source/runtime controls.',
        'Read timing/GC/read_heap_bytes exclude target-aligned warmup; separate '
        'warmup_heap_bytes and read_including_warmup_heap_bytes retain its occupancy.',
        'Timing/read-heap ranges describe successful JVM observations, not confidence intervals. '
        'Completed fill ranges also retain fills preceding later OOM/ERROR; these are '
        'completed phase observations, not correctness passes.',
        'Failed/incomplete observations are retained separately, not heap requirements.',
        'Fill+N reads = successful fill + N times that same JVM/consumer read median; '
        'a reuse-cost estimate, not measured end-to-end wall time. Warmup is excluded.',
        'A 1 GiB producer target produced a canonical maximum 895.753 MiB; no minimum '
        'fullness is implied. Coalescing cannot split an oversized decoded input.',
        'Consumer targets run on the same cache in order; residency/page-cache/GC state '
        'can evolve. Do not treat them as independent fresh-cache experiments.',
        'Java custom-codec counters are unavailable, not zero. Codec payload counters '
        'exclude framing and do not measure generic Java serialization.',
        'Heap is max(sampled occupancy, logged pre-GC occupancy), not post-GC live data.',
        'Residency checkpoints are after measured reads, not time-integrated residency.'
    ], 'rows': rows, 'groups': groups, 'raw_inputs': inputs, 'plans': plans}


def cell(value):
    if value is None:
        return ''
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(',', ':'))
    return value


def write_csv(path, rows):
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, lineterminator='\n')
        writer.writeheader()
        writer.writerows({k: cell(v) for k, v in row.items()} for row in rows)


def format_range(value, divisor=1):
    if not value:
        return '—'
    low, high = value['min'] / divisor, value['max'] / divisor
    return '%.3f' % low if low == high else '%.3f–%.3f' % (low, high)


def write_outputs(report, out):
    out.mkdir(parents=True, exist_ok=True)
    (out / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
    write_csv(out / 'cases.csv', report['rows'])
    write_csv(out / 'groups.csv', report['groups'])
    lines = ['# Controlled cache experiment observations', '',
             'Live snapshot; sparse cells are unavailable, not zero.', '']
    lines += ['- ' + note for note in report['notes']]
    lines += ['', 'Ranges across JVMs: completed fills include all outcomes; other columns require PASS. '
              'Outcome counts include pending and failed cases.',
              '', '| Stage | Storage | Mode | Reset/period | Record MiB | Output MiB | '
              'Outcomes | Completed fill s (all outcomes) | Read s (PASS) | Fill heap GiB (PASS) | '
              'Read heap incl warmup GiB (PASS) | Fill+1 read s | '
              'Fill+4 reads s |',
              '|---|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---:|']
    for group in report['groups']:
        ranges = group['successful_ranges']
        values = [group['stage'], group['storage'], group['serializer'],
                  '%s/%s' % (group['reset'], group['period']), group['record_mib'],
                  group['consumer_target_mib'], json.dumps(group['outcomes']),
                  format_range(group['completed_fill_observed_range']),
                  format_range(ranges['median_read_seconds']),
                  format_range(ranges['fill_heap_bytes'], 2**30),
                  format_range(ranges['read_including_warmup_heap_bytes'], 2**30),
                  format_range(ranges['fill_plus_1_reads_seconds']),
                  format_range(ranges['fill_plus_4_reads_seconds'])]
        lines.append('| ' + ' | '.join(str(v).replace('|', '\\|') for v in values) + ' |')
    lines += ['', 'Full-precision per-case/consumer data, GC, residency and batch counters: '
              '[cases.csv](cases.csv). Failed phase observations and complete source verifier '
              'results: [summary.json](summary.json).']
    (out / 'summary.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results', type=Path, nargs='+')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    write_outputs(summarize(args.results), args.out)
