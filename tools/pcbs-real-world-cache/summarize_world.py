# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Render all world-cache cases in one sparse comparison; never run or change a case."""
import argparse
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import statistics

GIB = 2**30
MODES = {
    'cpu-default': ('Spark columnar', 'Java'),
    'pcbs-java': ('Parquet original', 'Java'),
    'rg-java': ('Parquet row groups', 'Java'),
    'rg-codec': ('Parquet row groups', 'custom codec'),
}
COLUMNS = [
    ('dataset', 'Dataset'), ('mode', 'Mode'), ('cache_format', 'Cache format'),
    ('wire_path', 'Wire'), ('storage', 'Storage'),
    ('configured_heap_gib', 'Heap cfg GiB'), ('actual_heap_gib', 'Heap actual GiB'),
    ('input_rows', 'Input rows'), ('raw_value_gib', 'Raw value GiB'),
    ('payload_gib', 'Payload GiB'), ('raw_per_payload', 'Raw/payload ×'),
    ('full_disk_gib', 'Full disk GiB'), ('raw_per_disk', 'Raw/disk ×'),
    ('full_disk_checkpoint', 'Disk checkpoint'),
    ('fill', 'Fill s'), ('warmup', 'Warmup s'),
    ('read1', 'Read1 s'), ('read2', 'Read2 s'), ('read3', 'Read3 s'),
    ('median_read_seconds', 'Median s'), ('fill_peak_heap_gib', 'Fill heap GiB'),
    ('read_peak_heap_gib', 'Read heap GiB'), ('outcome_display', 'Outcome'),
    ('failed_tasks', 'Failed tasks'), ('evidence', 'Evidence'),
]
NOTES = [
    'GiB = 2^30 bytes. Raw values are nonnull scalar values plus UTF8 octets; '
    'they exclude offsets, null masks, padding and allocation overhead. Input rows/raw bytes '
    'describe the fixed dataset even when a run fails.',
    'Payload means actual cache-buffer bytes: CPU column buffers for Spark columnar, complete '
    'Parquet records for PCBS. Raw/payload and raw/disk are raw-value bytes divided by those '
    'sizes; a larger factor means fewer encoded bytes for this denominator.',
    'Full disk size/factor requires all16 cache blocks on disk at the named checkpoint. '
    'Partial MEMORY_AND_DISK residency has no whole-cache disk ratio. Disk means actual file '
    'length, including the selected serialization/compression path, not physical IO traffic.',
    'Java/custom codec identify the wire path when serialization occurs; deserialized '
    'MEMORY_AND_DISK cache hits may bypass that wire path.',
    'Times are seconds. Read0 is warmup; Read1–3 are separate measured reads. '
    '"failed:" is an incomplete phase; "unverified:" is elapsed time from an ERROR case '
    'or a negative run without a confirmed post-oracle read marker. '
    'Median is populated only for a PASS with all three completed measured reads.',
    'Heap peaks are observed occupancy, not live set or minimum required heap. Read heap '
    'includes warmup and failed read attempts. GC-log precollection observations augment '
    'the sampler where the verifier recorded them. Comparisons still need matching controls '
    'and repeated/balanced run order.',
]


def number(value):
    return (value if isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= 0 else None)


def divide(numerator, denominator):
    a, b = number(numerator), number(denominator)
    return a / b if a is not None and b is not None and b > 0 else None


def read_json(path, notes, evidence_hashes):
    path = Path(path)
    if not path.is_file():
        return {}
    try:
        content = path.read_bytes()
        evidence_hashes[str(path.resolve())] = hashlib.sha256(content).hexdigest()
        value = json.loads(content)
        if not isinstance(value, dict):
            raise ValueError('expected JSON object')
        return value
    except (ValueError, OSError) as error:
        notes.append(str(path) + ': ' + type(error).__name__ + ': ' + str(error))
        return {}


def summarize(case):
    case = Path(case).resolve()
    notes, evidence_hashes = [], {}
    meta = read_json(case / 'command.json', notes, evidence_hashes)
    result = read_json(case / 'result.json', notes, evidence_hashes)
    manifest = read_json(meta['manifest'], notes, evidence_hashes) if meta.get('manifest') else {}
    oracle = read_json(meta['oracle'], notes, evidence_hashes) if meta.get('oracle') else {}
    outcome = result.get('outcome', 'ERROR' if notes else 'NO_RESULT')
    process = result.get('process', {})
    if not process:
        process = read_json(case / 'process.json', notes, evidence_hashes)
    mode = result.get('mode', meta.get('mode'))
    raw = result.get('raw_value_bytes', oracle.get('raw_value_bytes'))
    rows = result.get('rows', manifest.get('total_rows'))
    payload = result.get('payload_encoded_bytes')
    actual_heap = result.get('actual_max_heap')
    fmt, wire = MODES.get(mode, (None, None))
    phases = result.get('phases', [])
    phase_values = {}
    for phase in phases:
        key = (phase.get('phase'), phase.get('iteration'))
        if key in phase_values:
            phase_values[key] = None
            notes.append('duplicate phase: ' + repr(key))
        else:
            phase_values[key] = phase
    trusted = outcome in ('PASS', 'HEAP_OOM', 'GC_LIMIT_OOM')
    confirmed_reads = set()
    if outcome in ('HEAP_OOM', 'GC_LIMIT_OOM') and (case / 'run.log').is_file():
        # The harness emits read only after its post-timing oracle check succeeds.
        keys = ('rows', 'hash_sums', 'null_counts', 'value_bytes', 'raw_value_bytes', 'schema_json')
        digest = hashlib.sha256()
        with (case / 'run.log').open('rb') as stream:
            for line in stream:
                digest.update(line)
                if line.startswith(b'WORLD_CACHE '):
                    try:
                        event = json.loads(line[len(b'WORLD_CACHE '):])
                    except ValueError:
                        notes.append('unparseable raw marker ignored for read confirmation')
                        continue
                    if (event.get('event') == 'read' and all(key in oracle for key in keys) and
                            all(event.get(key) == oracle[key] for key in keys)):
                        confirmed_reads.add(event.get('iteration'))
        evidence_hashes[str(case / 'run.log')] = digest.hexdigest()

    def phase_time(kind, iteration):
        phase = phase_values.get((kind, iteration))
        if not phase or number(phase.get('seconds')) is None:
            return {'seconds': None, 'status': 'missing'}
        confirmed = trusted and (kind != 'read' or outcome == 'PASS' or iteration in confirmed_reads)
        status = ('complete' if confirmed else 'unverified') if phase.get('success') is True else 'failed'
        return {'seconds': phase['seconds'], 'status': status}

    observations = result.get('phase_observations', [])
    heap_values = {'fill': [], 'read': []}
    for observation in observations:
        kind = observation.get('phase')
        observed = number(observation.get('observed_heap_peak_bytes'))
        if kind in heap_values and observed is not None:
            heap_values[kind].append(observed)
    # A verifier-failure result may lack combined observations; retain sampled-only evidence.
    for kind in heap_values:
        if not heap_values[kind]:
            heap_values[kind] = [phase['sampled_heap_peak'] for phase in phases
                if phase.get('phase') == kind and number(phase.get('sampled_heap_peak')) is not None]
            if heap_values[kind]:
                notes.append(kind + ' heap peak uses sampler only; combined observations absent')
    full_disk = []
    for checkpoint in result.get('storage_summary', []):
        size = number(checkpoint.get('actual_cache_disk_file_bytes'))
        if (checkpoint.get('all_cache_blocks_on_disk') is True and
                checkpoint.get('disk_blocks') == 16 and size is not None and size > 0):
            full_disk.append(checkpoint)
    chosen = full_disk[0] if full_disk else {}
    disk_bytes = chosen.get('actual_cache_disk_file_bytes')
    if len({x['actual_cache_disk_file_bytes'] for x in full_disk}) > 1:
        notes.append('full disk sizes vary; table shows first complete checkpoint')
    timings = {
        'fill': phase_time('fill', 0), 'warmup': phase_time('read', 0),
        'read1': phase_time('read', 1), 'read2': phase_time('read', 2),
        'read3': phase_time('read', 3),
    }
    measured = [timings['read' + str(i)] for i in (1, 2, 3)]
    median = statistics.median([x['seconds'] for x in measured]) if (
        outcome == 'PASS' and all(x['status'] == 'complete' for x in measured)) else None
    if median is not None and number(result.get('median_read_seconds')) is not None:
        if not math.isclose(median, result['median_read_seconds'], rel_tol=1e-12, abs_tol=1e-12):
            notes.append('reported median differs from phase-derived median')
    row = dict(
        case=str(case), evidence=str(case / 'result.json'),
        dataset=manifest.get('dataset_id', result.get('dataset_id')), mode=mode,
        cache_format=fmt, wire_path=wire, storage=result.get('storage_level', meta.get('storage')),
        configured_heap_gib=result.get('heap_gib', meta.get('heap_gib')),
        actual_heap_gib=divide(actual_heap, GIB), actual_max_heap_bytes=actual_heap,
        input_rows=rows, raw_value_bytes=raw, raw_value_gib=divide(raw, GIB),
        payload_encoded_bytes=payload, payload_gib=divide(payload, GIB),
        raw_per_payload=divide(raw, payload), full_disk_bytes=disk_bytes,
        full_disk_gib=divide(disk_bytes, GIB), raw_per_disk=divide(raw, disk_bytes),
        full_disk_checkpoint=(str(chosen.get('phase')) + ':' + str(chosen.get('iteration'))
                              if chosen else None),
        median_read_seconds=median,
        fill_peak_heap_gib=divide(max(heap_values['fill']) if heap_values['fill'] else None, GIB),
        read_peak_heap_gib=divide(max(heap_values['read']) if heap_values['read'] else None, GIB),
        outcome=outcome, outcome_display=outcome + (' (timeout)' if process.get('timeout') else ''),
        failed_tasks=result.get('failed_tasks'), issues=result.get('issues', []),
        notes=notes, source_evidence_sha256=evidence_hashes,
        complete_disk_checkpoints=full_disk,
    )
    row.update(timings)
    return row


def scalar(value):
    if value is None:
        return '—'
    if isinstance(value, dict) and 'status' in value:
        seconds = number(value.get('seconds'))
        if seconds is None:
            return '—'
        text = format(seconds, '.3f')
        return text if value['status'] == 'complete' else value['status'] + ':' + text
    if isinstance(value, float):
        return format(value, '.3f')
    return str(value)


def markdown(rows):
    lines = ['| ' + ' | '.join(label for _, label in COLUMNS) + ' |',
             '| ' + ' | '.join('---' for _ in COLUMNS) + ' |']
    for row in rows:
        cells = []
        for key, _ in COLUMNS:
            value = scalar(row.get(key)).replace('|', '\\|').replace('\n', ' ')
            if key == 'evidence' and Path(row['evidence']).is_file():
                value = '[result](' + row['evidence'] + ')'
            cells.append(value)
        lines.append('| ' + ' | '.join(cells) + ' |')
    lines += ['', *['- ' + note for note in NOTES]]
    for row in rows:
        if row['notes'] or row['issues']:
            lines += ['', str(row['case']) + ': ' + '; '.join(
                list(map(str, row['issues'])) + row['notes'])]
    return '\n'.join(lines) + '\n'


def csv_text(rows):
    stream = io.StringIO()
    writer = csv.writer(stream)
    writer.writerow([label for _, label in COLUMNS])
    for row in rows:
        writer.writerow([scalar(row.get(key)) for key, _ in COLUMNS])
    return stream.getvalue()


def cases_from(paths):
    cases = []
    for value in paths:
        path = Path(value).resolve()
        if path.name == 'result.json':
            candidates = [path.parent]
        elif (path / 'command.json').is_file():
            candidates = [path]
        elif path.is_dir():
            commands = {p.parent for p in path.rglob('command.json')}
            results = {p.parent for p in path.rglob('result.json')}
            # A supervisor's aggregate result at the matrix root is not a benchmark case.
            candidates = sorted(commands | (results - {path})) if commands else sorted(results)
            if not candidates:
                candidates = [path]
        else:
            candidates = [path]
        for case in candidates:
            if case not in cases:
                cases.append(case)
    return cases


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('cases', nargs='+', type=Path,
                        help='case directories, result.json files, or a dedicated matrix root')
    parser.add_argument('--out-prefix', type=Path,
                        help='fresh prefix for .md, .csv, .json snapshots; never overwrites files')
    args = parser.parse_args()
    rows = [summarize(case) for case in cases_from(args.cases)]
    report = dict(research_only=True, notes=NOTES, rows=rows)
    if args.out_prefix:
        paths = [Path(str(args.out_prefix) + suffix) for suffix in ('.md', '.csv', '.json')]
        assert not any(path.exists() for path in paths), 'fresh report prefix required'
        args.out_prefix.parent.mkdir(parents=True, exist_ok=True)
        for path, content in zip(paths, [markdown(rows), csv_text(rows),
                                        json.dumps(report, indent=2) + '\n']):
            with path.open('x') as stream:
                stream.write(content)
    print(markdown(rows), end='')


if __name__ == '__main__':
    main()
