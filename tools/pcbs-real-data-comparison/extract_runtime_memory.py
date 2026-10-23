# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Post-run extraction of actual JVM heap capacity and timestamp-aligned RSS.

No Spark event logs, source data or artifact hashes are read. A bounded run.log
prefix is inspected only until the REAL_CACHE config marker. Default invocation
requires a non-running matrix; --case-id allows explicit completed cases only.
"""
import argparse
import csv
import json
import math
from pathlib import Path


def runtime_config(path, budget=2*1024*1024):
    if not path.exists():
        return {}, 'run.log missing'
    consumed = 0
    with path.open('rb') as stream:
        while consumed < budget:
            line = stream.readline(budget-consumed)
            if not line:
                break
            consumed += len(line)
            marker = b'REAL_CACHE '
            at = line.find(marker)
            if at >= 0:
                try:
                    value = json.loads(line[at+len(marker):])
                except (ValueError, UnicodeError):
                    continue
                if value.get('event') == 'config':
                    return value, None
    return {}, 'config marker unavailable within bounded %d-byte prefix' % budget


def read_samples(path, selected_pids):
    # No verified JVM identity means every process-residency attribution is unknown.
    # Do not silently accept arbitrary rows from a sampler file.
    if not selected_pids:
        return [], 0, 'verified sampler JVM PIDs unavailable; RSS/HWM attribution unknown'
    valid, rejected = [], 0
    if not path.exists():
        return [], 0, 'process-memory.csv missing'
    with path.open(newline='') as stream:
        for row in csv.DictReader(stream):
            try:
                if row.get('error') or not row.get('pid') or not row.get('VmRSS_KiB'):
                    raise ValueError('invalid sample')
                pid = int(row['pid'])
                if selected_pids and pid not in selected_pids:
                    raise ValueError('not a verified sampler JVM PID')
                sample = {'epoch_ms': float(row['epoch_s'])*1000,
                          'pid': pid, 'rss_bytes': int(row['VmRSS_KiB'])*1024,
                          'hwm_bytes': int(row['VmHWM_KiB'])*1024
                          if row.get('VmHWM_KiB') else None}
                if (not math.isfinite(sample['epoch_ms']) or sample['rss_bytes'] < 0 or
                        (sample['hwm_bytes'] is not None and sample['hwm_bytes'] < 0)):
                    raise ValueError('negative RSS')
                valid.append(sample)
            except (ValueError, TypeError, KeyError):
                rejected += 1
    return valid, rejected, None


def peak(rows, key):
    values = [r[key] for r in rows if r.get(key) is not None]
    return max(values) if values else None


def extract_case(directory, entry):
    result = entry['result']
    config, config_error = runtime_config(directory/'run.log')
    pids = set((result.get('process_sampling') or {}).get('selected_pids') or [])
    samples, rejected, sample_error = read_samples(directory/'process-memory.csv', pids)
    phases = []
    for phase in result.get('phases', []):
        start, end = phase.get('start_epoch_ms'), phase.get('end_epoch_ms')
        rows = [r for r in samples if start <= r['epoch_ms'] < end] if (
            start is not None and end is not None) else []
        phases.append({
            'phase': phase['phase'], 'iteration': phase['iteration'],
            'consumer_target_mib': phase.get('consumer_target_mib'),
            'read_role': ('warmup' if phase['iteration']%4 == 0 else 'measured')
                         if phase['phase']=='read' else None,
            'phase_success': phase.get('success'), 'start_epoch_ms': start,
            'end_epoch_ms': end, 'valid_samples': len(rows),
            'sampled_rss_peak_bytes': peak(rows, 'rss_bytes')})
    consumers = []
    targets = result.get('consumer_targets', [])
    for index, target in enumerate(targets):
        reads = [p for p in phases if p['phase']=='read' and p['iteration']//4==index]
        consumers.append({
            'consumer_target_mib': target,
            'sampled_read_peak_including_warmup_bytes': peak(reads,'sampled_rss_peak_bytes'),
            'sampled_measured_read_peak_bytes': peak(
                [p for p in reads if p['read_role']=='measured'],'sampled_rss_peak_bytes'),
            'sampled_warmup_peak_bytes': peak(
                [p for p in reads if p['read_role']=='warmup'],'sampled_rss_peak_bytes')})
    return {
        'case': entry['case'], 'outcome': result.get('outcome'),
        'source_result': str(directory/'result.json'),
        'heap_capacity_source': str(directory/'run.log')+' REAL_CACHE config.heap_max',
        'actual_jvm_max_heap_bytes': config.get('heap_max'),
        'heap_config_error': config_error,
        'rss_source': str(directory/'process-memory.csv'),
        'rss_sample_error': sample_error, 'valid_samples': len(samples),
        'rejected_samples': rejected, 'sample_pids': sorted({r['pid'] for r in samples}),
        'verified_sampler_pids_available': bool(pids),
        'whole_process_sampled_rss_peak_bytes': peak(samples,'rss_bytes'),
        'whole_process_observed_hwm_bytes': peak(samples,'hwm_bytes'),
        'phases': phases, 'consumers': consumers}


def extract(results_path, case_ids=None):
    root = results_path.parent
    status_path = root/'status.json'
    status = json.loads(status_path.read_text()) if status_path.exists() else {}
    if status.get('state')=='RUNNING' and not case_ids:
        raise ValueError('Whole-matrix extraction is blocked while benchmark timings run')
    entries = json.loads(results_path.read_text())
    wanted = set(case_ids or [])
    available = {e['case']['id'] for e in entries}
    if wanted-available:
        raise ValueError('Requested cases are not completed: '+str(sorted(wanted-available)))
    chosen = [e for e in entries if not wanted or e['case']['id'] in wanted]
    if any(e['case']['id']==status.get('current') and status.get('state')=='RUNNING'
           for e in chosen):
        raise ValueError('Active-case IO prohibited')
    if any(e['result'].get('process',{}).get('exit_code') is None for e in chosen):
        raise ValueError('Case process completion is not recorded')
    return {'source_results': str(results_path),
            'notes': [
                'RSS is sampled total process residency, not native allocation attribution.',
                'Phase timestamps use start-inclusive, end-exclusive intervals.',
                'Unknown/missing observations are null, never zero.',
                'VmHWM is a process-lifetime high-water mark; never attributed to a phase.',
                'Observed peaks of failed/incomplete workloads are not sufficient memory bounds.',
                'Heap capacity is Runtime.maxMemory, distinct from configured -Xmx.',
                'Only completed phase intervals can be aligned; an interrupted phase may be absent.'
            ],
            'cases': [extract_case(root/e['case']['id'],e) for e in chosen]}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('results',type=Path)
    parser.add_argument('--case-id',action='append')
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    report=extract(args.results.resolve(),args.case_id)
    args.out.parent.mkdir(parents=True,exist_ok=True)
    args.out.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'cases':len(report['cases']),'out':str(args.out)}))
