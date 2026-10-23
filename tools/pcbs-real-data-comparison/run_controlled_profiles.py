#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Separate, bounded diagnostics after the 84 primary timing JVMs finish.

Default is plan-only. --execute runs four representative cases with JFR and
Nsight in eight fresh JVMs. This script never writes the primary timing results.
Use MCP docker_exec to launch; no production or frozen fixture changes.
"""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time

CASES = [
    dict(name='java-1-1-cap1024', serializer='java', reset=1, period=1, record_mib=1024),
    dict(name='codec-1-1-cap1024', serializer='codec', reset=1, period=1, record_mib=1024),
    dict(name='codec-default-cap256', serializer='codec', reset=100, period=16, record_mib=256),
    dict(name='codec-default-cap64', serializer='codec', reset=100, period=16, record_mib=64),
]


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for part in iter(lambda: stream.read(8 * 2**20), b''):
            h.update(part)
    return h.hexdigest()


def save(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2) + '\n')
    tmp.replace(path)


def checked(cmd, output=None):
    result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True)
    if output:
        output.write_text(result.stdout)
    if result.returncode:
        raise RuntimeError('command failed: ' + repr(cmd) + '\n' + result.stdout[-4000:])
    return result.stdout


def frozen(matrix):
    plan = json.loads((matrix / 'plan.json').read_text())
    for path, expected in plan['source_hashes'].items():
        if digest(path) != expected:
            raise RuntimeError('frozen fixture changed: ' + path)
    for path, expected in json.loads((matrix / 'frozen-inputs.json').read_text()).items():
        if digest(path) != expected:
            raise RuntimeError('frozen input changed: ' + path)


def matrix_complete(matrix):
    state = json.loads((matrix / 'status.json').read_text())
    if state.get('state') != 'RUNS_COMPLETE' or state.get('completed') != state.get('total') or state.get('total') != 84:
        raise RuntimeError('primary 84-run timing matrix is not complete')
    results = json.loads((matrix / 'results.json').read_text())
    if len(results) != 84 or any(r['result'].get('outcome') not in ('PASS', 'HEAP_OOM') or
                                r['result'].get('issues') for r in results):
        raise RuntimeError('primary results incomplete or contain unresolved errors')


def profile_evidence(case_out, mode, jfr, nsys):
    profile = json.loads((case_out / 'profile-result.json').read_text())
    if profile.get('profile') != mode or profile.get('error') or profile.get('exit_code') != 0:
        raise RuntimeError('profile launcher failure: ' + repr(profile))
    if mode == 'jfr':
        recording = case_out / 'allocation.jfr'
        text = checked([str(jfr), 'summary', str(recording)], case_out / 'jfr-summary.txt')
        counts = {}
        for line in text.splitlines():
            fields = line.split()
            if len(fields) >= 3 and fields[0].startswith('jdk.') and fields[1].isdigit():
                counts[fields[0]] = int(fields[1])
        allocation_events = sum(counts.get(k, 0) for k in (
            'jdk.ObjectAllocationSample', 'jdk.ObjectAllocationInNewTLAB',
            'jdk.ObjectAllocationOutsideTLAB'))
        if not allocation_events:
            raise RuntimeError('JFR readable but has no sampled allocation events')
        return dict(file=str(recording), bytes=recording.stat().st_size,
                    allocation_event_count=allocation_events, event_counts=counts,
                    interpretation='Statistical allocation samples, not exact total allocation bytes.')
    recording = case_out / 'gpu-trace.nsys-rep'
    database = case_out / 'gpu-trace.sqlite'
    checked([str(nsys), 'export', '--type=sqlite', '--force-overwrite=false',
             '--output=' + str(database), str(recording)], case_out / 'nsys-export.txt')
    with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        counts = {name: db.execute('SELECT count(*) FROM "' + name + '"').fetchone()[0]
                  for name in ['CUPTI_ACTIVITY_KIND_KERNEL', 'CUPTI_ACTIVITY_KIND_RUNTIME',
                               'CUPTI_ACTIVITY_KIND_MEMCPY', 'NVTX_EVENTS'] if name in tables}
    if not counts.get('CUPTI_ACTIVITY_KIND_KERNEL') or not counts.get('CUPTI_ACTIVITY_KIND_RUNTIME'):
        raise RuntimeError('Nsight recording has no CUDA kernel/API evidence: ' + repr(counts))
    return dict(file=str(recording), bytes=recording.stat().st_size, sqlite=str(database),
                activity_counts=counts,
                interpretation='Use raw timeline and REAL_CACHE phase timestamps; timings are profiled.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--matrix', type=Path, required=True)
    parser.add_argument('--template-case', type=Path, required=True,
                        help='Existing full case command.json supplies frozen runtime inputs.')
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    matrix, out = args.matrix.resolve(), args.out.resolve()
    frozen(matrix)
    template = json.loads((args.template_case / 'command.json').read_text())
    if template['rows_per_partition'] != 50331648 or template.get('smoke') or template['profile'] != 'none':
        raise RuntimeError('template must be an unprofiled full96GiB case')
    artifacts = json.loads(Path(template['artifacts']).read_text())
    all_plugins = {v['plugin'] for v in artifacts['variants'].values()}
    fixed_jars = [j['path'] for j in template['gpu_jars'] if j['path'] not in all_plugins]
    launcher = Path(next(p for p in json.loads((matrix / 'plan.json').read_text())['source_hashes']
                         if p.endswith('/run_controlled.py')))
    verifier = launcher.with_name('verify_controlled.py')
    nsys = Path('/opt/nvidia/nsight-systems/current/bin/nsys')
    jfr = Path(shutil.which('java')).resolve().with_name('jfr')
    commands = []
    for case in CASES:
        for mode in ['jfr', 'nsys']:
            name = case['name'] + '-' + mode
            variant = 'cap{}-dynamic'.format(case['record_mib'])
            cmd = [sys.executable, str(launcher), '--out', str(out / name),
                   '--bench', template['bench_jar'], '--canonical',
                   str(Path(json.loads(Path(template['manifest']).read_text())['files'][0]['path']).parent),
                   '--manifest', template['manifest'], '--artifacts', template['artifacts'],
                   '--variant', variant, '--serializer', case['serializer'],
                   '--reset', str(case['reset']), '--period', str(case['period']),
                   '--storage', 'DISK_ONLY', '--record-mib', str(case['record_mib']),
                   '--consumer-mib', '1024', '--profile', mode, '--sample-gpu']
            for jar in [artifacts['variants'][variant]['plugin']] + fixed_jars:
                cmd += ['--gpu-jar', jar]
            commands.append(dict(name=name, profile=mode, case=case, argv=cmd))
    out.mkdir(parents=True, exist_ok=False)
    save(out / 'plan.json', dict(diagnostic_only=True, primary_matrix=str(matrix),
         selection=CASES, maximum_jvms=8, commands=commands, launcher_sha256=digest(launcher),
         controls='96GiB/16parts/local8/48GiB/global1GiB/GPU15GiB/concurrency2/host40GiB/pinned2GiB/DISK_ONLY',
         limits='Selected diagnostic runs only; no heavier consumer, tuning or primary timing CSV edits.'))
    status = dict(state='PLAN_ONLY', completed=0, total=8, current=None)
    save(out / 'status.json', status)
    if not args.execute:
        print('PLAN_ONLY ' + str(out / 'plan.json'))
        return
    completed = []
    try:
        matrix_complete(matrix)
        # Same lock as the timing supervisor, even after its status becomes complete.
        with (matrix.parent / 'controlled-gpu.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            checked([str(jfr), '--version'], out / 'jfr-version.txt')
            checked([str(nsys), '--version'], out / 'nsys-version.txt')
            status['state'] = 'RUNNING'
            for item in commands:
                frozen(matrix)
                users = checked(['nvidia-smi', '--query-compute-apps=pid',
                                 '--format=csv,noheader,nounits'])
                if users.strip():
                    raise RuntimeError('GPU in use; park rather than contaminate diagnostics')
                if shutil.disk_usage(out).free <= 140 * 2**30:
                    raise RuntimeError('less than140GiB free; park before next full case')
                status.update(current=item['name'], started_epoch=time.time())
                save(out / 'status.json', status)
                run = subprocess.run(item['argv'])
                case_out = out / item['name']
                result = json.loads((case_out / 'result.json').read_text())
                if run.returncode or result.get('outcome') != 'PASS' or result.get('issues'):
                    raise RuntimeError('profiled case failed; retain raw evidence: ' + repr(result))
                reparsed = json.loads(checked([sys.executable, str(verifier), str(case_out)]))
                if reparsed != result:
                    raise RuntimeError('repeated reparse with same verifier differs')
                evidence = profile_evidence(case_out, item['profile'], jfr, nsys)
                completed.append(dict(case=item, result=result, profile=evidence))
                save(out / 'diagnostic-results.json', completed)
                # Only verified case-local disposable cache/spill storage; retain all evidence.
                scratch = case_out / 'local'
                if scratch.exists():
                    if scratch.resolve().parent != case_out.resolve():
                        raise RuntimeError('unsafe scratch cleanup path')
                    shutil.rmtree(scratch)
                status.update(completed=len(completed), last_outcome='PASS')
                save(out / 'status.json', status)
            status.update(state='DIAGNOSTICS_COMPLETE', current=None,
                          review='Raw profile interpretation and independent review remain required.')
    except BaseException as error:
        status.update(state='PARKED', error=repr(error))
        raise
    finally:
        save(out / 'status.json', status)


if __name__ == '__main__':
    main()
