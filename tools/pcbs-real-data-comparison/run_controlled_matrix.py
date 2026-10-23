# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Explicit frozen research matrix; serial GPU ownership and fail-closed checkpoints."""
import argparse
import csv
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from canonical_source import digest

HERE = Path(__file__).resolve().parent


def schedule():
    cases = []
    # First question has exactly one downstream target and four reads per cache.
    for storage in ['DISK_ONLY', 'MEMORY_AND_DISK']:
        variants = [('java', 100, 16), ('java', 1, 1),
                    ('codec', 1, 1), ('codec', 100, 16)]
        for repetition in range(3):
            for serializer, reset, period in (variants if repetition % 2 == 0 else variants[::-1]):
                cases.append(dict(stage='serialization', storage=storage, repetition=repetition,
                                  serializer=serializer, reset=reset, period=period,
                                  record_mib=1024, targets='1024'))
    # Separate repeated fills preserve the first question's exact read schedule.
    # All three consumer sizes in each case share one populated cache/RDD id.
    for storage in ['DISK_ONLY', 'MEMORY_AND_DISK']:
        variants = [(size, mode) for size in [64, 128, 256, 512, 1024]
                    for mode in ['java', 'codec']]
        for repetition in range(3):
            for size, serializer in (variants if repetition % 2 == 0 else variants[::-1]):
                cases.append(dict(stage='record-size', storage=storage, repetition=repetition,
                                  serializer=serializer, reset=1 if serializer == 'java' else 100,
                                  period=1 if serializer == 'java' else 16, record_mib=size,
                                  targets='0,256,1024' if repetition % 2 == 0 else '1024,256,0'))
    for index, case in enumerate(cases):
        case['id'] = '{:03d}-{}-{}-{}-r{}-p{}-cap{}-rep{}'.format(
            index, case['stage'], case['storage'], case['serializer'], case['reset'],
            case['period'], case['record_mib'], case['repetition'])
    return cases


def save(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def summary(out, completed):
    fields = ['case', 'stage', 'repetition', 'serializer', 'storage', 'reset', 'period',
              'record_mib', 'consumer_mib', 'outcome', 'fill_s', 'read_median_s',
              'fill_heap_gib', 'read_heap_gib', 'memory_bytes', 'disk_bytes', 'records',
              'gpu_sampler_error', 'process_samples_available']
    with (out / 'results.csv').open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fields, lineterminator='\n')
        writer.writeheader()
        for item in completed:
            case, result = item['case'], item['result']
            fill_peaks = [p['observed_peak_bytes'] for p in result['phase_heap']
                          if p['phase'] == 'fill']
            fill_peak = max(fill_peaks) / 2**30 if fill_peaks else None
            for group, reads in enumerate(result['read_groups']):
                read_peak = max([p['observed_peak_bytes'] for p in result['phase_heap']
                                 if p['phase'] == 'read' and p['iteration'] // 4 == group] or [0])
                checkpoints = [e for e in result['storage'] if e['phase'] == 'read'
                               and e['iteration'] // 4 == group]
                resident = checkpoints[-1] if checkpoints else {}
                writer.writerow(dict(case=case['id'], stage=case['stage'],
                                     repetition=case['repetition'], serializer=case['serializer'],
                                     storage=case['storage'], reset=case['reset'], period=case['period'],
                                     record_mib=case['record_mib'], consumer_mib=reads['consumer_target_mib'],
                                     outcome=result['outcome'], fill_s=result['fill_seconds'],
                                     read_median_s=reads['median_read_seconds'], fill_heap_gib=fill_peak,
                                     read_heap_gib=read_peak / 2**30 if read_peak else None,
                                     memory_bytes=resident.get('memory_bytes'), disk_bytes=resident.get('disk_bytes'),
                                     records=result['records'],
                                     gpu_sampler_error=result.get('gpu_sampling', {}).get('error'),
                                     process_samples_available=result.get('process_sampling', {}).get(
                                         'verified_samples_available', False)))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--bench', required=True, type=Path)
    parser.add_argument('--canonical', required=True, type=Path)
    parser.add_argument('--manifest', required=True, type=Path)
    parser.add_argument('--artifacts', required=True, type=Path)
    parser.add_argument('--extra-jar', action='append', default=[], type=Path)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    artifact = json.loads(args.artifacts.read_text())
    cases = schedule()
    sources = {str(HERE / name): digest(HERE / name) for name in [
        'run_controlled_matrix.py', 'run_controlled.py', 'verify_controlled.py',
        'RealCacheBench.scala', 'PcbsRecordCodec.scala']}
    save(out / 'plan.json', dict(cases=cases, source_hashes=sources,
                                 artifact_manifest_sha256=digest(args.artifacts),
                                 canonical_manifest_sha256=digest(args.manifest),
                                 rules='No tuning. Stop unexpected error. OOM retained as negative result.'))
    frozen_inputs = {str(path.resolve()): digest(path) for path in
                     [args.manifest, args.artifacts, args.bench] + args.extra_jar}
    save(out / 'frozen-inputs.json', frozen_inputs)
    completed = []
    status = dict(state='RUNNING', completed=0, total=len(cases), current=None)
    save(out / 'status.json', status)
    # Advisory sandbox-wide lock for this experiment's GPU runs.
    with (out.parent / 'controlled-gpu.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            for case in cases:
                for path, sha in sources.items():
                    assert digest(path) == sha, 'fixture changed during matrix: ' + path
                for path, sha in frozen_inputs.items():
                    assert digest(path) == sha, 'input changed during matrix: ' + path
                users = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid',
                                                 '--format=csv,noheader,nounits'], universal_newlines=True)
                assert not users.strip(), 'GPU already in use; park rather than contaminate timings'
                assert shutil.disk_usage(out).free > 120 * 2**30, 'insufficient case scratch space'
                case_out = out / case['id']
                variant = artifact['variants']['cap{}-dynamic'.format(case['record_mib'])]
                cmd = [sys.executable, str(HERE / 'run_controlled.py'), '--out', str(case_out),
                       '--bench', str(args.bench.resolve()), '--canonical', str(args.canonical.resolve()),
                       '--manifest', str(args.manifest.resolve()), '--artifacts', str(args.artifacts.resolve()),
                       '--variant', 'cap{}-dynamic'.format(case['record_mib']),
                       '--serializer', case['serializer'], '--reset', str(case['reset']),
                       '--period', str(case['period']), '--storage', case['storage'],
                       '--record-mib', str(case['record_mib']), '--sample-gpu']
                if case['stage'] == 'record-size':
                    cmd += ['--consumer-targets', case['targets']]
                else:
                    cmd += ['--consumer-mib', '1024']
                for path in [variant['plugin'], artifact['fixture']] + list(map(str, args.extra_jar)):
                    cmd += ['--gpu-jar', path]
                status.update(current=case['id'], started_epoch=time.time())
                save(out / 'status.json', status)
                print('START ' + case['id'], flush=True)
                # The launcher owns the JVM process group and its bounded timeout.
                run = subprocess.run(cmd)
                result = json.loads((case_out / 'result.json').read_text())
                completed.append(dict(case=case, result=result))
                save(out / 'results.json', completed)
                summary(out, completed)
                assert run.returncode == 0 and result['outcome'] in ('PASS', 'HEAP_OOM'), result
                assert not result['issues'], result['issues']
                # Reparse persisted raw evidence in a fresh process after the launched verifier.
                audited = subprocess.check_output([sys.executable, str(HERE / 'verify_controlled.py'),
                                                   str(case_out)], universal_newlines=True)
                assert json.loads(audited) == result, 'independent reparse differs'
                # Remove only this completed JVM's disposable local block/spill files.
                scratch = case_out / 'local'
                if scratch.exists():
                    assert scratch.resolve().parent == case_out.resolve()
                    shutil.rmtree(scratch)
                status.update(completed=len(completed), last_outcome=result['outcome'])
                save(out / 'status.json', status)
                print('END ' + case['id'] + ' ' + result['outcome'], flush=True)
            status.update(state='RUNS_COMPLETE', current=None,
                          review='Independent result review remains required')
        except BaseException as error:
            status.update(state='PARKED', error=repr(error))
            save(out / 'status.json', status)
            raise
        finally:
            save(out / 'status.json', status)


if __name__ == '__main__':
    main()
