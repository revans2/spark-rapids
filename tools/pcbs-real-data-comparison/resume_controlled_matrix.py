# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Resume the frozen matrix after an orchestration failure; never rerun a recorded case."""
import argparse
import errno
import fcntl
import json
from pathlib import Path
import shutil
import subprocess
import sys
import time

from canonical_source import digest
from run_controlled_matrix import HERE, save, schedule, summary


def fresh_verify(directory):
    raw = subprocess.check_output([sys.executable, str(HERE / 'verify_controlled.py'),
                                   str(directory)], universal_newlines=True)
    return json.loads(raw)


def frozen_checks(plan, inputs):
    for path, expected in dict(list(plan['source_hashes'].items()) + list(inputs.items())).items():
        assert digest(path) == expected, 'frozen source/input changed: ' + path


def ordered_prefix(cases, completed):
    assert len(completed) <= len(cases), 'too many recorded cases'
    ids = [item['case']['id'] for item in completed]
    assert len(ids) == len(set(ids)), 'duplicate completed case'
    assert [item['case'] for item in completed] == cases[:len(completed)], 'non-prefix results'


def quiescence_snapshot(case_out, timeout):
    """Read-only observation; a case reference is not proof of process ownership."""
    needle = str(case_out.resolve()).encode()
    matches = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            command = (proc / 'cmdline').read_bytes()
            if needle not in command:
                continue
            stat = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
        except FileNotFoundError:
            continue
        matches.append({'pid': int(proc.name), 'start_time_ticks': int(stat[19]),
                        'ppid': int(stat[1]), 'process_group': int(stat[2]),
                        'cmdline': [value.decode(errors='replace')
                                    for value in command.rstrip(b'\0').split(b'\0')]})
    users = subprocess.check_output(['nvidia-smi', '--query-compute-apps=pid',
                                     '--format=csv,noheader,nounits'], universal_newlines=True,
                                    timeout=timeout)
    return {'case': case_out.name, 'epoch': time.time(), 'live_case_processes': matches,
            'gpu_compute_pids': [int(value.strip()) for value in users.splitlines() if value.strip()]}


def assert_quiescent(case_out, grace_seconds=30, poll_seconds=1):
    """Wait outside benchmark phases; never kill or disregard a persistent process."""
    assert 0 <= grace_seconds <= 30 and 0 < poll_seconds <= 1
    started = time.monotonic()
    deadline = started + grace_seconds
    observations = []
    while True:
        if observations:
            assert time.monotonic() < deadline, (
                'case/GPU still busy after bounded quiescence wait: ' + json.dumps(observations))
        remaining = max(0.001, deadline - time.monotonic())
        observed = quiescence_snapshot(case_out, min(5, remaining))
        observations.append(observed)
        # OS/probe stalls cannot be preempted here, but a late observation is never accepted.
        assert time.monotonic() <= deadline, (
            'quiescence observation exceeded bounded grace: ' + json.dumps(observations))
        if not observed['live_case_processes'] and not observed['gpu_compute_pids']:
            return dict(observed, observations=observations,
                        waited_seconds=time.monotonic() - started)
        print('QUIESCENCE_WAIT ' + json.dumps(observed), flush=True)
        remaining = deadline - time.monotonic()
        assert remaining > 0, ('case/GPU still busy after bounded quiescence wait: ' +
                               json.dumps(observations))
        time.sleep(min(poll_seconds, remaining))


def cleanup_error(function, path, exception):
    error = exception[1]
    if isinstance(error, FileNotFoundError) and error.errno == errno.ENOENT:
        return
    raise error


def cleanup(case_out):
    proof = assert_quiescent(case_out)
    scratch = case_out / 'local'
    assert not scratch.is_symlink(), 'local scratch must not be a symlink'
    assert scratch.resolve().parent == case_out.resolve(), 'unsafe local scratch path'
    try:
        shutil.rmtree(str(scratch), onerror=cleanup_error)
    except FileNotFoundError as error:
        # Python versions differ in whether root ENOENT reaches onerror.
        if error.errno != errno.ENOENT:
            raise
    assert not scratch.exists(), 'scratch remains after cleanup'
    return proof


def launcher_command(case, out, first, artifact, extras):
    variant = artifact['variants']['cap{}-dynamic'.format(case['record_mib'])]
    cmd = [sys.executable, str(HERE / 'run_controlled.py'), '--out', str(out / case['id']),
           '--bench', first['bench_jar'], '--canonical', first['application_argv'][-2],
           '--manifest', first['manifest'], '--artifacts', first['artifacts'],
           '--variant', 'cap{}-dynamic'.format(case['record_mib']),
           '--serializer', case['serializer'], '--reset', str(case['reset']),
           '--period', str(case['period']), '--storage', case['storage'],
           '--record-mib', str(case['record_mib']), '--sample-gpu']
    if case['stage'] == 'record-size':
        cmd += ['--consumer-targets', case['targets']]
    else:
        cmd += ['--consumer-mib', '1024']
    for path in [variant['plugin'], artifact['fixture']] + extras:
        cmd += ['--gpu-jar', path]
    return cmd


def validate(out):
    plan = json.loads((out / 'plan.json').read_text())
    inputs = json.loads((out / 'frozen-inputs.json').read_text())
    completed = json.loads((out / 'results.json').read_text())
    parked = json.loads((out / 'status.json').read_text())
    assert parked['state'] == 'PARKED', 'only an explicitly parked matrix can be resumed'
    assert plan['cases'] == schedule(), 'frozen schedule differs from original implementation'
    frozen_checks(plan, inputs)
    cases = plan['cases']
    ordered_prefix(cases, completed)
    assert completed, 'no recorded prefix to resume'
    assert parked['completed'] in (len(completed), len(completed)-1), 'status/result gap exceeds one'
    assert parked['current'] == completed[-1]['case']['id'], 'parked case differs from final result'
    first = json.loads((out / cases[0]['id'] / 'command.json').read_text())
    assert first['profile'] == 'none' and first['sample_gpu'] and not first['smoke']
    assert first['rows_per_partition'] == 50331648
    artifact = json.loads(Path(first['artifacts']).read_text())
    assert inputs[first['manifest']] == plan['canonical_manifest_sha256'] == first['manifest_sha256']
    assert inputs[first['artifacts']] == plan['artifact_manifest_sha256'] == first['artifacts_sha256']
    assert inputs[first['bench_jar']] == first['bench_sha256']
    excluded = {artifact['fixture'], artifact['variants'][first['variant']]['plugin']}
    extras = [e['path'] for e in first['gpu_jars'] if e['path'] not in excluded]
    assert all(path in inputs for path in extras), 'unfrozen extra runtime jar'
    evidence = {}
    for index, item in enumerate(completed):
        case = item['case']
        directory = out / case['id']
        meta = json.loads((directory / 'command.json').read_text())
        expected_targets = [int(value) for value in case['targets'].split(',')]
        assert meta['serializer'] == case['serializer'] and meta['reset'] == case['reset']
        assert meta['period'] == case['period'] and meta['storage_level'] == case['storage']
        assert meta['record_mib'] == case['record_mib'] and meta['consumer_targets'] == expected_targets
        assert meta['profile'] == 'none' and meta['sample_gpu'] and not meta['smoke']
        for key in ['bench_jar', 'bench_sha256', 'manifest', 'manifest_sha256',
                    'artifacts', 'artifacts_sha256', 'source_sha256']:
            assert meta[key] == first[key], 'case input provenance differs: ' + key
        assert meta['application_argv'][-2:] == first['application_argv'][-2:]
        selected = artifact['variants']['cap{}-dynamic'.format(case['record_mib'])]
        expected_jars = [selected['plugin'], artifact['fixture']] + extras
        assert [entry['path'] for entry in meta['gpu_jars']] == expected_jars
        variable_keys = {'spark.sql.cache.serializer', 'spark.serializer.objectStreamReset',
                         'spark.storage.unrollMemoryCheckPeriod'}
        def normalized(value, directory, plugin):
            return value.replace(str(directory), '{CASE}').replace(plugin, '{PLUGIN}')
        for key, expected in first['expected_conf'].items():
            if key not in variable_keys:
                actual_conf = meta['expected_conf'][key]
                assert normalized(actual_conf, directory, selected['plugin']) == normalized(
                    expected, out / cases[0]['id'], first['variant_definition']['plugin']), (
                        'fixed case control changed: ' + key)
        actual = fresh_verify(directory)
        assert actual == item['result'], 'fresh raw reparse differs: ' + case['id']
        assert actual['outcome'] in ('PASS', 'HEAP_OOM') and not actual['issues'], actual
        evidence[case['id']] = {str(p.relative_to(directory)): digest(p)
                              for p in directory.rglob('*') if p.is_file()
                              and 'local' not in p.relative_to(directory).parts}
    for case in cases[len(completed):]:
        assert not (out / case['id']).exists(), 'unrecorded case directory; investigate before resume'
    frozen_checks(plan, inputs)
    return plan, inputs, completed, parked, first, artifact, extras, evidence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--matrix', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path,
                        help='Fresh provenance directory, separate from original raw cases')
    parser.add_argument('--validate-only', action='store_true',
                        help='Read-only validation; no cleanup, status writes or GPU launch')
    args = parser.parse_args()
    matrix = args.matrix.resolve()
    provenance = args.out.resolve()
    assert not provenance.exists(), 'fresh resumer provenance directory required'
    with (matrix.parent / 'controlled-gpu.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        plan, inputs, completed, parked, first, artifact, extras, evidence = validate(matrix)
        proof = assert_quiescent(matrix / completed[-1]['case']['id'])
        if args.validate_only:
            print(json.dumps({'valid': True, 'recorded_cases': len(completed),
                              'next_case': (plan['cases'][len(completed)]['id']
                                            if len(completed) < len(plan['cases']) else None),
                              'total': len(plan['cases']), 'quiescence': proof}, indent=2))
            return
        provenance.mkdir(parents=True, exist_ok=False)
        self_hash = digest(Path(__file__))
        plan_hash = digest(matrix / 'plan.json')
        save(provenance / 'resume-supervisor.json', {
            'original_plan_sha256': digest(matrix / 'plan.json'),
            'original_frozen_inputs_sha256': digest(matrix / 'frozen-inputs.json'),
            'original_parked_status': parked, 'original_prefix_length': len(completed),
            'resumer_path': str(Path(__file__).resolve()), 'resumer_sha256': self_hash,
            'prefix_raw_hashes': evidence, 'quiescence': proof,
            'rules': 'Original schedule/controls unchanged. ENOENT-only idempotent scratch cleanup.'})
        for name in ['status.json', 'results.json', 'results.csv']:
            shutil.copyfile(str(matrix / name), str(provenance / ('original-' + name)))
        status = dict(parked, state='RUNNING', completed=len(completed), current=None,
                      resumed_from=str(provenance))
        status.pop('error', None)
        cleanup_proofs = []
        try:
            cleanup_proofs.append(cleanup(matrix / completed[-1]['case']['id']))
            save(provenance / 'cleanup-proofs.json', cleanup_proofs)
            save(matrix / 'status.json', status)
            for case in plan['cases'][len(completed):]:
                assert digest(Path(__file__)) == self_hash, 'resumer changed during run'
                assert digest(matrix / 'plan.json') == plan_hash, 'original plan changed during run'
                frozen_checks(plan, inputs)
                assert_quiescent(matrix / case['id'])
                assert shutil.disk_usage(str(matrix)).free > 120 * 2**30, 'insufficient scratch space'
                cmd = launcher_command(case, matrix, first, artifact, extras)
                status.update(current=case['id'], started_epoch=time.time())
                save(matrix / 'status.json', status)
                print('START ' + case['id'], flush=True)
                process = subprocess.run(cmd)
                result = json.loads((matrix / case['id'] / 'result.json').read_text())
                completed.append({'case': case, 'result': result})
                save(matrix / 'results.json', completed)
                summary(matrix, completed)
                assert process.returncode == 0 and result['outcome'] in ('PASS', 'HEAP_OOM'), result
                assert not result['issues'], result['issues']
                assert fresh_verify(matrix / case['id']) == result, 'fresh raw reparse differs'
                cleanup_proofs.append(cleanup(matrix / case['id']))
                save(provenance / 'cleanup-proofs.json', cleanup_proofs)
                status.update(completed=len(completed), last_outcome=result['outcome'])
                save(matrix / 'status.json', status)
                print('END ' + case['id'] + ' ' + result['outcome'], flush=True)
            status.update(state='RUNS_COMPLETE', current=None,
                          review='Independent result review remains required')
        except BaseException as error:
            status.update(state='PARKED', error=repr(error))
            raise
        finally:
            save(matrix / 'status.json', status)


if __name__ == '__main__':
    main()
