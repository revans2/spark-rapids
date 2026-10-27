#!/usr/bin/env python3
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

"""Sample an isolated JVM command; sampled residency is not allocation accounting."""
import argparse
import json
import os
import signal
from pathlib import Path
import shutil
import subprocess
import time


def proc_tree(root_pid):
    parents = {}
    for entry in Path('/proc').iterdir():
        if entry.name.isdigit():
            try:
                stat = (entry / 'stat').read_text()
                parents[int(entry.name)] = int(stat[stat.rfind(')') + 2:].split()[1])
            except (OSError, ValueError, IndexError):
                pass
    found = {root_pid}
    while True:
        more = {pid for pid, parent in parents.items() if parent in found}
        if more.issubset(found):
            return found
        found.update(more)


def residency(pids):
    rss = 0
    pss = 0
    rss_seen = False
    pss_complete = True
    for pid in pids:
        try:
            status = Path('/proc', str(pid), 'status').read_text().splitlines()
            value = next(line.split()[1] for line in status if line.startswith('VmRSS:'))
            rss += int(value) * 1024
            rss_seen = True
        except (OSError, ValueError, StopIteration):
            continue
        try:
            rollup = Path('/proc', str(pid), 'smaps_rollup').read_text().splitlines()
            value = next(line.split()[1] for line in rollup if line.startswith('Pss:'))
            pss += int(value) * 1024
        except (OSError, ValueError, StopIteration):
            pss_complete = False
    return {'rss_bytes': rss if rss_seen else None,
            'pss_bytes': pss if rss_seen and pss_complete else None}


def heap_sample(pid):
    """jstat reports G1/parallel heap usage in KiB; unsupported collectors stay unknown."""
    try:
        result = subprocess.run(['jstat', '-gc', str(pid)], capture_output=True,
                                text=True, timeout=2)
        lines = result.stdout.strip().splitlines()
        if result.returncode or len(lines) != 2:
            return {'unavailable': result.stderr.strip() or 'jstat did not return one sample'}
        values = dict(zip(lines[0].split(), lines[1].split()))
        used = sum(float(values[key]) for key in ('S0U', 'S1U', 'EU', 'OU'))
        committed = sum(float(values[key]) for key in ('S0C', 'S1C', 'EC', 'OC'))
        return {'used_bytes': int(used * 1024), 'committed_bytes': int(committed * 1024),
                'young_gc_count': int(values['YGC']), 'full_gc_count': int(values['FGC']),
                'gc_seconds': float(values['GCT'])}
    except (OSError, ValueError, KeyError, subprocess.TimeoutExpired) as error:
        return {'unavailable': str(error)}


def gpu_sample():
    try:
        result = subprocess.run(
            ['nvidia-smi', '--query-compute-apps=pid,used_gpu_memory',
             '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=2)
        if result.returncode:
            return {'unavailable': result.stderr.strip()}
        usages = {}
        for line in result.stdout.splitlines():
            pid, mib = [piece.strip() for piece in line.split(',')]
            if not pid.isdigit() or not mib.isdigit():
                return {'unavailable': 'GPU process query returned unavailable fields',
                        'raw': result.stdout.strip()}
            usages[pid] = usages.get(pid, 0) + int(mib) * 1024 * 1024
        return {'bytes_by_reported_pid': usages}
    except (OSError, ValueError, subprocess.TimeoutExpired) as error:
        return {'unavailable': str(error)}


def peak(samples, path):
    values = []
    for sample in samples:
        value = sample
        for key in path:
            value = value.get(key) if isinstance(value, dict) else None
        if value is not None:
            values.append(value)
    return max(values) if values else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--command-file', type=Path, required=True,
                        help='JSON argv list, normally exported by the harness runner')
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--interval', type=float, default=0.25)
    parser.add_argument('--timeout', type=float, default=300,
                        help='Maximum command duration in seconds')
    parser.add_argument('--heap', action='store_true', help='Sample JVM heap/GC using jstat')
    parser.add_argument('--gpu', action='store_true', help='Sample NVIDIA process residency')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('timeout must be positive')
    if args.interval < 0.05:
        parser.error('interval must be at least 0.05 seconds')
    command = json.loads(args.command_file.read_text())
    if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
        parser.error('command-file must contain a nonempty JSON array of strings')
    args.output_dir.mkdir(parents=True, exist_ok=False)
    (args.output_dir / 'command.json').write_text(json.dumps(command, indent=2) + '\n')
    baseline_gpu = gpu_sample() if args.gpu else None
    samples = []
    started = time.monotonic()
    with (args.output_dir / 'stdout.txt').open('w') as stdout, \
            (args.output_dir / 'stderr.txt').open('w') as stderr, \
            (args.output_dir / 'samples.jsonl').open('w') as log:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            while process.poll() is None:
                if time.monotonic() - started > args.timeout:
                    raise TimeoutError('command exceeded timeout of ' + str(args.timeout) + ' seconds')
                sample_started = time.monotonic()
                pids = proc_tree(process.pid)
                sample = {'elapsed_seconds': sample_started - started, 'pids': sorted(pids),
                          'process_tree': residency(pids)}
                if args.heap:
                    sample['heap'] = heap_sample(process.pid)
                if args.gpu:
                    gpu = gpu_sample()
                    sample['gpu'] = gpu
                    if 'bytes_by_reported_pid' in gpu:
                        reported = gpu['bytes_by_reported_pid']
                        sample['gpu_matching_pid_bytes'] = sum(
                            size for pid, size in reported.items() if int(pid) in pids)
                        sample['gpu_unmatched_pids'] = sorted(
                            pid for pid in reported if int(pid) not in pids)
                sample['collection_seconds'] = time.monotonic() - sample_started
                samples.append(sample)
                log.write(json.dumps(sample) + '\n')
                log.flush()
                remaining = args.interval - sample['collection_seconds']
                if remaining > 0 and process.poll() is None:
                    time.sleep(remaining)
            returncode = process.wait()
        finally:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                finally:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
    unmatched = sorted({pid for sample in samples for pid in sample.get('gpu_unmatched_pids', [])})
    summary = {
        'returncode': returncode, 'elapsed_seconds': time.monotonic() - started,
        'sample_count': len(samples), 'requested_interval_seconds': args.interval,
        'sampled_peak_tree_rss_bytes': peak(samples, ['process_tree', 'rss_bytes']),
        'sampled_peak_tree_pss_bytes': peak(samples, ['process_tree', 'pss_bytes']),
        'sampled_peak_heap_used_bytes': peak(samples, ['heap', 'used_bytes']),
        'sampled_peak_heap_committed_bytes': peak(samples, ['heap', 'committed_bytes']),
        'sampled_peak_matching_pid_gpu_bytes': peak(samples, ['gpu_matching_pid_bytes']),
        'gpu_unmatched_reported_pids': unmatched, 'gpu_baseline': baseline_gpu,
        'jstat_path': shutil.which('jstat') if args.heap else None,
        'scope': 'Whole command including JVM startup; sampled peaks can miss short allocations. '
                 'RSS includes heap, native memory and mappings: do not subtract heap used '
                 'to infer native allocation. GPU PIDs may use a different namespace; '
                 'unmatched entries prevent a per-process attribution claim. Collection '
                 'overhead is included in command wall time. No cache capacity accounting.'}
    (args.output_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))
    raise SystemExit(returncode if returncode >= 0 else 128 - returncode)


if __name__ == '__main__':
    main()
