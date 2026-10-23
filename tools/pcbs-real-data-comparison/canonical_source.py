# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Freeze canonical input evidence after a successful untimed CPU materialization."""
import hashlib
import json
import struct
from pathlib import Path

PREFIX = 'REAL_CACHE '


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def events(path):
    return [json.loads(line[len(PREFIX):]) for line in Path(path).read_text().splitlines()
            if line.startswith(PREFIX)]


def freeze(log, destination):
    data = events(log)
    assert any(e.get('event') == 'complete' and e.get('success') for e in data)
    config = next(e for e in data if e['event'] == 'config')
    assert config['action'] == 'canonical'
    files = [dict(e) for e in data if e['event'] == 'canonical_file']
    assert len(files) == 16 and {e['partition'] for e in files} == set(range(16))
    for entry in files:
        path = Path(entry['path'])
        entry['sha256'] = digest(path)
        entry['mtime_ns'] = path.stat().st_mtime_ns
        with path.open('rb') as stream:
            stream.seek(-8, 2)
            tail = stream.read(8)
            assert tail[4:] == b'PAR1'
            size = struct.unpack('<I', tail[:4])[0]
            stream.seek(-8-size, 2)
            entry['footer_sha256'] = hashlib.sha256(stream.read(size)).hexdigest()
    oracle = next(e for e in data if e['event'] == 'oracle')
    result = {'format': 1, 'generator': 'salted-xxhash64-mask53-double-v1',
              'config': config, 'files': files, 'oracle': oracle,
              'canonical_log_sha256': digest(log),
              'hashing_note': 'Full file hashes computed outside timed benchmark phases.'}
    with Path(destination).open('x') as stream:
        json.dump(result, stream, indent=2)
        stream.write('\n')
    return result


def check(manifest):
    data = json.loads(Path(manifest).read_text())
    assert len(data['files']) == 16
    for entry in data['files']:
        path = Path(entry['path'])
        assert path.stat().st_size == entry['bytes']
        assert path.stat().st_mtime_ns == entry['mtime_ns']
        assert digest(path) == entry['sha256'], str(path)
    return data
