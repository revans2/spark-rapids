# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Extract the exact benchmark library and pinned cuDF headers for an isolated JNI spike."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import urllib.request
import zipfile
import zlib

CUDF_REVISION = '69866c121b4cf2d48b6ef631025292547cde528f'


def sha(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for data in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            result.update(data)
    return result.hexdigest()


def properties(raw):
    return dict(line.split('=', 1) for line in raw.splitlines()
                if line and not line.startswith('#'))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fat-jar', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    with zipfile.ZipFile(str(args.fat_jar)) as archive:
        version = properties(archive.read('cudf-java-version-info.properties').decode())
        assert version['revision'] == CUDF_REVISION, 'unexpected benchmark cuDF revision'
        chunks = properties(archive.read('amd64/Linux/libcudf.so.chunks.properties').decode())
        library = out / 'libcudf.so'
        total = 0
        with library.open('xb') as target:
            for index in range(int(chunks['chunk.count'])):
                name = '{:05d}'.format(index)
                data = archive.read('amd64/Linux/libcudf.so.chunks/' + name)
                assert '{:08x}'.format(zlib.crc32(data) & 0xffffffff) == chunks[
                    'chunk.' + name + '.crc32'], 'native chunk checksum differs'
                target.write(data)
                total += len(data)
        assert total == int(chunks['library.size'])
    url = 'https://codeload.github.com/NVIDIA/cudf/tar.gz/' + CUDF_REVISION
    package = out / 'cudf-source.tar.gz'
    with urllib.request.urlopen(url, timeout=60) as response, package.open('xb') as target:
        while True:
            block = response.read(1024 * 1024)
            if not block:
                break
            target.write(block)
    sources = out / 'cudf-source'
    sources.mkdir()
    with tarfile.open(str(package)) as archive:
        for member in archive:
            pieces = Path(member.name).parts
            relative = Path(*pieces[1:])
            if not member.isfile() or '..' in relative.parts or relative.is_absolute():
                continue
            name = relative.as_posix()
            if not (name.startswith(('cpp/include/', 'java/src/main/native/include/',
                                     'java/src/main/native/src/', 'cmake/')) or
                    name in ('VERSION', 'RAPIDS_BRANCH', 'dependencies.yaml') or
                    name.startswith('cpp/cmake/')):
                continue
            destination = sources / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.extractfile(member) as source, destination.open('xb') as target:
                target.write(source.read())
    inspections = {}
    for name, command in [
            ('dynamic', ['readelf', '-d', str(library)]),
            ('exports', ['nm', '-D', '--defined-only', '-C', str(library)])]:
        path = out / (name + '.txt')
        with path.open('x') as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=60)
        inspections[name] = {'path': str(path), 'sha256': sha(path)}
    evidence = {'research_only': True, 'fat_jar': str(args.fat_jar.resolve()),
                'fat_jar_sha256': sha(args.fat_jar), 'cudf_version': version,
                'library': str(library), 'library_sha256': sha(library),
                'library_bytes': total, 'all_chunk_checksums_verified': True,
                'source_url': url, 'source_package_sha256': sha(package),
                'source_root': str(sources), 'inspections': inspections,
                'preparation_source_sha256': sha(Path(__file__)),
                'remaining_prerequisites': ['ABI-matched RMM and CCCL headers',
                                             'generated cuDF/RMM header definitions',
                                             'thin addon compile/link and loader verification']}
    (out / 'prerequisites.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps(evidence, indent=2))


if __name__ == '__main__':
    main()
