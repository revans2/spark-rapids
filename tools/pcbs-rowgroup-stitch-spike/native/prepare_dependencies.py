# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Pinned research dependency headers, explicitly inferred from native-build date, not a CI lock."""
import argparse
import json
from pathlib import Path
import tarfile
import urllib.request

from prepare_native import sha

# Latest official main commits preceding the native artifact's recorded build timestamp.
RMM = 'ed843de7a1afb4c6e119480ea36d4e5b895cd7f6'
RAPIDS_CMAKE = '037d4c5489cc13f5b378b54197238a6e2cdbff17'


def download(url, destination):
    with urllib.request.urlopen(url, timeout=60) as response, destination.open('xb') as target:
        while True:
            block = response.read(1024 * 1024)
            if not block:
                break
            target.write(block)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    catalog_url = ('https://raw.githubusercontent.com/rapidsai/rapids-cmake/' + RAPIDS_CMAKE +
                   '/rapids-cmake/cpm/versions.json')
    catalog_file = out / 'versions.json'
    download(catalog_url, catalog_file)
    catalog = json.loads(catalog_file.read_text())['packages']
    requested = [('rmm', 'rapidsai/rmm', RMM, None)]
    for name in ['CCCL', 'rapids_logger', 'nvtx3']:
        entry = catalog[name]
        repository, suffix = entry['url'].split('github.com/', 1)[1].split('/archive/', 1)
        requested.append((name, repository, suffix[:-7], entry['url_hash'].split('=', 1)[1]))
    evidence = []
    for name, repository, commit, expected in requested:
        url = 'https://codeload.github.com/' + repository + '/tar.gz/' + commit
        package = out / (name + '.tar.gz')
        download(url, package)
        checksum = sha(package)
        if expected:
            assert checksum == expected, 'pinned package checksum differs: ' + name
        source_root = out / name
        source_root.mkdir()
        with tarfile.open(str(package)) as archive:
            for member in archive:
                relative = Path(*Path(member.name).parts[1:])
                if not member.isfile() or '..' in relative.parts or relative.is_absolute():
                    continue
                destination = source_root / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                with archive.extractfile(member) as source, destination.open('xb') as target:
                    target.write(source.read())
        evidence.append({'name': name, 'repository': repository, 'commit': commit, 'url': url,
                         'sha256': checksum, 'catalog_checksum_verified': bool(expected),
                         'source_root': str(source_root)})
    result = {
        'research_only': True, 'exact_native_dependency_lock': False,
        'assumption': 'RMM and rapids-cmake commits are inferred as latest main before '
                      '2026-10-14T13:47:29Z. Dependency versions are not claimed to be the exact '
                      'native build lock; root authorized this isolated prototype with ABI checks.',
        'rapids_cmake_commit': RAPIDS_CMAKE, 'catalog_url': catalog_url,
        'catalog_sha256': sha(catalog_file), 'dependencies': evidence,
        'preparation_source_sha256': sha(Path(__file__))}
    (out / 'dependencies.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
