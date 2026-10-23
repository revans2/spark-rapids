# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Compile only the isolated JNI addon; never build or replace libcudf."""
import argparse
import json
import os
from pathlib import Path
import subprocess

from prepare_native import sha

HERE = Path(__file__).resolve().parent


def run(command, log, env=None):
    with log.open('x') as stream:
        subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                       check=True, timeout=180, env=env)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prerequisites', required=True, type=Path)
    parser.add_argument('--dependencies', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    prerequisite = json.loads((args.prerequisites / 'prerequisites.json').read_text())
    dependencies = json.loads((args.dependencies / 'dependencies.json').read_text())
    assert prerequisite['all_chunk_checksums_verified']
    assert prerequisite['cudf_version']['revision'] == '69866c121b4cf2d48b6ef631025292547cde528f'
    library = Path(prerequisite['library'])
    assert sha(library) == prerequisite['library_sha256']
    assert sha(Path(prerequisite['fat_jar'])) == prerequisite['fat_jar_sha256']
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    generated = out / 'generated'
    generated.mkdir()
    source = Path(prerequisite['source_root'])
    deps = {entry['name']: Path(entry['source_root']) for entry in dependencies['dependencies']}
    logger_template = (deps['rapids_logger'] / 'cmake/logger_macros.hpp.in').read_text()
    for name in ['cudf', 'rmm']:
        destination = generated / name
        destination.mkdir()
        (destination / 'version_config.hpp').write_text(
            '#pragma once\n#define ' + name.upper() + '_VERSION_MAJOR 26\n#define ' +
            name.upper() + '_VERSION_MINOR 12\n#define ' + name.upper() + '_VERSION_PATCH 0\n')
        (destination / 'logger_macros.hpp').write_text(logger_template.replace(
            '@_RAPIDS_LOGGER_MACRO_PREFIX@', name.upper()).replace(
            '@_RAPIDS_LOGGER_DEFAULT_LOGGER@', name + '::default_logger()'))
    classes = out / 'classes'
    classes.mkdir()
    commands = []
    java = ['javac', '--release', '11', '-cp', prerequisite['fat_jar'], '-d', str(classes),
            '-h', str(generated), str(HERE / 'ParquetMultiFileReader.java')]
    commands.append(java)
    run(java, out / 'javac.log')
    jar = out / 'pcbs-parquet-multifile.jar'
    package = ['jar', 'cf', str(jar), '-C', str(classes), '.']
    commands.append(package)
    run(package, out / 'jar.log')
    java_home = Path('/usr/lib/jvm/java-17-openjdk')
    includes = [
        generated, source / 'cpp/include', source / 'java/src/main/native/include',
        source / 'java/src/main/native/src', deps['rmm'] / 'cpp/include',
        deps['CCCL'] / 'libcudacxx/include', deps['CCCL'] / 'thrust', deps['CCCL'] / 'cub',
        deps['rapids_logger'] / 'include', deps['nvtx3'] / 'c/include',
        Path('/usr/local/cuda/include'), java_home / 'include', java_home / 'include/linux']
    output_library = out / 'libpcbs_parquet_multifile.so'
    native = ['/opt/rh/gcc-toolset-13/root/usr/bin/g++', '-std=c++20', '-O2', '-shared',
              '-fPIC', '-Wl,-z,defs', '-Wl,-soname,libpcbs_parquet_multifile.so']
    native += ['-I' + str(path) for path in includes]
    native += [str(HERE / 'ParquetMultiFileReader.cpp'), '-L' + str(library.parent),
               '-l:libcudf.so', '-L/usr/local/cuda/lib64', '-lcudart', '-ldl',
               '-Wl,-rpath,/usr/local/cuda/lib64', '-o', str(output_library)]
    commands.append(native)
    (out / 'build-commands.json').write_text(json.dumps(commands, indent=2) + '\n')
    run(native, out / 'native-build.log')
    env = dict(os.environ)
    env['LD_LIBRARY_PATH'] = str(library.parent) + ':' + env.get('LD_LIBRARY_PATH', '')
    inspections = {}
    for name, command in [
            ('dynamic', ['readelf', '-d', str(output_library)]),
            ('undefined', ['nm', '-D', '--undefined-only', '-C', str(output_library)]),
            ('linkage', ['ldd', '-r', str(output_library)])]:
        log = out / (name + '.txt')
        run(command, log, env=env)
        text = log.read_text()
        if name == 'linkage':
            assert 'undefined symbol' not in text and 'not found' not in text, text
            assert str(library) in text and '26.08' not in text, 'wrong linked cuDF'
        inspections[name] = {'path': str(log), 'sha256': sha(log)}
    undefined = (out / 'undefined.txt').read_text()
    assert 'cuda::__4::stream_ref' in undefined and 'resource_ref<' in undefined, undefined
    assert '_RMM_26_08' not in undefined
    cpu_classes = out / 'cpu-check-classes'
    cpu_classes.mkdir()
    cpu_compile = ['javac', '--release', '11', '-cp', prerequisite['fat_jar'] + ':' + str(jar),
                   '-d', str(cpu_classes), str(HERE / 'ParquetMultiFileReaderCpuCheck.java')]
    cpu_run = ['java', '-cp', ':'.join(map(str, [cpu_classes, jar, prerequisite['fat_jar']])),
               'ai.rapids.cudf.ParquetMultiFileReaderCpuCheck']
    commands.extend([cpu_compile, cpu_run])
    run(cpu_compile, out / 'cpu-check-compile.log')
    run(cpu_run, out / 'cpu-check.json')
    cpu_result = json.loads((out / 'cpu-check.json').read_text())
    assert cpu_result['success'] and not cpu_result['native_libraries_loaded']
    (out / 'build-commands.json').write_text(json.dumps(commands, indent=2) + '\n')
    result = {
        'research_only': True, 'success': True, 'gpu_execution_verified': False,
        'cpu_checks': cpu_result,
        'exact_native_dependency_lock': False, 'dependency_assumption': dependencies['assumption'],
        'prerequisites_sha256': sha(args.prerequisites / 'prerequisites.json'),
        'dependencies_sha256': sha(args.dependencies / 'dependencies.json'),
        'native_library': str(output_library), 'native_library_sha256': sha(output_library),
        'java_jar': str(jar), 'java_jar_sha256': sha(jar),
        'runtime_library': str(library), 'runtime_library_sha256': sha(library),
        'commands': commands, 'include_directories': list(map(str, includes)),
        'sources': {str(HERE / name): sha(HERE / name)
                    for name in ['ParquetMultiFileReader.java', 'ParquetMultiFileReader.cpp',
                                 'ParquetMultiFileReaderCpuCheck.java', 'prepare_native.py',
                                 'prepare_dependencies.py', 'build_native.py']},
        'generated_headers': {str(path): sha(path) for path in generated.rglob('*.hpp')},
        'inspections': inspections,
        'remaining_gates': ['one already loaded cuDF instance', 'multi-file GPU correctness',
                            'projection/timestamp/nulls', 'RMM shared pool and limits',
                            'malformed-input cleanup']}
    (out / 'build-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
