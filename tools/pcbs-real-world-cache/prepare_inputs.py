# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Freeze 16 contiguous raw-row-group partitions without decoding or changing values."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import struct
import subprocess
import time

import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
SPARK = Path('/home/roberte/src/spark_3.5.5')


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(2 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def footer_sha(path):
    with path.open('rb') as stream:
        assert stream.read(4) == b'PAR1', 'only unencrypted complete Parquet inputs supported'
        stream.seek(-8, 2)
        trailer = stream.read(8)
        assert trailer[4:] == b'PAR1'
        length = struct.unpack('<I', trailer[:4])[0]
        assert 0 < length <= path.stat().st_size - 12
        stream.seek(-8 - length, 2)
        return hashlib.sha256(stream.read(length + 8)).hexdigest()


def physical_schema(metadata):
    text = str(metadata.schema)
    return '\n'.join(text.splitlines()[1:]) if text.startswith('<pyarrow.') else text


def key_values(metadata):
    return {base64.b64encode(key).decode(): base64.b64encode(value).decode()
            for key, value in sorted((metadata.metadata or {}).items())}


def counts(metadata):
    return {
        'rows': metadata.num_rows, 'row_groups': metadata.num_row_groups,
        'column_compressed_bytes': sum(metadata.row_group(r).column(c).total_compressed_size
            for r in range(metadata.num_row_groups) for c in range(metadata.num_columns)),
        'column_uncompressed_bytes': sum(metadata.row_group(r).column(c).total_uncompressed_size
            for r in range(metadata.num_row_groups) for c in range(metadata.num_columns))}


def group_payload_sha(path, group):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            assert not column.file_path, 'external column chunks are unsupported'
            offsets = [column.data_page_offset]
            if column.has_dictionary_page:
                offsets.append(column.dictionary_page_offset)
            stream.seek(min(offsets))
            remaining = column.total_compressed_size
            while remaining:
                data = stream.read(min(remaining, 2 << 20))
                assert data, 'truncated encoded column chunk'
                digest.update(data)
                remaining -= len(data)
    return digest.hexdigest()


def split_run(groups, parts):
    result = []
    remaining = list(groups)
    for number in range(parts):
        slots = parts - number
        if slots == 1:
            selected = remaining
        else:
            target = sum(g['column_uncompressed_bytes'] for g in remaining) / slots
            selected = [remaining[0]]
            weight = selected[0]['column_uncompressed_bytes']
            while len(selected) < len(remaining) - (slots - 1):
                next_weight = remaining[len(selected)]['column_uncompressed_bytes']
                if abs(weight - target) <= abs(weight + next_weight - target):
                    break
                selected.append(remaining[len(selected)])
                weight += next_weight
        assert selected
        result.append(selected)
        remaining = remaining[len(selected):]
    assert not remaining
    return result


def assignments(groups):
    runs = []
    for group in groups:
        if not runs or runs[-1][-1]['metadata_sha256'] != group['metadata_sha256']:
            runs.append([])
        runs[-1].append(group)
    assert len(runs) <= 16 <= len(groups), 'cannot form 16 nonempty metadata-preserving files'
    parts = [1] * len(runs)
    for _ in range(16 - len(runs)):
        candidates = [i for i, run in enumerate(runs) if parts[i] < len(run)]
        choice = max(candidates, key=lambda i:
                     sum(g['column_uncompressed_bytes'] for g in runs[i]) / parts[i])
        parts[choice] += 1
    output = [piece for run, n in zip(runs, parts) for piece in split_run(run, n)]
    assert [g for piece in output for g in piece] == groups
    return output, len(runs)


def command(argv, log):
    with log.open('x') as stream:
        subprocess.run(argv, stdout=stream, stderr=subprocess.STDOUT, check=True, timeout=1800)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dataset-id', required=True)
    parser.add_argument('--source-list', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--roundtrip-values', action='store_true',
                        help='small CPU gate: compare all decoded values before/after copy')
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    result = {'success': False, 'dataset_id': args.dataset_id, 'gpu_used': False}
    start = time.monotonic()
    try:
        entries = json.loads(args.source_list.read_text())
        paths = [Path(e['path']).resolve() for e in entries]
        assert paths and paths == sorted(set(paths)), 'source list must be unique and sorted'
        expected_by_path = {str(Path(e['path']).resolve()): e for e in entries}
        source_files = []
        source_metadata = []
        groups = []
        schema = None
        for index, path in enumerate(paths):
            original = expected_by_path[str(path)]
            stat = path.stat()
            metadata = pq.read_metadata(path)
            physical = physical_schema(metadata)
            schema = physical if schema is None else schema
            assert physical == schema, 'mixed physical schema: ' + str(path)
            footer = footer_sha(path)
            assert stat.st_size == original.get('file_bytes', stat.st_size)
            assert stat.st_mtime_ns == original.get('mtime_ns', stat.st_mtime_ns)
            assert metadata.num_rows == original.get('rows', metadata.num_rows)
            assert footer == original.get('footer_sha256', footer)
            values = key_values(metadata)
            metadata_hash = hashlib.sha256(json.dumps(values, sort_keys=True).encode()).hexdigest()
            source_files.append(dict(path=str(path), bytes=stat.st_size,
                mtime_ns=stat.st_mtime_ns, sha256=sha(path), footer_sha256=footer,
                metadata_sha256=metadata_hash, key_values_base64=values,
                created_by=metadata.created_by, **counts(metadata)))
            source_metadata.append(metadata)
            for row_group in range(metadata.num_row_groups):
                group = metadata.row_group(row_group)
                groups.append(dict(source_index=index, source_path=str(path),
                    source_row_group=row_group, rows=group.num_rows,
                    column_compressed_bytes=sum(group.column(c).total_compressed_size
                                                for c in range(group.num_columns)),
                    column_uncompressed_bytes=sum(group.column(c).total_uncompressed_size
                                                  for c in range(group.num_columns)),
                    metadata_sha256=metadata_hash))
        pieces, runs = assignments(groups)
        canonical = out / 'data'
        canonical.mkdir()
        plan = dict(sources=list(map(str, paths)), partitions=[
            dict(partition=i, path=str(canonical / ('part-%05d.parquet' % i)), groups=piece)
            for i, piece in enumerate(pieces)])
        (out / 'copy-plan.json').write_text(json.dumps(plan, indent=2) + '\n')
        jars = sorted((SPARK / 'dist/jars').glob('*.jar'))
        cp = ':'.join(map(str, jars))
        classes = out / 'classes'
        classes.mkdir()
        scala_source = HERE / 'PrepareInputs.scala'
        compile_command = ['java', '-Xmx2g', '-cp', cp, 'scala.tools.nsc.Main',
                           '-classpath', cp, '-d', str(classes), str(scala_source)]
        copy_command = ['java', '-Xmx2g', '-cp', str(classes) + ':' + cp,
                        'PrepareInputs', str(out / 'copy-plan.json')]
        command(compile_command, out / 'compile.log')
        command(copy_command, out / 'copy.log')
        assert 'PREPARE_INPUTS COPY PASS' in (out / 'copy.log').read_text()
        files = []
        for partition, piece in zip(plan['partitions'], pieces):
            path = Path(partition['path'])
            metadata = pq.read_metadata(path)
            assert physical_schema(metadata) == schema
            assert key_values(metadata) == key_values(source_metadata[piece[0]['source_index']])
            assert metadata.num_row_groups == len(piece)
            origins = []
            for local_index, original in enumerate(piece):
                group = metadata.row_group(local_index)
                old = source_metadata[original['source_index']].row_group(original['source_row_group'])
                assert group.num_rows == old.num_rows and group.num_columns == old.num_columns
                for c in range(group.num_columns):
                    actual, expected = group.column(c), old.column(c)
                    assert actual.path_in_schema == expected.path_in_schema
                    assert actual.compression == expected.compression
                    assert set(actual.encodings) == set(expected.encodings)
                    assert actual.total_compressed_size == expected.total_compressed_size
                    assert actual.total_uncompressed_size == expected.total_uncompressed_size
                    assert actual.num_values == expected.num_values
                    assert not actual.has_column_index and not actual.has_offset_index
                original_digest = group_payload_sha(Path(original['source_path']), old)
                assert group_payload_sha(path, group) == original_digest, 'encoded bytes changed'
                origins.append(dict(original, encoded_payload_sha256=original_digest))
            files.append(dict(partition=partition['partition'], path=str(path),
                bytes=path.stat().st_size, sha256=sha(path), footer_sha256=footer_sha(path),
                metadata_sha256=piece[0]['metadata_sha256'], origins=origins, **counts(metadata)))
        for source in source_files:
            path = Path(source['path'])
            assert path.stat().st_size == source['bytes']
            assert path.stat().st_mtime_ns == source['mtime_ns']
            assert sha(path) == source['sha256'], 'source changed during preparation'
        totals = {key: sum(f[key] for f in files) for key in
                  ['rows', 'row_groups', 'column_compressed_bytes', 'column_uncompressed_bytes']}
        assert totals == {key: sum(f[key] for f in source_files) for key in totals}
        if args.roundtrip_values:
            import pyarrow as pa
            original_values = pa.concat_tables([pq.ParquetFile(path).read() for path in paths])
            copied_values = pa.concat_tables([pq.ParquetFile(f['path']).read() for f in files])
            assert original_values.equals(copied_values), 'decoded value/order roundtrip failed'
            result['decoded_roundtrip_exact'] = True
        manifest = dict(research_only=True, dataset_id=args.dataset_id, partitions=16,
            total_rows=totals['rows'], totals=totals, physical_parquet_schema=schema,
            physical_schema_sha256=hashlib.sha256(schema.encode()).hexdigest(),
            files=files, source_files=source_files, metadata_runs=runs,
            partition_uncompressed_min=min(f['column_uncompressed_bytes'] for f in files),
            partition_uncompressed_max=max(f['column_uncompressed_bytes'] for f in files),
            assignment='sorted source files, original RG order, contiguous metadata runs; '
                       '16 nonempty chunks approximately balanced by footer uncompressed bytes',
            no_values_decoded_or_reencoded=True,
            page_indexes='column and offset indexes intentionally omitted by Parquet-MR1.13.1 '
                         'appendRowGroups; all benchmark inputs are identical full scans',
            source_list=dict(path=str(args.source_list.resolve()), sha256=sha(args.source_list)),
            sources={str(path): sha(path) for path in [scala_source, Path(__file__).resolve()]},
            parquet_hadoop_jar=dict(path=str(SPARK / 'dist/jars/parquet-hadoop-1.13.1.jar'),
                                    sha256=sha(SPARK / 'dist/jars/parquet-hadoop-1.13.1.jar')),
            footer_hash_definition='serialized footer metadata plus 8-byte length/magic trailer',
            pyarrow_version=__import__('pyarrow').__version__,
            compile_command=compile_command, copy_command=copy_command,
            decoded_roundtrip_checked=args.roundtrip_values)
        (out / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        for file in files:
            os.chmod(file['path'], 0o444)
        os.chmod(out / 'manifest.json', 0o444)
        result.update(success=True, total_rows=totals['rows'], files=16,
                      manifest=str(out / 'manifest.json'), metadata_runs=runs)
    except Exception as error:
        result['error'] = type(error).__name__ + ': ' + str(error)
        raise
    finally:
        result['seconds'] = time.monotonic() - start
        (out / 'preparation-result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
