# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Make explicitly reencoded compatibility smoke inputs, never a benchmark substitute."""
import argparse
import json
import os
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from prepare_inputs import counts, footer_sha, key_values, physical_schema, sha


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inputs', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    reports = []
    for dataset in ['tlc2024', 'gharchive2025']:
        original_manifest = args.inputs / dataset / 'manifest.json'
        original = json.loads(original_manifest.read_text())
        assert original['dataset_id'] == dataset and original['partitions'] == 16
        destination = out / dataset
        data = destination / 'data'
        data.mkdir(parents=True, exist_ok=False)
        files = []
        for entry in original['files']:
            source = Path(entry['path'])
            assert source.stat().st_size == entry['bytes']
            assert footer_sha(source) == entry['footer_sha256']
            reader = pq.ParquetFile(source)
            batches = []
            selected = 0
            for batch in reader.iter_batches(batch_size=128, use_threads=False):
                count = min(128 - selected, batch.num_rows)
                batches.append(batch.slice(0, count))
                selected += count
                if selected == 128:
                    break
            assert selected == 128
            expected = pa.Table.from_batches(batches, schema=reader.schema_arrow)
            path = data / ('part-%05d.parquet' % entry['partition'])
            with pq.ParquetWriter(path, expected.schema, compression='snappy') as writer:
                # Preserve original ARROW schema and any additional file key/value metadata.
                writer.add_key_value_metadata(reader.metadata.metadata or {})
                writer.write_table(expected, row_group_size=128)
            actual_reader = pq.ParquetFile(path)
            actual = actual_reader.read()
            assert expected.equals(actual, check_metadata=True), 'smoke values/order/types changed'
            assert key_values(actual_reader.metadata) == key_values(reader.metadata)
            logical_types = []
            for index in range(reader.metadata.num_columns):
                before = reader.schema.column(index)
                after = actual_reader.schema.column(index)
                assert before.physical_type == after.physical_type
                assert before.logical_type.to_json() == after.logical_type.to_json(), \
                    'smoke timestamp/logical inference changed'
                logical_types.append(dict(column=before.path,
                    physical_type=before.physical_type,
                    logical_type=json.loads(before.logical_type.to_json())))
            files.append(dict(partition=entry['partition'], path=str(path), rows=128,
                bytes=path.stat().st_size, sha256=sha(path), footer_sha256=footer_sha(path),
                source_path=str(source), source_sha256=entry['sha256'],
                source_rows=entry['rows'], selected_source_range=[0, 128],
                exact_values_types_nulls_and_order=True, logical_types=logical_types,
                **{key: value for key, value in counts(actual_reader.metadata).items()
                   if key != 'rows'}))
        manifest = dict(research_only=True, smoke_only=True, reencoded=True,
            reencoding='PyArrow19 SNAPPY, first128 source rows of each canonical partition; '
                       'all original file key/value metadata and physical/logical types preserved',
            dataset_id=dataset, partitions=16, total_rows=2048, files=files,
            physical_parquet_schema=physical_schema(pq.read_metadata(files[0]['path'])),
            source_manifest=dict(path=str(original_manifest.resolve()),
                                 sha256=sha(original_manifest)),
            exact_source_vs_smoke_checks='each128-row source prefix compared to all decoded '
                                        'smoke values, schema/nulls/order and original file KV; '
                                        'physical and logical annotations checked per column',
            sources={str(Path(__file__).resolve()): sha(Path(__file__).resolve()),
                     str(Path(__file__).with_name('prepare_inputs.py')):
                         sha(Path(__file__).with_name('prepare_inputs.py'))},
            pyarrow_version=pa.__version__)
        target = destination / 'manifest.json'
        target.write_text(json.dumps(manifest, indent=2) + '\n')
        for entry in files:
            os.chmod(entry['path'], 0o444)
        os.chmod(target, 0o444)
        reports.append(dict(dataset_id=dataset, manifest=str(target), rows=2048,
                            files=16, exact_source_values=True))
    (out / 'result.json').write_text(json.dumps(dict(success=True, gpu_used=False,
        smoke_only=True, reencoded=True, reports=reports), indent=2) + '\n')
    print(json.dumps(reports, indent=2))


if __name__ == '__main__':
    main()
