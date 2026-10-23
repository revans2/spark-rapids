# Copyright (c) 2026, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations.
"""Synthetic reporting checks; no Spark, GPU, or existing experiment files."""
import copy
import json
from pathlib import Path
import tempfile
import unittest

import summarize_world as report


class SummaryChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.case = self.root / 'case'
        self.case.mkdir()
        self.manifest = self.root / 'input.json'
        self.oracle = self.root / 'oracle.json'
        self.write(self.manifest, {'dataset_id': 'real-data', 'total_rows': 1000})
        self.write(self.oracle, {'raw_value_bytes': 8 * report.GIB})
        self.meta = dict(mode='rg-codec', heap_gib=16, storage='DISK_ONLY',
                         manifest=str(self.manifest), oracle=str(self.oracle))
        self.write(self.case / 'command.json', self.meta)
        phases = [dict(phase='fill', iteration=0, seconds=10, success=True)] + [
            dict(phase='read', iteration=i, seconds=t, success=True)
            for i, t in enumerate([9, 3, 1, 2])]
        observations = [dict(phase='fill', iteration=0,
                             observed_heap_peak_bytes=4 * report.GIB)] + [
            dict(phase='read', iteration=i, observed_heap_peak_bytes=gib * report.GIB)
            for i, gib in enumerate([5, 7, 6, 4])]
        self.result = dict(outcome='PASS', mode='rg-codec', heap_gib=16,
            storage_level='DISK_ONLY', rows=1000, raw_value_bytes=8 * report.GIB,
            actual_max_heap=15 * report.GIB, payload_encoded_bytes=2 * report.GIB,
            phases=phases, phase_observations=observations, median_read_seconds=2,
            storage_summary=[dict(phase='fill', iteration=0,
                all_cache_blocks_on_disk=True, disk_blocks=16,
                actual_cache_disk_file_bytes=2.5 * report.GIB)],
            failed_tasks=0, process=dict(exit_code=0, timeout=False), issues=[])

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def write(path, value):
        path.write_text(json.dumps(value))

    def row(self, value=None):
        self.write(self.case / 'result.json', self.result if value is None else value)
        return report.summarize(self.case)

    def test_complete_sizes_factors_and_times(self):
        row = self.row()
        self.assertEqual((row['configured_heap_gib'], row['actual_heap_gib']), (16, 15))
        self.assertEqual((row['raw_value_gib'], row['payload_gib'], row['raw_per_payload']),
                         (8, 2, 4))
        self.assertEqual((row['full_disk_gib'], row['raw_per_disk']), (2.5, 3.2))
        self.assertEqual(row['median_read_seconds'], 2)
        self.assertEqual([row['read' + str(i)]['seconds'] for i in (1, 2, 3)], [3, 1, 2])
        self.assertEqual((row['fill_peak_heap_gib'], row['read_peak_heap_gib']), (4, 7))
        self.assertTrue(row['source_evidence_sha256'])
        self.assertIn('Raw/payload', report.markdown([row]))

    def test_partial_disk_has_no_whole_cache_ratio(self):
        value = copy.deepcopy(self.result)
        value['storage_level'] = 'MEMORY_AND_DISK'
        value['storage_summary'][0].update(all_cache_blocks_on_disk=False, disk_blocks=7)
        row = self.row(value)
        self.assertIsNone(row['full_disk_gib'])
        self.assertIsNone(row['raw_per_disk'])

    def test_requires_all_sixteen_disk_blocks(self):
        value = copy.deepcopy(self.result)
        value['storage_summary'][0]['disk_blocks'] = 15
        self.assertIsNone(self.row(value)['full_disk_bytes'])

    def test_later_full_checkpoint_is_labeled(self):
        value = copy.deepcopy(self.result)
        value['storage_summary'][0].update(all_cache_blocks_on_disk=False, disk_blocks=7)
        value['storage_summary'].append(dict(phase='read', iteration=1,
            all_cache_blocks_on_disk=True, disk_blocks=16, actual_cache_disk_file_bytes=report.GIB))
        row = self.row(value)
        self.assertEqual(row['full_disk_checkpoint'], 'read:1')
        self.assertEqual(row['raw_per_disk'], 8)

    def test_oom_failed_phase_never_becomes_success_or_median(self):
        value = copy.deepcopy(self.result)
        value.update(outcome='HEAP_OOM', failed_tasks=8,
                     phases=[dict(phase='fill', iteration=0, seconds=12, success=False)])
        row = self.row(value)
        self.assertEqual(row['outcome'], 'HEAP_OOM')
        self.assertEqual(report.scalar(row['fill']), 'failed:12.000')
        self.assertEqual(row['read1']['status'], 'missing')
        self.assertIsNone(row['median_read_seconds'])
        self.assertEqual(row['failed_tasks'], 8)

    def test_oom_completed_body_without_post_oracle_marker_is_not_success(self):
        value = copy.deepcopy(self.result)
        value['outcome'] = 'HEAP_OOM'
        row = self.row(value)
        self.assertEqual(row['warmup']['status'], 'unverified')
        self.assertEqual(row['read1']['status'], 'unverified')
        self.assertIsNone(row['median_read_seconds'])

    def test_oom_prior_read_requires_matching_post_oracle_marker(self):
        oracle = dict(rows=1000, hash_sums=[1, 2, 3, 4], null_counts=[1],
                      value_bytes=[8 * report.GIB], raw_value_bytes=8 * report.GIB,
                      schema_json='{}')
        self.write(self.oracle, oracle)
        marker = dict(oracle, event='read', iteration=1)
        (self.case / 'run.log').write_text('WORLD_CACHE ' + json.dumps(marker) + '\n')
        value = copy.deepcopy(self.result)
        value['outcome'] = 'HEAP_OOM'
        row = self.row(value)
        self.assertEqual(row['read1']['status'], 'complete')
        self.assertEqual(row['read2']['status'], 'unverified')
        self.assertIsNone(row['median_read_seconds'])

    def test_error_elapsed_times_are_unverified(self):
        value = copy.deepcopy(self.result)
        value.update(outcome='ERROR', issues=['wrong answer'])
        row = self.row(value)
        self.assertEqual(report.scalar(row['read1']), 'unverified:3.000')
        self.assertIsNone(row['median_read_seconds'])

    def test_missing_result_retains_declared_input_controls(self):
        row = report.summarize(self.case)
        self.assertEqual(row['outcome'], 'NO_RESULT')
        self.assertEqual(row['input_rows'], 1000)
        self.assertEqual(row['raw_value_gib'], 8)
        self.assertEqual(row['configured_heap_gib'], 16)
        self.assertIsNone(row['actual_heap_gib'])
        self.assertIsNone(row['median_read_seconds'])

    def test_malformed_result_preserves_error_row(self):
        (self.case / 'result.json').write_text('{broken')
        row = report.summarize(self.case)
        self.assertEqual(row['outcome'], 'ERROR')
        self.assertTrue(row['notes'])
        self.assertEqual(row['input_rows'], 1000)

    def test_duplicate_phase_not_silently_selected(self):
        value = copy.deepcopy(self.result)
        value['phases'].append(dict(phase='read', iteration=1, seconds=0.01, success=True))
        row = self.row(value)
        self.assertIsNone(row['median_read_seconds'])
        self.assertEqual(row['read1']['status'], 'missing')
        self.assertTrue(any('duplicate phase' in note for note in row['notes']))

    def test_cpu_format_is_distinct_and_zero_factor_guard(self):
        value = copy.deepcopy(self.result)
        value.update(mode='cpu-default', payload_encoded_bytes=0)
        row = self.row(value)
        self.assertEqual((row['cache_format'], row['wire_path']), ('Spark columnar', 'Java'))
        self.assertIsNone(row['raw_per_payload'])

    def test_root_summary_excluded_and_cases_deduplicated(self):
        self.row()
        self.write(self.root / 'result.json', {'supervisor': True})
        self.assertEqual(report.cases_from([self.root, self.case]), [self.case])

    def test_timeout_does_not_overwrite_verifier_outcome(self):
        value = copy.deepcopy(self.result)
        value.update(outcome='ERROR', process={'timeout': True})
        row = self.row(value)
        self.assertEqual(row['outcome'], 'ERROR')
        self.assertEqual(row['outcome_display'], 'ERROR (timeout)')


if __name__ == '__main__':
    unittest.main()
