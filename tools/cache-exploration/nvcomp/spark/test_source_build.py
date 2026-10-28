# Copyright (c) 2026, NVIDIA CORPORATION.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parent
def load(name):
    spec = importlib.util.spec_from_file_location("test_spark_" + name, ROOT / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
builder = load("build")
launcher = load("run")

class SourceProofChecks(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "proof.json"
        self.proof = {"source_root": str(self.root), "source_head": "test-head",
                      "source_clean": True, "command": ["rapids-plugin-mvn", "verify"],
                      "returncode": 0}
        for name in ["build_log", "jni", "private"]:
            path = self.root / name
            path.write_bytes(name.encode())
            self.proof[name] = {"path": str(path), "sha256": builder.digest(path)}
        for name in ["jni", "private"]:
            path = self.root / name
            with zipfile.ZipFile(path, "w") as jar:
                if name == "private":
                    jar.writestr("cudf-spark-private-version-info.properties", b"version=test")
                    jar.writestr("private/Example.class", b"private-class")
                else:
                    jar.writestr("ai/rapids/cudf/Table.class", b"jni-class")
            self.proof[name]["sha256"] = builder.digest(path)
        self.aggregator(True, True)
        self.save()
        self.git = patch.object(builder.subprocess, "check_output",
                                side_effect=lambda command, **kw:
                                "test-head\n" if "rev-parse" in command else "")
        self.git.start()
        self.addCleanup(self.git.stop)
        self.addCleanup(self.temp.cleanup)
    def aggregator(self, reader, reference):
        path = (self.root / "aggregator/target/spark355/"
                "rapids-4-spark-aggregator_2.12-26.12.0-SNAPSHOT-spark355.jar")
        path.parent.mkdir(parents=True, exist_ok=True)
        prefix = "com/nvidia/spark/rapids/parquet/"
        with zipfile.ZipFile(path, "w") as jar:
            jar.writestr("cudf-spark-private-version-info.properties", b"version=test")
            jar.writestr("private/Example.class", b"private-class")
            jar.writestr(prefix + "ParquetCachedBatchSerializer.class",
                         b"ParquetCachedBatchReader" if reference else b"old-reader")
            if reader:
                jar.writestr(prefix + "ParquetCachedBatchReader.class", b"reader")
        self.proof["aggregator"] = {"path": str(path), "sha256": builder.digest(path)}
    def save(self):
        self.path.write_text(json.dumps(self.proof))
    def reject_before_native(self, message):
        with patch("sys.argv", ["build.py", "--output", str(self.root / "output"),
                               "--source-build", str(self.path), "--source-head", "test-head"]), \
                patch.object(builder.subprocess, "run") as native:
            with self.assertRaisesRegex(RuntimeError, message):
                builder.main()
            native.assert_not_called()
        self.assertFalse((self.root / "output").exists())
    def test_valid_proof(self):
        result = builder.validate_source_build(self.path, "test-head")
        self.assertEqual(result["proof"], self.proof)
        self.assertEqual(result["sha256"], builder.digest(self.path))
    def test_failed_build_rejected_before_native(self):
        self.proof["returncode"] = 1
        self.save()
        self.reject_before_native("must have succeeded")
    def test_missing_reader_rejected_before_native(self):
        self.aggregator(False, True)
        self.save()
        self.reject_before_native("missing the current PCBS")
    def test_old_serializer_rejected(self):
        self.aggregator(True, False)
        self.save()
        self.reject_before_native("does not reference")
    def test_changed_log_rejected(self):
        (self.root / "build_log").write_text("changed")
        self.reject_before_native("Changed source build build_log")
    def test_dirty_sources_rejected(self):
        with patch.object(builder.subprocess, "check_output",
                          side_effect=["test-head\n", "test-head\n", " M source.scala\n"]):
            self.reject_before_native("checkout changed")
    def test_wrong_expected_head_rejected(self):
        with patch.object(builder.subprocess, "check_output",
                          side_effect=["other-head", "test-head", ""]):
            self.reject_before_native("does not match the expected source commit")
    def test_wrong_aggregator_path_rejected(self):
        old = Path(self.proof["aggregator"]["path"])
        other = self.root / "old.jar"
        other.write_bytes(old.read_bytes())
        self.proof["aggregator"]["path"] = str(other)
        self.save()
        self.reject_before_native("must be the recorded source worktree")
    def change_aggregator(self, name, value):
        path = Path(self.proof["aggregator"]["path"])
        with zipfile.ZipFile(path) as jar:
            entries = {key: jar.read(key) for key in jar.namelist()}
        entries[name] = value
        with zipfile.ZipFile(path, "w") as jar:
            for key, contents in entries.items():
                jar.writestr(key, contents)
        self.proof["aggregator"]["sha256"] = builder.digest(path)
        self.save()
    def test_private_class_mismatch_rejected(self):
        self.change_aggregator("private/Example.class", b"different-private")
        self.reject_before_native("Pinned private class verification failed")
    def test_private_metadata_mismatch_rejected(self):
        self.change_aggregator("cudf-spark-private-version-info.properties", b"wrong-version")
        self.reject_before_native("private build metadata differs")
    def test_jni_overlap_rejected(self):
        self.change_aggregator("ai/rapids/cudf/Table.class", b"jni-class")
        self.reject_before_native("excluded JNI class/native entries")
    def test_launcher_checks_proof_before_artifacts(self):
        manifest = {"source_build": {"path": str(self.path), "sha256": "incorrect"}}
        with self.assertRaisesRegex(RuntimeError, "Changed source build proof"):
            launcher.verify(manifest, self.root)
    def test_launcher_missing_reader_before_artifacts(self):
        self.aggregator(False, True)
        self.save()
        manifest = {"source_build": {"path": str(self.path),
                    "sha256": builder.digest(self.path), "expected_head": "test-head",
                    "proof": self.proof}}
        with self.assertRaisesRegex(RuntimeError, "missing the current PCBS"):
            launcher.verify(manifest, self.root)

if __name__ == "__main__":
    unittest.main(verbosity=2)
