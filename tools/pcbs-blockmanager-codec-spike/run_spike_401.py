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
"""Run the shared heap-codec proof against exact Spark 4.0.1 sources and binaries.

Only generated source copies are adapted. Neither installed Spark nor the shared
3.5.5 fixture is modified. This is a research adapter, not a supported Spark API.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys

REVISION = "29434ea766b0fc3c3bf6eaadb43a8f931133649e"
RELATIVES = (
    "core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala",
    "sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala",
)
SCALA_FILES = (
    "BlockManagerCodecSpike.scala", "CacheCodecContext.scala",
    "SqlCacheCodecProvider.scala", "StorageRouteChecks.scala",
)

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--spark-home", required=True, type=Path)
parser.add_argument("--source-git", required=True, type=Path)
parser.add_argument("--output-dir", required=True, type=Path)
args = parser.parse_args()
args.spark_home = args.spark_home.resolve()
args.source_git = args.source_git.resolve()
args.output_dir = args.output_dir.resolve()
args.output_dir.mkdir(parents=True, exist_ok=False)
source = Path(__file__).resolve().parent
fixture = args.output_dir / "fixture"
fixture.mkdir()
spark = args.output_dir / "spark"
spark.mkdir()
for child in args.spark_home.iterdir():
    if child.name not in {"core", "sql", "work"}:
        (spark / child.name).symlink_to(child, target_is_directory=child.is_dir())
for relative in RELATIVES:
    content = subprocess.check_output(
        ["git", "show", REVISION + ":" + relative], cwd=args.source_git)
    destination = spark / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
for name in SCALA_FILES + ("prepare_overlay.py", "run_spike.py"):
    shutil.copyfile(source / name, fixture / name)

# The ordinary SQL wrapper gained its logicalPlan constructor argument in 4.0.
path = fixture / "BlockManagerCodecSpike.scala"
original = path.read_text()
old = "StorageLevel.DISK_ONLY, spark.range(100).queryExecution.executedPlan, None)"
new = ("StorageLevel.DISK_ONLY, spark.range(100).queryExecution.executedPlan, None,\n"
       "      spark.range(100).queryExecution.logical)")
assert original.count(old) == 1
modified = original.replace(old, new)
old_import = "import org.apache.spark.sql.SparkSession"
assert modified.count(old_import) == 1
modified = modified.replace(old_import, "import org.apache.spark.sql.classic.SparkSession")
path.write_text(modified)

# Reuse all checks, selecting the installed Scala 2.13 distribution and version.
path = fixture / "run_spike.py"
original = path.read_text()
old = 'a.spark_home / "assembly/target/scala-2.12/jars"'
assert original.count(old) == 1
modified = original.replace(old, 'a.spark_home / "jars"')
assert modified.count('versions == {"3.5.5"}') == 1
modified = modified.replace('versions == {"3.5.5"}', 'versions == {"4.0.1"}')
modified = modified.replace('"newer Spark execution"', '"Spark newer than 4.0.1 execution"')
path.write_text(modified)

sha = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
manifest = {
    "spark_revision": REVISION,
    "runtime_home": str(args.spark_home),
    "source_git": str(args.source_git),
    "original_sources": {relative: sha(spark / relative) for relative in RELATIVES},
    "shared_inputs": {name: sha(source / name)
                      for name in SCALA_FILES + ("prepare_overlay.py", "run_spike.py")},
    "adapted_inputs": {path.name: sha(path) for path in fixture.iterdir()},
    "adapter_sha256": sha(Path(__file__)),
    "changes": [
        "Use exact Spark 4.0.1 original overlay sources.",
        "Add logicalPlan to generated default CachedRDDBuilder test constructor.",
        "Use classic SparkSession for Spark 4 SQL CacheManager Dataset API.",
        "Use installed Scala 2.13 jars and require Spark 4.0.1 event-log version.",
    ],
    "spark_off_heap_used": False,
}
(args.output_dir / "adapter-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
command = [sys.executable, str(fixture / "run_spike.py"),
           "--spark-home", str(spark), "--output-dir", str(args.output_dir / "run")]
status = subprocess.run(command)
raise SystemExit(status.returncode)
