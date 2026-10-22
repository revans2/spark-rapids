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
"""Generate a disposable Spark 3.5.5 source overlay; never mutate installed Spark."""
import argparse
import difflib
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--spark-home", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
args = parser.parse_args()
args.output_dir.mkdir(parents=True, exist_ok=False)
patch = []
for relative in (
    "core/src/main/scala/org/apache/spark/serializer/SerializerManager.scala",
    "sql/core/src/main/scala/org/apache/spark/sql/execution/columnar/InMemoryRelation.scala",
):
    original = (args.spark_home / relative).read_text()
    modified = original
    if relative.endswith("SerializerManager.scala"):
        old = "    if (autoPick && canUseKryo(ct)) {"
        new = """    if (ct.isInstanceOf[CacheCodecClassTag[_]]) {
      ct.asInstanceOf[CacheCodecClassTag[_]].codec
    } else if (autoPick && canUseKryo(ct)) {"""
        assert modified.count(old) == 1
        modified = modified.replace(old, new)
    else:
        old = "    val cached = cb.mapPartitionsInternal { it =>"
        new = """    val cacheTag: scala.reflect.ClassTag[CachedBatch] = serializer match {
      case provider: SqlCacheCodecProvider =>
        new org.apache.spark.serializer.CacheCodecClassTag[CachedBatch](
          scala.reflect.classTag[CachedBatch], provider.cacheCodec(cachedPlan.output))
      case _ => scala.reflect.classTag[CachedBatch]
    }
    val cached = cb.mapPartitionsInternal({ it =>"""
        assert modified.count(old) == 1
        modified = modified.replace(old, new)
        old = "    }.persist(storageLevel)"
        assert modified.count(old) == 1
        modified = modified.replace(old, "    })(cacheTag).persist(storageLevel)")
    target = args.output_dir / Path(relative).name
    target.write_text(modified)
    patch.extend(difflib.unified_diff(original.splitlines(True), modified.splitlines(True),
                                    fromfile="a/" + relative, tofile="b/" + relative))
(args.output_dir / "spark-research.patch").write_text("".join(patch))
