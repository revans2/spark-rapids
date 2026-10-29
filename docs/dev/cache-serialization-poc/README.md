# Cache serialization proof of concept

Branch `poc/cache-serialization`, upmerged from production
[PR #16633](https://github.com/NVIDIA/cudf-spark/pull/16633) at `f409b860fa`,
including main `49d3be946c`. Validation of the upmerge is in progress.

- [Additional PCBS predicate pruning](issue-pcbs.md)
- [Shared CPU/GPU packed cache](issue-packed-cache.md)
- [Results](results.json) · [Detailed tracker](../nvcomp-cache-exploration.md)

Attach the `figures/*.png` files when posting; SVGs are also provided. Replace
relative links with published branch links or issue attachments.

## Patch scope

Only **`rapids-plugin-4-spark`** needs patches. PCBS changes are in `sql-plugin`
and `tests`; the experimental serializer and native bridges are under
`tools/cache-exploration/nvcomp`. No Spark/cuDF/JNI companion patch is required
for the tested configuration. The stock Spark UTF-8-prefix pruning bug remains
a separate follow-up, guarded locally.

Small-record batching belongs to PR #16633. The POC adds PCBS predicate pruning
and the experimental packed cache; historical Vortex work is outside both issues.
Released PCBS contracts are preserved. PR #16633 and the packed wire format are
unreleased; obsolete experimental formats need no compatibility support.

## Build

The harness has local paths and is not yet portable. Requirements: Java 17,
Python 3.9+, Spark 3.5.5/Scala 2.12, CUDA toolkit, C++20 compiler, nvCOMP 5.0.0.6
and matching cuDF/RMM headers.

| Dependency | Pinned version / variant |
| --- | --- |
| cudf-spark-jni | `26.12.0-20261028.150055-31`, `cuda12` |
| cudf-spark-private_2.12 | `26.12.0-20261028.142847-25`, `spark355` |

The host bridge uses headers and `rmm/src/cuda_stream_view.cpp` from a populated
spark-rapids-jni build tree, linking `libcudf.so` from the pinned artifact. Its
bounds-checked `cudf::unpack` overload is exported by that runtime but absent
from the mounted public header: a pinned POC dependency, not a portable ABI.

Plugin package (`rapids-plugin-mvn` selects the sandbox Java/Maven toolchain):

```text
rapids-plugin-mvn -nsu -Dbuildver=355 \
  -Dcudf-spark-jni.version=26.12.0-20261028.150055-31 \
  -Dcudf-spark-private.version=26.12.0-20261028.142847-25 \
  -DsecondaryCacheDir=/data/tmp/pcbs-upmerge-validation/zinc \
  -pl aggregator -am verify -DskipTests
```

The experimental builder compiles native libraries and a separate JVM jar:

```text
python3.9 tools/cache-exploration/nvcomp/spark/build.py \
  --output /data/tmp/poc-review-new \
  --source-build /data/tmp/poc-production-upmerge-validation/source-build.json \
  --source-head c0941b6bf211ecd002ea92d180bade3fc9882166
```

Use a new output directory. The upmerge proof retains the exact command and jar hashes. It requires an
unchanged clean source checkout,
`source_root`, `source_head`, `source_clean`, `returncode`, build `command`, and
`{path,sha256}` entries for `build_log`, `aggregator`, `jni`, `private`. A portable
proof generator is not yet committed. Probe/benchmark options are in
`nvcomp/spark/run.py --help`; retain the approved budgets recorded in the tracker.

## Calculations and figures

These commands use the committed summary without launching Spark:

```text
python3.9 tools/cache-exploration/cache_reuse.py \
  --results docs/dev/cache-serialization-poc/results.json \
  --cohort 256m --writer cpu --reader cpu --projection full --baseline default

python3.9 tools/cache-exploration/cache_reuse.py \
  --results docs/dev/cache-serialization-poc/results.json \
  --cohort 2g-gpu --writer gpu --reader gpu --projection id --baseline pcbs

python3 tools/cache-exploration/plot_cache_results.py \
  --results docs/dev/cache-serialization-poc/results.json \
  --output docs/dev/cache-serialization-poc/figures
```

Plotting requires matplotlib. Reuse curves model `P + N × R` from measured phases;
crossovers use full precision. `pcbs-pruning.png` shows paired pruning controls;
`pcbs-historical-tradeoffs.png` retains the earlier batching results for reference.

## Verification and provenance

| Evidence | Source |
| --- | --- |
| Build45 numeric timings: prototype | `9fa1d78378cea17caf6f696a37250ed40c158ff6` |
| Build45 numeric timings: plugin | `e8491e4f5684f2b338445b155533cf676e3eed44` |
| Build46 final guard validation | `91a86cdc3c7a7e5a55dcfc965dbe8abb01edcb23` |

The numeric fixture does not exercise the later UTF-8 fixes. Historical build46
passed Spark 3.5.5 Maven verify, twelve PCBS filter tests, Snappy/PCBS predicate
probes and Snappy CPU/GPU interoperability. Tests ran separately from the Maven
command above. Other codecs and statistics/stream cases have earlier coverage
in the tracker.

All 21 current cases were replay-verified at handoff: fourteen 256 MiB/task cases
and seven 2 GiB/task GPU-only cases, zero failed/invalid. `results.json` contains
their summary and replay-output hashes. Full replay requires external logs,
manifests, frozen sources and artifacts under `/data/tmp/cache-predicate-pruning`.
The local Spark binary is instrumented; compression tracing was disabled.

Historical PCBS results come from the Git report `91d748957d`; their original
`/data/pcbs-oom` archive is unavailable. Later CPU lazy-writer cleanup was validated
separately, not retimed in that report.

Cross-version acceptance remains **INCOMPLETE**. The earlier Spark 3.3 attempt
failed on join symbols without a baseline rebuild; current-main validation is
being refreshed after this upmerge. No 3.3/4.x/Databricks success or spill-recovery
validation is claimed. Timing data and reuse curves remain historical; no
post-upmerge performance rerun is implied.
