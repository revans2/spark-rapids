# Cache serialization proof of concept

Local branch: `poc/cache-serialization`, created from `pcbs-rowgroup-production`
at `77afe5a90f`. Status: **READY FOR REVIEW** for the local exploration and issue
drafts. This is not production acceptance. Only the architect marks work DONE.

## Issue drafts and figures

- [PCBS: smaller stored records, large GPU read batches, and heap pressure](issue-pcbs.md).
- [Packed column cache: CPU fallback and shared CPU/GPU compression](issue-packed-cache.md).
- [Machine-readable measurement summary](results.json).
- [Detailed chronological tracker](../nvcomp-cache-exploration.md).

The drafts disclose AI assistance. Review the complete branch scope and claims
before publishing. No branch push, GitHub issue or pull request is created by
this handoff. When filing, attach the PNGs under `figures/` and replace local
Markdown links with links to the published branch/files or issue attachments.
SVG versions are available for higher-resolution export.

Vortex remains historical exploration elsewhere in this branch. It is excluded
from both issues and from the patch/dependency assessment below. No Vortex
cleanup or rerun is needed for this handoff.

## Repository scope

**One repository needs implementation patches: `rapids-plugin-4-spark`.**

PCBS production changes live in `sql-plugin` and `tests`. The experimental packed
serializer, CPU conversion/codecs, native metadata bridge, codec adapters and
CUDA statistics kernels all live under `tools/cache-exploration/nvcomp`.
`tools/cache-exploration/spark_cache_bench.py` measures real Spark operations.
No Spark, cuDF or spark-rapids-jni companion patch is required for the tested
configuration. The existing Spark invalid-UTF8-prefix statistics bug is a
separate follow-up; both POC serializers have conservative guards.

This is a continuation branch, not a clean patch against current upstream main.
Its inherited base includes unrelated metadata/Delta work, and its history
includes Vortex. Review changes relative to the relevant cache base when
considering production PRs; do not describe the complete main diff as cache-only.
Existing branches/history and unrelated untracked files were preserved. No
production source was restructured as part of the publication cleanup.

Released PCBS record layout, Parquet bytes/reader contracts and existing controls
must remain supported. The packed prototype and its wire format are unreleased;
no compatibility with obsolete experimental formats is promised.

## Building the experiment

This is an evidence-preserving local harness, **not yet a portable checkout-and-run
package**. It uses Java 17, Python 3.9+, Spark 3.5.5/Scala 2.12, CUDA toolkit,
C++20 compiler, nvCOMP 5.0.0.6 SDK and matching cuDF/RMM headers. Existing scripts
contain sandbox paths under `/home/roberte` and `/data/tmp`; adapt those paths for
another environment. No special Java vendor or Rust/Vortex toolchain is required.

The final plugin proof uses downloaded artifacts, not a modified local JNI build:

| Dependency | Pinned version / variant |
| --- | --- |
| cudf-spark-jni | `26.12.0-20261023.150441-28`, `cuda12` |
| cudf-spark-private | `26.12.0-20261023.142435-23`, `spark355` |

The native host bridge consumes cuDF/RMM headers and `rmm/src/cuda_stream_view.cpp`
from a populated spark-rapids-jni build tree. It links `libcudf.so` extracted from
the pinned artifact. It declares a bounds-checked `cudf::unpack` overload exported
by that runtime but absent from the mounted public header. That is a pinned POC
dependency, not a claim of portable public ABI support. Matching headers/build
inputs are needed, but no changes to their repository are proposed.

The final clean plugin build ran `rapids-plugin-mvn` (the sandbox Maven wrapper)
with these arguments; outside the sandbox use the corresponding Maven installation:

```text
-o -nsu -Dbuildver=355
-Dcudf-spark-jni.version=26.12.0-20261023.150441-28
-Dcudf-spark-private.version=26.12.0-20261023.142435-23
-DsecondaryCacheDir=/data/tmp/cache-predicate-pruning/production-build-01/zinc
-pl aggregator -am verify -DskipTests
```

Offline mode assumes dependencies are cached. This builds/packages the plugin;
`-DskipTests` is not a claim that unit tests ran in that command. The twelve PCBS
filter tests and real Spark probes ran separately, as recorded below.

`nvcomp/spark/build.py` requires `--output`, `--source-build` and `--source-head`.
The source-build proof JSON records `source_root`, `source_head`, `source_clean`,
`returncode`, exact `command`, and `{path,sha256}` entries for `build_log`,
`aggregator`, `jni`, `private`. Its clean source checkout and recorded files must
remain available and unchanged. A portable proof-generation helper is not yet
committed. Do not bypass this validation or substitute an unverified prebuilt jar.

For this sandbox, the final corrected build can be reproduced into a NEW output
directory while retaining its frozen source checkout:

```text
python3.9 tools/cache-exploration/nvcomp/spark/build.py \
  --output /data/tmp/nvcomp-spark-probe/build-review-new \
  --source-build /data/tmp/cache-predicate-pruning/production-final/source-build.json \
  --source-head 91a86cdc3c7a7e5a55dcfc965dbe8abb01edcb23
```

The builder compiles the native libraries and experimental JVM jar separately
from production Maven targets. `nvcomp/spark/run.py --help` describes probes and
benchmark launch options. Use a fresh output directory per run, approved memory
settings, serial GPU jobs and correctness prerequisites before larger timings.
All project operations, worker handoffs and resumptions in this environment must
continue to use MCP docker_exec tools; workers must confirm that at checkpoints.

## Reuse calculations and figures

No Spark/GPU work is launched by either command below. The calculation uses full
precision from the summary; display rounding must not decide the first winning
integer reuse count.

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

Plotting additionally requires matplotlib and its dependencies. The CPU curves
model `P + N × R`; they are not measurements of hundreds of reuse actions.
The PCBS figure uses the historical final-policy report and shows both gains
and the TLC regression.

## Evidence boundaries

Numeric measurements use experimental build45: prototype sources
`9fa1d78378cea17caf6f696a37250ed40c158ff6`, production plugin sources
`e8491e4f5684f2b338445b155533cf676e3eed44`. Final invalid-string correctness fixes
are separately validated in build46/source
`91a86cdc3c7a7e5a55dcfc965dbe8abb01edcb23`. The numeric fixture does not exercise
those string predicates; no final-build timing rerun is claimed.

Final source-clean Spark 3.5.5 Maven verify passed, twelve packaged-artifact PCBS
filter tests passed, and final Snappy/PCBS predicate plus Snappy CPU/GPU
interoperability probes passed. Earlier prerequisites cover the other codecs
and stream/statistics edge cases. Full acceptance evidence is in the tracker.

At publication cleanup, both immutable archive replays passed again: fourteen
256 MiB/task cases and seven 2 GiB/task GPU-only cases, zero failed/invalid.
The committed summary was extracted from those fresh verifier outputs. It
includes all writer modes, available CPU/GPU full/projected read medians, payload
sizes, paired predicate controls and whole-job memory peaks. It is a summary,
not a replacement for raw logs, samples, manifests, hardware/runtime identities
and source-bound artifacts retained under `/data/tmp/cache-predicate-pruning`.
The JSON records hashes of the replay outputs; full replay still requires those
external artifacts, frozen source checkouts and Git objects.

The earlier matched PCBS evidence is recovered from a Git report and clearly
labeled historical; its raw archive is not currently mounted. Later CPU lazy
writer/source cleanup has separate validation and is not retimed by that report.
The local Spark binary has instrumentation absent from its clean source tree;
stream tracing is disabled for build45 but it is not a stock-Spark benchmark.

Cross-version acceptance is INCOMPLETE: static HEAD coverage alone is not the
required current-main combination/advisory; upstream fetch and GitHub tooling
were unavailable. Spark 3.3 compile emitted missing join symbols in unchanged
files, with no successful baseline rebuild. No 3.3/4.x/Databricks success is
claimed. Successful bounded runs did not exercise spill recovery. No machine
exhaustion or production-readiness claim follows from this exploration.
