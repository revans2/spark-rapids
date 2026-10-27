# First Spark comparison: measured phases and computed combinations

Source: six `/data/tmp/cache-first-comparison/<format>-<producer>/result.json` files.
All cases use the original **strings-wide** fixture: 524,288 rows and two source
partitions; these are separate from the newer fixed-wide/task-size experiments.

Times below are milliseconds. Population is the single measured cache materialization.
**First** is the first read for that requested consumer mode and projection
(the recorded warmup, iteration -1). **Warmed** is the median of the two following
measured reads (iterations 0 and 1).

Neither first-read column is a globally cold/JVM-cold result. CPU consumers run
before GPU consumers on the same cache; narrow reads also follow full reads.
Default's requested GPU mode still uses a CPU cache reader; PCBS and Vortex use
direct GPU cache readers. GPU-enabled downstream aggregation remains part of the
read-only query timing. Vortex CPU output is row-based in this prototype.

The final two columns are **computed sums of measured phases**, not independently
measured end-to-end runs. They model one population plus the indicated read; prior
queries and initialization differ, so the sums must retain this qualification.

## Full read (all cached columns)

| Format / producer | Requested consumer (cache reader) | Population | First read | Warmed read median | Computed population + first | Computed population + warmed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 775.594 | 257.674 | 185.129 | 1033.268 | 960.723 |
| Default / CPU | GPU (CPU) | 775.594 | 766.248 | 258.774 | 1541.842 | 1034.368 |
| Default / GPU | CPU (CPU) | 2245.839 | 196.732 | 146.216 | 2442.571 | 2392.055 |
| Default / GPU | GPU (CPU) | 2245.839 | 324.365 | 250.506 | 2570.204 | 2496.345 |
| PCBS / CPU | CPU (CPU) | 1157.194 | 266.009 | 224.079 | 1423.203 | 1381.273 |
| PCBS / CPU | GPU (GPU) | 1157.194 | 663.607 | 204.775 | 1820.801 | 1361.968 |
| PCBS / GPU | CPU (CPU) | 976.781 | 621.971 | 246.615 | 1598.752 | 1223.396 |
| PCBS / GPU | GPU (GPU) | 976.781 | 302.184 | 198.712 | 1278.964 | 1175.492 |
| Vortex / CPU | CPU (CPU) | 1221.914 | 387.805 | 248.621 | 1609.718 | 1470.535 |
| Vortex / CPU | GPU (GPU) | 1221.914 | 1362.942 | 815.468 | 2584.856 | 2037.382 |
| Vortex / GPU | CPU (CPU) | 1318.547 | 486.564 | 236.199 | 1805.110 | 1554.746 |
| Vortex / GPU | GPU (GPU) | 1318.547 | 896.763 | 871.027 | 2215.310 | 2189.574 |

## Narrow projection (ID only)

| Format / producer | Requested consumer (cache reader) | Population | First read | Warmed read median | Computed population + first | Computed population + warmed |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Default / CPU | CPU (CPU) | 775.594 | 127.687 | 80.392 | 903.281 | 855.986 |
| Default / CPU | GPU (CPU) | 775.594 | 178.628 | 134.645 | 954.222 | 910.239 |
| Default / GPU | CPU (CPU) | 2245.839 | 99.634 | 63.480 | 2345.473 | 2309.319 |
| Default / GPU | GPU (CPU) | 2245.839 | 159.639 | 111.325 | 2405.478 | 2357.164 |
| PCBS / CPU | CPU (CPU) | 1157.194 | 159.429 | 125.438 | 1316.622 | 1282.632 |
| PCBS / CPU | GPU (GPU) | 1157.194 | 133.736 | 127.193 | 1290.930 | 1284.387 |
| PCBS / GPU | CPU (CPU) | 976.781 | 178.262 | 137.217 | 1155.043 | 1113.997 |
| PCBS / GPU | GPU (GPU) | 976.781 | 125.424 | 133.467 | 1102.205 | 1110.248 |
| Vortex / CPU | CPU (CPU) | 1221.914 | 117.708 | 81.638 | 1339.622 | 1303.552 |
| Vortex / CPU | GPU (GPU) | 1221.914 | 144.403 | 126.606 | 1366.317 | 1348.520 |
| Vortex / GPU | CPU (CPU) | 1318.547 | 142.399 | 86.632 | 1460.946 | 1405.178 |
| Vortex / GPU | GPU (GPU) | 1318.547 | 148.126 | 117.781 | 1466.673 | 1436.327 |

Verification: independently parsed all six source JSON files, checked the shared
fixture/row/partition settings, all 72 recorded read entries, CPU-before-GPU order,
one first/warmup plus two measured samples for every consumer/projection cell,
and every reader backend against its recorded executed plan. Computed medians and additions from original
nanoseconds before rounding. No benchmarks, builds, implementation edits, or commits
were performed; all file access used MCP docker_exec.
