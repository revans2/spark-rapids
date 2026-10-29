#!/usr/bin/env python3
# Copyright (c) 2026, NVIDIA CORPORATION.
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
"""Compare measured cache costs using total seconds = population + reads * read time.

This arithmetic model excludes eviction/repopulation and changes in query mix.
It computes no new benchmark measurements. A read is one measured query action.
"""
import argparse
import json
import math
from pathlib import Path


def measurement(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(label + " must be a number")
    if not math.isfinite(value) or value < 0:
        raise ValueError(label + " must be finite and nonnegative")
    return value


def compare(codec, population, read, baseline, baseline_population, baseline_read):
    """Describe the linear cost difference over nonnegative real read counts."""
    intercept = population - baseline_population
    slope = read - baseline_read
    result = {
        "codec": codec,
        "population_s": population,
        "read_s": read,
        "cost_difference_s": {"population": intercept, "per_read": slope},
        "positive_crossover_reads_per_population": None,
        "first_integer_read_after_crossover": None,
    }
    if intercept == 0 and slope == 0:
        result.update(relationship="tie", lower_reuse_winner="tie",
                      higher_reuse_winner="tie", tie_at_reads="all")
    elif slope == 0:
        winner = codec if intercept < 0 else baseline
        result.update(relationship="dominance", lower_reuse_winner=winner,
                      higher_reuse_winner=winner, tie_at_reads=None)
    else:
        crossing = -intercept / slope
        high = codec if slope < 0 else baseline
        if crossing > 0:
            low = baseline if slope < 0 else codec
            # At an integer crossing the costs tie; the next read is strictly cheaper.
            first_strict = math.floor(crossing) + 1
            result.update(
                relationship="crossover", lower_reuse_winner=low,
                higher_reuse_winner=high, tie_at_reads=crossing,
                positive_crossover_reads_per_population=crossing,
                first_integer_read_after_crossover=first_strict)
        else:
            result.update(relationship="dominance", lower_reuse_winner=high,
                          higher_reuse_winner=high,
                          tie_at_reads=0 if crossing == 0 else None)
            if crossing == 0:
                result["first_integer_read_after_crossover"] = 1
    return result


def analyze(data, cohort_name, writer, reader, projection, baseline):
    cohorts = data.get("cohorts")
    if not isinstance(cohorts, list):
        raise ValueError("results must contain a cohorts array")
    matches = [cohort for cohort in cohorts if cohort.get("name") == cohort_name]
    if len(matches) != 1:
        raise ValueError("cohort must identify exactly one entry: " + cohort_name)
    rows = matches[0].get("results")
    if not isinstance(rows, list):
        raise ValueError("cohort must contain a results array")
    key = reader + "_" + projection + "_s"
    selected = {}
    unavailable = []
    for row in rows:
        population = measurement(row["population_s"], "population_s")
        medians = row["normal_medians"]
        if not isinstance(medians, dict):
            raise ValueError("normal_medians must be an object")
        for name, value in medians.items():
            if value is not None:
                measurement(value, name)
        if row["writer"] != writer:
            continue
        codec = row["codec"]
        if not isinstance(codec, str) or not codec:
            raise ValueError("codec must be a nonempty string")
        if codec in selected or codec in unavailable:
            raise ValueError("duplicate writer/codec: " + writer + "/" + codec)
        if medians.get(key) is None:
            unavailable.append(codec)
        else:
            selected[codec] = (population, medians[key])
    if baseline not in selected:
        raise ValueError("baseline has no selected measurement: " + baseline)
    baseline_population, baseline_read = selected[baseline]
    return {
        "cohort": cohort_name, "writer": writer, "reader": reader,
        "projection": projection,
        "model": "total_s = population_s + reuse_reads_per_population * read_s",
        "baseline": {"codec": baseline, "population_s": baseline_population,
                     "read_s": baseline_read},
        "comparisons": [compare(codec, *selected[codec], baseline,
                                baseline_population, baseline_read)
                        for codec in sorted(selected) if codec != baseline],
        "unavailable_codecs": sorted(unavailable),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--cohort", required=True)
    parser.add_argument("--writer", choices=("cpu", "gpu"), required=True)
    parser.add_argument("--reader", choices=("cpu", "gpu"), required=True)
    parser.add_argument("--projection", choices=("full", "id"), required=True)
    parser.add_argument("--baseline", choices=("default", "pcbs"), default="default")
    args = parser.parse_args()
    try:
        result = analyze(json.loads(args.results.read_text()), args.cohort, args.writer,
                         args.reader, args.projection, args.baseline)
        print(json.dumps(result, indent=2, allow_nan=False))
    except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
