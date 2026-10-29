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

"""Render issue attachments from the committed cache measurement summary."""

import argparse
import hashlib
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


CODECS = ("default", "pcbs", "uncompressed", "snappy", "lz4", "zstd")
LABELS = ("Spark default", "PCBS", "Uncompressed", "Snappy", "LZ4", "Zstd")
COLORS = ("#333333", "#D55E00", "#999999", "#0072B2", "#009E73", "#CC79A7")


def save_figure(fig, output, name, description):
    for extension in ("svg", "png"):
        metadata = {"Description": description}
        if extension == "svg":
            metadata["Date"] = None
        destination = output / (name + "." + extension)
        fig.savefig(destination, dpi=160, facecolor="white", metadata=metadata)
        if extension == "svg":
            # Normalize generated path lines for repository whitespace checks.
            destination.write_text("\n".join(
                line.rstrip() for line in destination.read_text().splitlines()) + "\n")
    plt.close(fig)


def plot_cpu_reuse(summary, output):
    cohort = next(item for item in summary["cohorts"] if item["name"] == "256m")
    rows = {row["codec"]: row for row in cohort["results"] if row["writer"] == "cpu"}
    assert set(CODECS).issubset(rows)
    assert all(rows[codec]["task_mib"] == 256
               and rows[codec]["logical_total_mib"] == 1024 for codec in CODECS)
    fig, axes = plt.subplots(1, 2, figsize=(14, 7.5))
    crossings = {}
    for ax, width, maximum, title in zip(
            axes, ("full", "id"), (50, 550),
            ("Read all 32 cached columns", "Read ID column only")):
        reads = np.arange(maximum + 1)
        for codec, label, color in zip(CODECS, LABELS, COLORS):
            row = rows[codec]
            population = row["population_s"]
            read = row["normal_medians"]["cpu_" + width + "_s"]
            totals = population + reads * read
            assert totals[0] == population
            np.testing.assert_allclose(totals[-1], population + maximum * read)
            ax.plot(reads, totals, label=label, color=color, linewidth=2.2,
                    linestyle="--" if codec == "uncompressed" else "-")
        default = rows["default"]
        snappy = rows["snappy"]
        crossing = ((default["population_s"] - snappy["population_s"])
                    / (snappy["normal_medians"]["cpu_" + width + "_s"]
                       - default["normal_medians"]["cpu_" + width + "_s"]))
        crossings[width] = crossing
        time = default["population_s"] + crossing * default[
            "normal_medians"]["cpu_" + width + "_s"]
        ax.scatter([crossing], [time], color=COLORS[3], s=42, zorder=5)
        ax.annotate("Snappy / default crossover\n≈ " + format(crossing, ".1f") + " reads",
                    (crossing, time), xytext=(18, -52), textcoords="offset points",
                    fontsize=11, arrowprops={"arrowstyle": "->", "color": "#555555"},
                    bbox={"boxstyle": "round,pad=0.35", "fc": "white", "ec": "#cccccc"})
        ax.set(title=title, xlabel="Number of cache reads after population",
               ylabel="Modeled total time (seconds; logarithmic scale)",
               xlim=(0, maximum), yscale="log")
        ax.grid(True, which="major", alpha=0.22)
        ax.spines[["top", "right"]].set_visible(False)
    fig.suptitle("CPU cache population savings versus repeated-read cost",
                 fontsize=19, y=0.98)
    fig.text(0.5, 0.918,
             "Modeled total = measured population + N × measured median read; "
             "these totals were not benchmarked",
             ha="center", fontsize=12)
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="lower center", bbox_to_anchor=(0.5, 0.09),
               ncol=6, frameon=False, fontsize=11)
    fig.text(0.5, 0.035,
             "CPU writer + CPU reader • 256 MiB/task × 4 tasks (1 GiB total), 2 task slots\n"
             "Build 45, Spark 3.5.5 POC cohort • 1 warmup + 5 read samples; "
             "source: committed results.json",
             ha="center", fontsize=10.5, linespacing=1.5)
    fig.subplots_adjust(top=0.83, bottom=0.24, left=0.075, right=0.98, wspace=0.23)
    save_figure(fig, output, "cpu-reuse-cost",
                "Modeled CPU total cost from measured population and median reads.")
    return crossings


def plot_pcbs_history(summary, output):
    history = summary["historical_pcbs"]
    rows = history["results"]
    assert [(row["dataset"], row["outer_compression"]) for row in rows] == [
        ("GHArchive", "OFF"), ("GHArchive", "LZ4"),
        ("GHArchive", "Zstd"), ("TLC", "OFF")]
    positions = np.arange(len(rows))
    labels = [row["dataset"] + "\nOuter " + row["outer_compression"] for row in rows]
    fig, axes = plt.subplots(1, 2, figsize=(14, 7.5))
    for ax, field, title, unit in zip(
            axes, ("observed_heap_gib", "read_s"),
            ("Observed heap occupancy", "Median cache read time"),
            ("GiB", "Seconds")):
        for shift, arm, label, color in (
                (-0.19, "before", "Original PCBS", "#777777"),
                (0.19, "after", "8 MiB / default row limit", "#0072B2")):
            values = np.array([row[arm][field] for row in rows])
            bars = ax.bar(positions + shift, values, width=0.36, color=color, label=label)
            np.testing.assert_allclose([bar.get_height() for bar in bars], values)
            ax.bar_label(bars, labels=[format(value, ".2f") for value in values],
                         padding=4, fontsize=10.5)
        ax.set(title=title, ylabel=unit, xticks=positions, xticklabels=labels)
        ax.set_ylim(0, ax.get_ylim()[1] * 1.14)
        ax.grid(axis="y", alpha=0.2)
        ax.set_axisbelow(True)
        ax.spines[["top", "right"]].set_visible(False)
    regression = ((rows[-1]["after"]["read_s"] / rows[-1]["before"]["read_s"] - 1) * 100)
    axes[1].annotate("TLC read regression\n+" + format(regression, ".1f") + "%",
                     (3.19, rows[-1]["after"]["read_s"]), xytext=(2.8, 12.1),
                     ha="center", fontsize=11, color="#A23214",
                     arrowprops={"arrowstyle": "->", "color": "#A23214"})
    fig.suptitle("PCBS: smaller records reduce observed heap pressure; tradeoffs remain",
                 fontsize=18, y=0.98)
    fig.text(0.5, 0.917,
             "Matched historical final-policy comparison • baseline 60c28e44f0 → "
             "selected candidate d9ba8787f6",
             ha="center", fontsize=11.5)
    handles, legend_labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, legend_labels, loc="lower center", bbox_to_anchor=(0.5, 0.135),
               ncol=2, frameon=False, fontsize=11)
    fig.text(0.5, 0.034,
             "Spark 3.5.5 • 48 GiB heap, DISK_ONLY, 16 partitions, 1 GiB GPU target; "
             "inner Snappy, outer codec as labeled\n"
             "One JVM/cell; separate warmup + 3 reads. Heap includes garbage; "
             "not a live set or minimum requirement.\n"
             "Historical Git report 91d748957d • raw archive unavailable at handoff; "
             "not freshly replayed",
             ha="center", fontsize=10.5, linespacing=1.45)
    fig.subplots_adjust(top=0.83, bottom=0.29, left=0.065, right=0.98, wspace=0.2)
    save_figure(fig, output, "pcbs-historical-tradeoffs",
                "Historical PCBS final-policy before/after results; raw archive unavailable.")


def main():
    root = Path(__file__).resolve().parents[2]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results", type=Path,
                        default=root / "docs/dev/cache-serialization-poc/results.json")
    parser.add_argument("--output", type=Path,
                        default=root / "docs/dev/cache-serialization-poc/figures")
    args = parser.parse_args()
    raw = args.results.read_bytes()
    summary = json.loads(raw)
    args.output.mkdir(parents=True, exist_ok=True)
    matplotlib.rcParams.update({
        "font.family": "DejaVu Sans", "font.size": 11,
        "axes.titlesize": 14, "axes.labelsize": 11,
        "svg.hashsalt": "cache-serialization-poc",
    })
    crossings = plot_cpu_reuse(summary, args.output)
    plot_pcbs_history(summary, args.output)
    print(json.dumps({
        "matplotlib": matplotlib.__version__,
        "results_sha256": hashlib.sha256(raw).hexdigest(),
        "snappy_default_crossover_reads": crossings,
        "arrays_checked_against_results": True,
        "figures": ["cpu-reuse-cost", "pcbs-historical-tradeoffs"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
