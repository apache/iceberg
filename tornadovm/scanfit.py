#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
"""Pairs the per-scan timings of two runs (spark.iceberg.gpu.scanLog) and fits the read offload rule.

    scanfit.py <gpu-run-dir> <cpu-run-dir>

A scan is identified by (table, files, rows, columns, bytes, deleteFiles); its time per engine is
the median over the runs' repetitions. The script prints where the device wins, then searches rules
of the form "device if decoded bytes >= B or delete files >= D" (decoded bytes = rows x columns x 8)
for the one that minimizes the summed scan time, against all-CPU, all-GPU and the ideal per-scan
choice.
"""
import csv
import glob
import itertools
import statistics
import sys
from collections import defaultdict

KEY = ("table", "files", "rows", "columns", "bytes", "deleteFiles")


def load(run_dir, engine):
    times = defaultdict(list)
    for path in glob.glob(f"{run_dir}/scans-*.csv"):
        with open(path) as f:
            for row in csv.DictReader(f):
                if row["engine"] == engine:
                    times[tuple(row[k] for k in KEY)].append(float(row["ms"]))
    return {k: statistics.median(v) for k, v in times.items()}


def main():
    gpu, cpu = load(sys.argv[1], "gpu"), load(sys.argv[2], "cpu")
    keys = sorted(set(gpu) & set(cpu))
    if not keys:
        sys.exit(f"no paired scans (gpu {len(gpu)}, cpu {len(cpu)})")
    scans = []
    for k in keys:
        d = dict(zip(KEY, k))
        rows, cols = int(d["rows"]), int(d["columns"])
        scans.append({"table": d["table"].split(".")[-1], "rows": rows, "cols": cols,
                      "decoded": rows * cols * 8, "deletes": int(d["deleteFiles"]),
                      "gpu": gpu[k], "cpu": cpu[k]})
    print(f"{len(scans)} paired scans ({len(gpu)} on the GPU run, {len(cpu)} on the CPU run)")
    wins = [s for s in scans if s["gpu"] < s["cpu"]]
    print(f"device faster on {len(wins)} of {len(scans)}")

    print("\nby table: scans, device wins, CPU ms, GPU ms (sums of medians)")
    for table in sorted({s["table"] for s in scans}):
        ts = [s for s in scans if s["table"] == table]
        print(f"  {table:24} {len(ts):4} {sum(s['gpu'] < s['cpu'] for s in ts):4} "
              f"{sum(s['cpu'] for s in ts):10.0f} {sum(s['gpu'] for s in ts):10.0f}")

    total_cpu = sum(s["cpu"] for s in scans)
    total_gpu = sum(s["gpu"] for s in scans)
    ideal = sum(min(s["cpu"], s["gpu"]) for s in scans)
    best = None
    sizes = sorted({s["decoded"] for s in scans})
    deletes = sorted({s["deletes"] for s in scans}) + [10 ** 9]
    for b, d in itertools.product(sizes + [10 ** 18], deletes):
        t = sum(s["gpu"] if (s["decoded"] >= b or s["deletes"] >= d) else s["cpu"] for s in scans)
        if best is None or t < best[0]:
            best = (t, b, d)
    t, b, d = best
    print(f"\nsummed scan time: all CPU {total_cpu / 1e3:.1f} s, all GPU {total_gpu / 1e3:.1f} s, "
          f"ideal per scan {ideal / 1e3:.1f} s")
    print(f"best rule: device if decoded bytes >= {b / 2**20:.0f} MiB"
          + (f" or delete files >= {d}" if d < 10 ** 9 else "")
          + f": {t / 1e3:.1f} s")


if __name__ == "__main__":
    main()
