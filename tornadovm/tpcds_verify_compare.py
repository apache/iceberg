#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
#
# Compares two engines' exact TPC-DS results (tpcds_verify.py): decimals, integers and strings must
# be equal; floating-point values (Spark's DOUBLE results: averages and variances of doubles) equal
# within a relative 1e-9. Rows are matched after sorting each result.
#
#   tpcds_verify_compare.py <reference dir> <candidate dir>
import glob
import json
import math
import os
import sys


def key(row):
    return [("" if v is None else f"{float(v['f']):.6g}" if isinstance(v, dict) and "f" in v else json.dumps(v)) for v in row]


def same(a, b):
    if isinstance(a, dict) and "f" in a and isinstance(b, dict) and "f" in b:
        x, y = float(a["f"]), float(b["f"])
        return x == y or (math.isnan(x) and math.isnan(y)) or abs(x - y) <= 1e-9 * max(abs(x), abs(y))
    return a == b


ref, cand = sys.argv[1:3]
print("query,rows,exact_values,float_values,max_rel_float_diff,verdict")
bad = 0
for path in sorted(glob.glob(f"{ref}/*.jsonl")):
    name = os.path.basename(path)[:-6]
    other = f"{cand}/{name}.jsonl"
    if not os.path.exists(other):
        print(f"{name},,,,,missing"); bad += 1; continue
    a = sorted((json.loads(l) for l in open(path)), key=key)
    b = sorted((json.loads(l) for l in open(other)), key=key)
    exact = floats = 0
    worst = 0.0
    ok = len(a) == len(b)
    for ra, rb in zip(a, b):
        ok &= len(ra) == len(rb)
        for x, y in zip(ra, rb):
            if isinstance(x, dict) and "f" in x:
                floats += 1
                if isinstance(y, dict) and "f" in y:
                    fx, fy = float(x["f"]), float(y["f"])
                    if fx != fy and not (math.isnan(fx) and math.isnan(fy)):
                        worst = max(worst, abs(fx - fy) / max(abs(fx), abs(fy)))
            else:
                exact += 1
            ok &= same(x, y)
    bad += not ok
    print(f"{name},{len(a)},{exact},{floats},{worst:.2e},{'equal' if ok else 'DIFFERENT'}")
sys.exit(1 if bad else 0)
