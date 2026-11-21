#
# Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
#
# Every TPC-DS query's full result, once, as this engine computes it: one JSON line a row, values
# exact (a decimal as its string, a float as its repr), for tpcds_verify_compare.py.
#
#   BENCH_SCRIPT=tpcds_verify.py run.sh spark|spark-gpu <warehouse> <query dir> <out dir> [query...]
import json
import os
import sys
from decimal import Decimal

from pyspark.sql import SparkSession

warehouse, query_dir, out = sys.argv[1:4]
names = sys.argv[4:] or sorted((f[:-4] for f in os.listdir(query_dir) if f.endswith(".sql")),
                               key=lambda q: (int("".join(c for c in q[1:] if c.isdigit())), q))
spark = SparkSession.builder.appName("tpcds-verify").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql(f"USE local.{os.path.basename(warehouse.rstrip('/'))}")
os.makedirs(out, exist_ok=True)


def exact(value):
    if isinstance(value, Decimal):
        return {"d": str(value)}
    if isinstance(value, float):
        return {"f": repr(value)}
    return value if value is None or isinstance(value, (int, bool)) else str(value)


for name in names:
    sql = open(f"{query_dir}/{name}.sql").read().strip().rstrip(";")
    try:
        rows = spark.sql(sql).collect()
        with open(f"{out}/{name}.jsonl", "w") as f:
            for row in rows:
                f.write(json.dumps([exact(v) for v in row]) + "\n")
        print(f"{name},{len(rows)}", flush=True)
    except Exception as e:
        print(f"{name},failed,{str(e).splitlines()[0][:200]}", flush=True)
