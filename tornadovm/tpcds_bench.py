#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# TPC-DS queries (Spark's own copies, sql/core/src/test/resources/tpcds on branch-3.5) over the
# Iceberg tables tpcds_setup.py made, on whichever engine run.sh configured: one warm-up and
# <runs> timed runs a query, the median printed with a checksum of the result -- its rows,
# doubles and decimals rounded to 1e-6 relative, sorted -- for comparing engines.
#
#   [SPLIT_SIZE=bytes] run.sh spark|rapids|spark-gpu <warehouse>/<namespace> <query dir> <runs> [query...]
import hashlib
import os
import statistics
import sys
import time
from decimal import Decimal

from pyspark.sql import SparkSession

warehouse, query_dir, runs = sys.argv[1], sys.argv[2], int(sys.argv[3])
names = sys.argv[4:] or sorted((f[:-4] for f in os.listdir(query_dir) if f.endswith(".sql")),
                               key=lambda q: (int("".join(c for c in q[1:] if c.isdigit())), q))
engine = os.environ.get("ENGINE", "spark")
spark = SparkSession.builder.appName(f"tpcds-{engine}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql(f"USE local.{os.path.basename(warehouse.rstrip('/'))}")
if os.environ.get("SPLIT_SIZE"):
    # Iceberg's read split size for this engine's run: bigger splits, fewer and larger tasks.
    spark.conf.set("spark.sql.iceberg.read.split-size", os.environ["SPLIT_SIZE"])


def canonical(value):
    if isinstance(value, (float, Decimal)):
        return f"{float(value):.6g}"
    return str(value)


def checksum(rows):
    lines = sorted("|".join(canonical(v) for v in row) for row in rows)
    return hashlib.sha1("\n".join(lines).encode()).hexdigest()[:12], len(lines)


pid = spark.sparkContext._jvm.java.lang.ProcessHandle.current().pid()


def rss_mb():
    """The Spark JVM's resident memory: heap, and the device scan's off-heap host buffers."""
    with open(f"/proc/{pid}/status") as status:
        for line in status:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) // 1024
    return -1


print("query,engine,median_ms,min_ms,rows,checksum,runs_ms,rss_mb,first_ms", flush=True)
for name in names:
    sql = open(f"{query_dir}/{name}.sql").read().strip().rstrip(";")
    try:
        start = time.perf_counter()
        rows = spark.sql(sql).collect()  # warm-up: the query's first run, timed apart
        first = (time.perf_counter() - start) * 1000
        times = []
        for _ in range(runs):
            start = time.perf_counter()
            rows = spark.sql(sql).collect()
            times.append((time.perf_counter() - start) * 1000)
        digest, count = checksum(rows)
        print(f"{name},{engine},{statistics.median(times):.1f},{min(times):.1f},{count},{digest},"
              f"\"{' '.join(f'{t:.1f}' for t in times)}\",{rss_mb()},{first:.1f}", flush=True)
    except Exception as e:  # one failing query costs that query
        java = getattr(e, "java_exception", None)
        reason = java.toString() if java is not None else str(e)
        cause = java.getCause() if java is not None else None
        while cause is not None:
            reason = cause.toString()
            cause = cause.getCause()
        print(f"{name},{engine},,,,failed,\"{reason.splitlines()[0][:300]}\"", flush=True)
