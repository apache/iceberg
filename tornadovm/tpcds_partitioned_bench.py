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
# TpcdsBench's operations through Spark on the same tables, for comparison: one CSV line a run in
# TpcdsBench's format, the answer checked as it checks it.
#   agg      SELECT ss_store_sk, count(*), sum(ss_quantity), sum(ss_net_paid),
#            sum(ss_ext_sales_price) GROUP BY ss_store_sk
#   sorted   rewrite_data_files(strategy => 'sort') in the table's sort order, rolled back
#   rewrite  rewrite_data_files bin-pack with deletes applied (Spark's way to drop equality
#            deletes), rolled back
#
#   BENCH_SCRIPT=tpcds_partitioned_bench.py run.sh spark <warehouse>/<namespace> <agg|sorted|rewrite> <runs> <table>...
import os
import sys
import time

from pyspark.sql import SparkSession

warehouse, operation, runs = sys.argv[1], sys.argv[2], int(sys.argv[3])
tables = sys.argv[4:]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName(f"tpcds-partitioned-{operation}").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
# REWRITE_OPTIONS overrides, e.g. one output file a group (group size = target size) for a per-file sort.
options = os.environ.get(
    "REWRITE_OPTIONS",
    "'rewrite-all','true','remove-dangling-deletes','true','max-file-group-size-bytes','268435456',"
    "'max-concurrent-file-group-rewrites','32','target-file-size-bytes','134217728'")
print("table,engine,operation,run,warmup,ms,live_rows,checksum,cpu_s,detail", flush=True)


def sales(table):
    rows = spark.sql(
        "SELECT count(*) c, sum(ss_quantity) q, sum(ss_net_paid) p, sum(ss_ext_sales_price) s "
        f"FROM {table} GROUP BY ss_store_sk").collect()
    return sum(r["c"] for r in rows), sum(float(r["q"] or 0) + float(r["p"] or 0) + float(r["s"] or 0) for r in rows)


for name in tables:
    table = f"local.{namespace}.{name}"
    answer = sales(table)
    for run in range(runs):
        snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()[0][0]
        before = {r[0] for r in spark.sql(f"SELECT file_path FROM {table}.all_files").collect()}
        start = time.perf_counter()
        detail = ""
        if operation == "agg":
            answer = sales(table)
        else:
            strategy = ", strategy => 'sort'" if operation == "sorted" else ""
            result = spark.sql(
                f"CALL local.system.rewrite_data_files(table => '{namespace}.{name}'{strategy}, "
                f"options => map({options}))").collect()[0]
            detail = f"{result[0]} data files -> {result[1]}"
        millis = (time.perf_counter() - start) * 1000
        if operation != "agg":
            spark.sql(f"REFRESH TABLE {table}")
            after = sales(table)
            if after[0] != answer[0] or abs(after[1] - answer[1]) > 1e-6 * abs(answer[1]):
                raise SystemExit(f"{name}: {operation} changed the answer: {after} vs {answer}")
            added = [r[0] for r in spark.sql(f"SELECT file_path FROM {table}.all_files").collect()
                     if r[0] not in before]
            written = sum(os.path.getsize(p[len("file:"):] if p.startswith("file:") else p) for p in added)
            detail += f", {written / 1e9:.2f} GB written"
            spark.sql(f"CALL local.system.set_current_snapshot('{namespace}.{name}', {snapshot})").collect()
            for path in added:
                local = path[len("file:"):] if path.startswith("file:") else path
                if os.path.exists(local):
                    os.remove(local)
        print(f"{name},spark,{operation},{run},{'yes' if run == 0 else 'no'},{millis:.1f},{answer[0]},"
              f"{answer[1]:.6e},,\"{detail}\"", flush=True)
