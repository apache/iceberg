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
# The Spark baseline for MorBench: the same three queries over the same path-based tables, with
# Iceberg's vectorized merge-on-read in Spark. Output has MorBench's CSV columns.
#
#   spark-submit ... spark_bench.py <warehouse> <q1|q2|q3> <runs> <table>...
#
# q3 materializes every live row through Spark's no-op sink, which runs the whole read and drops
# the rows; its row count and checksum come from one extra, untimed, aggregate.
import os
import sys
import time

from pyspark.sql import SparkSession

warehouse, query, runs, tables = sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4:]
# The catalog's warehouse is the warehouse's parent, so its tables are <warehouse name>.<table>.
# "spark", or "rapids" when run.sh adds the RAPIDS Accelerator plugin.
engine = os.environ.get("ENGINE", "spark")
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName("mor-bench").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("table,engine,query,run,warmup,millis,live_rows,checksum,breakdown", flush=True)
def rewrite_run(name, run, query="rewrite"):
    # Compaction: rewrite every file with its deletes applied, check Q2's answer on the result,
    # then restore the snapshot the table had and delete the files the rewrite wrote, so every run
    # starts from the same merge-on-read table.
    table = f"local.{namespace}.{name}"
    snapshot = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()[0][0]
    before = {r[0] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect()}
    options = os.environ.get(
        "REWRITE_OPTIONS",
        "'rewrite-all','true','remove-dangling-deletes','true',"
        "'max-file-group-size-bytes','268435456','max-concurrent-file-group-rewrites','32',"
        "'target-file-size-bytes','134217728'",
    )
    start = time.perf_counter()
    # zorder: the same compaction, sorting each file group by Iceberg's Z-order of (k, v1); sorted:
    # by the table's sort order, (k, v1) ascending, set first when the table has none.
    sort = ""
    if query == "zorder":
        sort = ", strategy => 'sort', sort_order => 'zorder(k, v1)'"
    elif query == "sorted":
        if run == 0:
            spark.sql(f"ALTER TABLE {table} WRITE ORDERED BY k, v1")
        sort = ", strategy => 'sort'"
    spark.sql(f"CALL local.system.rewrite_data_files(table => '{namespace}.{name}'{sort}, options => map({options}))").collect()
    millis = (time.perf_counter() - start) * 1000
    spark.sql(f"REFRESH TABLE {table}")
    result = spark.sql(
        f"SELECT k, count(*) c, sum(v1) a, sum(v2) b, sum(v3) d, sum(v4) e FROM {table} GROUP BY k"
    ).collect()
    rows = sum(r["c"] for r in result)
    checksum = sum(r["a"] + r["b"] + r["d"] + r["e"] for r in result) + len(result)
    added = [r[0] for r in spark.sql(f"SELECT file_path FROM {table}.files").collect() if r[0] not in before]
    spark.sql(f"CALL local.system.set_current_snapshot('{namespace}.{name}', {snapshot})").collect()
    for path in added:
        local = path[len("file:"):] if path.startswith("file:") else path
        if os.path.exists(local):
            os.remove(local)
    print(f"{name},{engine},{query},{run},{'yes' if run == 0 else 'no'},{millis:.1f},{rows},{checksum:.6e},\"{len(added)} files\"", flush=True)


for name in tables:
    if query in ("rewrite", "zorder", "sorted"):
        for run in range(runs):
            rewrite_run(name, run, query)
        continue
    reader = spark.read.format("iceberg")
    if os.environ.get("SPLIT_SIZE"):
        # Iceberg packs small files into splits of read.split.target-size (128 MB); a smaller
        # target gives every core a task when the files are small.
        reader = reader.option("split-size", os.environ["SPLIT_SIZE"])
    reader.load(f"{warehouse}/{name}").createOrReplaceTempView("t")
    for run in range(runs):
        start = time.perf_counter()
        if query == "tpch1":
            result = spark.sql(
                "SELECT l_returnflag, l_linestatus, sum(l_quantity) q, sum(l_extendedprice) b, "
                "sum(l_extendedprice * (1 - l_discount)) d, "
                "sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) c, avg(l_quantity), "
                "avg(l_extendedprice), avg(l_discount), count(*) n FROM t "
                "WHERE l_shipdate <= date '1998-09-02' GROUP BY 1, 2 ORDER BY 1, 2"
            ).collect()
            rows = sum(r["n"] for r in result)
            checksum = sum(r["q"] + r["b"] + r["d"] + r["c"] for r in result)
        elif query == "tpch6":
            row = spark.sql(
                "SELECT count(*), sum(l_extendedprice * l_discount) FROM t "
                "WHERE l_shipdate >= date '1994-01-01' AND l_shipdate < date '1995-01-01' "
                "AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24"
            ).collect()[0]
            rows, checksum = row[0], row[1] or 0.0
        elif query == "q1":
            row = spark.sql("SELECT count(*), sum(v1) FROM t").collect()[0]
            rows, checksum = row[0], row[1] or 0.0
        elif query == "q2":
            result = spark.sql(
                "SELECT k, count(*) c, sum(v1) a, sum(v2) b, sum(v3) d, sum(v4) e FROM t GROUP BY k"
            ).collect()
            rows = sum(r["c"] for r in result)
            checksum = sum(r["a"] + r["b"] + r["d"] + r["e"] for r in result) + len(result)
        elif query == "q5":
            result = spark.sql(
                "SELECT k, count(*) c, sum(v1) a FROM t WHERE id < 25000000 AND v2 > 0.5 GROUP BY k"
            ).collect()
            rows = sum(r["c"] for r in result)
            checksum = sum(r["a"] for r in result) + len(result)
        elif query == "q4":
            # Iceberg's bucket transform as a Spark function, from the catalog run.sh configures.
            result = spark.sql(
                "SELECT local.system.bucket(16, id) b, count(*) c, sum(v1) a FROM t GROUP BY b"
            ).collect()
            rows = sum(r["c"] for r in result)
            checksum = sum(r["a"] for r in result) + len(result)
        else:
            spark.sql("SELECT id, k, v1 FROM t").write.format("noop").mode("overwrite").save()
            rows, checksum = None, None
        millis = (time.perf_counter() - start) * 1000
        if query == "q3":
            row = spark.sql("SELECT count(*), sum(id + k) + sum(v1) FROM t").collect()[0]
            rows, checksum = row[0], row[1]
        print(f"{name},{engine},{query},{run},{'yes' if run == 0 else 'no'},{millis:.1f},{rows},{checksum:.6e},\"\"", flush=True)
spark.stop()
