# Licensed to the Apache Software Foundation (ASF) under the Apache License, Version 2.0.
# Physical plan shapes of TPC-DS queries (not executed): joins, their strategy and types, scanned
# tables, Expand (ROLLUP/CUBE), windows, and the join key data types. Run through run.sh spark.
import collections, json, os, re, sys
from pyspark.sql import SparkSession

warehouse, query_dir = sys.argv[1], sys.argv[2]
names = sys.argv[3:]
spark = SparkSession.builder.appName("tpcds-shapes").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.adaptive.enabled", "false")  # the static plan: join strategies as planned
spark.sql(f"USE local.{os.path.basename(warehouse.rstrip('/'))}")
out = {}
for q in names:
    sql = open(os.path.join(query_dir, q + ".sql")).read()
    try:
        df = spark.sql(sql)
        plan = df._jdf.queryExecution().executedPlan().toString()
        logical = df._jdf.queryExecution().optimizedPlan().toString()
    except Exception as e:
        out[q] = {"error": str(e).splitlines()[0][:200]}
        continue
    joins = collections.Counter(re.findall(r"(BroadcastHashJoin|SortMergeJoin|BroadcastNestedLoopJoin|ShuffledHashJoin|CartesianProduct)", plan))
    jtypes = collections.Counter(re.findall(r"(?:BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin) \[[^\]]*\], \[[^\]]*\], (\w+)", plan))
    tables = collections.Counter(re.findall(r"BatchScan local\.\w+\.(\w+)", plan))
    out[q] = {"joins": dict(joins), "joinTypes": dict(jtypes), "tables": dict(tables),
              "expand": plan.count("Expand"), "window": plan.count("Window"),
              "aggregates": plan.count("HashAggregate") + plan.count("SortAggregate"),
              "subqueries": logical.count("scalar-subquery") + logical.count("ScalarSubquery")}
print("SHAPES " + json.dumps(out))
spark.stop()
