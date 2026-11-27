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
# Iceberg on the GPU from Spark, end to end: a merge-on-read table with deletes, a grouped
# aggregate and a read on the GPU (and the same on Spark's CPU path, to compare), then compaction.
#
#   spark-submit-gpu --driver-memory 16g tornadovm/spark/examples/quickstart.py /tmp/quickstart-wh [rows]

import sys
import time

from pyspark.sql import SparkSession

warehouse = sys.argv[1]
rows = int(sys.argv[2]) if len(sys.argv) > 2 else 20_000_000

spark = (
    SparkSession.builder.appName("iceberg-gpu-quickstart")
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.local.type", "hadoop")
    .config("spark.sql.catalog.local.warehouse", warehouse)
    .getOrCreate()
)

spark.sql("DROP TABLE IF EXISTS local.db.sales PURGE")
spark.sql(
    """CREATE TABLE local.db.sales (id BIGINT NOT NULL, k INT NOT NULL, v DOUBLE NOT NULL)
       USING iceberg
       TBLPROPERTIES ('format-version' = '3', 'write.delete.mode' = 'merge-on-read')"""
)
spark.sql(
    f"""INSERT INTO local.db.sales
        SELECT id, CAST(id % 1000 AS INT), rand(7) FROM range({rows}) DISTRIBUTE BY id % 8"""
)
# Merge-on-read: deletion vectors, applied at read time.
spark.sql("DELETE FROM local.db.sales WHERE id % 20 = 0")

query = "SELECT k, count(*) AS n, sum(v) AS total FROM local.db.sales GROUP BY k ORDER BY k"


def run(gpu):
    spark.conf.set("spark.iceberg.gpu.enabled", str(gpu).lower())
    best, result, plan = None, None, None
    for _ in range(3):
        start = time.time()
        frame = spark.sql(query)
        result = frame.collect()
        elapsed = time.time() - start
        best = elapsed if best is None else min(best, elapsed)
        plan = frame._jdf.queryExecution().executedPlan().toString()
    return best, result, "GpuIcebergAggregate" in plan


gpu_time, gpu_rows, on_gpu = run(True)
cpu_time, cpu_rows, _ = run(False)
print(f"aggregate: GPU {gpu_time:.2f} s (on the GPU: {on_gpu}), Spark {cpu_time:.2f} s, "
      f"same answer: {[r.n for r in gpu_rows] == [r.n for r in cpu_rows] and all(abs(a.total - b.total) <= 1e-9 * abs(b.total) for a, b in zip(gpu_rows, cpu_rows))}")

spark.conf.set("spark.iceberg.gpu.enabled", "true")
start = time.time()
spark.sql("CALL local.system.rewrite_data_files(table => 'db.sales', options => map('rewrite-all', 'true'))").show()
print(f"compaction: {time.time() - start:.2f} s")
print(f"rows after compaction: {spark.table('local.db.sales').count()} (expected {rows - (rows + 19) // 20})")
spark.stop()
