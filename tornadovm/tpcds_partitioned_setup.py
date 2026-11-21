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
# TPC-DS store_sales as partitioned Iceberg tables, from DuckDB's dsdgen Parquet, each with the same
# 2% of rows deleted (every ticket with ss_ticket_number % 50 = 7) in a different way:
#   ss_pos  v2, Spark's merge-on-read DELETE (file-scoped position deletes)
#   ss_dv   v3, the same DELETE (deletion vectors)
#   ss_eq   v3, nothing deleted here; the deleted tickets go to <warehouse>/_eq_keys as CSV
#           (partition value, ticket) for AddPartitionedEqualityDeletes, which writes one equality
#           delete file a partition, as a CDC writer does
# Partitioned by month of sale (truncate(30, ss_sold_date_sk)); sort order (ss_item_sk,
# ss_customer_sk) for sorted compaction.
#
#   BENCH_SCRIPT=tpcds_partitioned_setup.py run.sh spark <warehouse>/<namespace> <store_sales parquet dir>
import os
import shutil
import sys

from pyspark.sql import SparkSession

warehouse, source = sys.argv[1:3]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName("tpcds-partitioned-setup").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
spark.read.parquet(source).createOrReplaceTempView("src")
for name, version in (("ss_pos", 2), ("ss_dv", 3), ("ss_eq", 3)):
    t = f"local.{namespace}.{name}"
    spark.sql(f"DROP TABLE IF EXISTS {t}")
    spark.sql(
        f"CREATE TABLE {t} USING iceberg PARTITIONED BY (truncate(30, ss_sold_date_sk)) TBLPROPERTIES ("
        f"'format-version'='{version}','write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
        "'write.merge.mode'='merge-on-read','write.delete.granularity'='file',"
        "'write.target-file-size-bytes'='134217728','write.parquet.compression-codec'='zstd') "
        "AS SELECT * FROM src ORDER BY ss_sold_date_sk")
    spark.sql(f"ALTER TABLE {t} WRITE ORDERED BY ss_item_sk, ss_customer_sk")
    if name != "ss_eq":
        spark.sql(f"DELETE FROM {t} WHERE ss_ticket_number % 50 = 7")
    files = spark.sql(f"SELECT count(*) FROM {t}.files WHERE content = 0").collect()[0][0]
    deletes = spark.sql(f"SELECT count(*) FROM {t}.files WHERE content <> 0").collect()[0][0]
    rows = spark.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
    print(f"{name}: v{version}, {files} data files, {deletes} delete files, {rows} live rows", flush=True)

keys = os.path.join(warehouse, "_eq_keys")
shutil.rmtree(keys, ignore_errors=True)
spark.sql(
    "SELECT DISTINCT CAST(FLOOR(ss_sold_date_sk / 30) * 30 AS INT) AS p, ss_ticket_number AS k "
    f"FROM local.{namespace}.ss_eq WHERE ss_ticket_number % 50 = 7"
).coalesce(1).write.option("nullValue", "").csv(keys)
print(f"keys: {keys}", flush=True)
