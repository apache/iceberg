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
# TPC-DS as Iceberg v2 merge-on-read tables, from DuckDB's dsdgen Parquet (decimals stay decimals),
# then mutated as a warehouse's data maintenance would leave them: in each sales and returns fact
# table and in inventory, ~2% of rows deleted and ~1% updated with Spark's merge-on-read DELETE and
# UPDATE, keyed by ticket or order number so that every data file carries position deletes (and,
# for updates, the rewritten rows in new files).
#
# With FORMAT_VERSION=3 the tables are version 3, so the same DELETE and UPDATE write deletion vectors.
#
#   BENCH_SCRIPT=tpcds_setup.py run.sh spark <warehouse>/<namespace> <parquet dir: a directory a table>
#       [FORMAT_VERSION=2]
import os
import sys

from pyspark.sql import SparkSession

warehouse, source = sys.argv[1:3]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName("tpcds-setup").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
version = os.environ.get("FORMAT_VERSION", "2")
props = (f"'format-version'='{version}','write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
         "'write.merge.mode'='merge-on-read','write.target-file-size-bytes'='134217728',"
         "'write.parquet.compression-codec'='zstd'")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
for table in sorted(os.listdir(source)):
    spark.read.parquet(f"{source}/{table}").createOrReplaceTempView("src")
    spark.sql(f"DROP TABLE IF EXISTS local.{namespace}.{table}")
    spark.sql(f"CREATE TABLE local.{namespace}.{table} USING iceberg TBLPROPERTIES ({props}) AS SELECT * FROM src")

# (table, key column, a numeric column to update, a decimal column to update)
mutations = [
    ("store_sales", "ss_ticket_number", "ss_quantity", "ss_net_paid"),
    ("catalog_sales", "cs_order_number", "cs_quantity", "cs_net_paid"),
    ("web_sales", "ws_order_number", "ws_quantity", "ws_net_paid"),
    ("store_returns", "sr_ticket_number", "sr_return_quantity", "sr_net_loss"),
    ("catalog_returns", "cr_order_number", "cr_return_quantity", "cr_net_loss"),
    ("web_returns", "wr_order_number", "wr_return_quantity", "wr_net_loss"),
]
for table, key, quantity, amount in mutations:
    t = f"local.{namespace}.{table}"
    spark.sql(f"DELETE FROM {t} WHERE {key} % 50 = 7")
    spark.sql(f"UPDATE {t} SET {quantity} = {quantity} + 1, {amount} = {amount} + 1 WHERE {key} % 100 = 11")
spark.sql(f"DELETE FROM local.{namespace}.inventory WHERE inv_item_sk % 50 = 7")
spark.sql(f"UPDATE local.{namespace}.inventory SET inv_quantity_on_hand = inv_quantity_on_hand + 1 "
          f"WHERE inv_item_sk % 100 = 11")

for table in sorted(os.listdir(source)):
    t = f"local.{namespace}.{table}"
    n = spark.sql(f"SELECT count(*) FROM {t}").collect()[0][0]
    files = spark.sql(f"SELECT content, count(*) FROM {t}.files GROUP BY content ORDER BY content").collect()
    print(f"TPCDS {table}: {n} rows, files by content {[(r[0], r[1]) for r in files]}", flush=True)
