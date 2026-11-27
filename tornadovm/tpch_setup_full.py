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
# All of TPC-H as Iceberg v2 merge-on-read tables, from DuckDB's dbgen Parquet (decimals stay
# decimals), then maintained by ten refresh pairs in the style of TPC-H's RF1 and RF2. Refresh p
# (0..9): RF2 deletes the orders with o_orderkey % 1000 = p and their line items (SF x 1500 orders,
# as the specification's RF2), and RF1 inserts as many new orders with their line items: copies of the
# orders with o_orderkey % 1000 = 600 + p, keyed o_orderkey + 8 (so % 1000 = 608 + p), which TPC-H's
# sparse key space leaves unused (dbgen assigns 8 of every 32 keys), so keys stay as dense as dbgen's.
# The RF1 rows are written to Parquet once, before any refresh, so every refresh inserts exactly its own.
#
#   BENCH_SCRIPT=tpch_setup_full.py run.sh spark <warehouse>/<namespace> <parquet dir> pos
#       the refreshes with Spark's merge-on-read DELETE (position deletes) and INSERT
#   ... eq-load    the tables, and the deleted keys in <warehouse>/_keys/{orders,lineitem}.txt for
#                  AddEqualityDeletes (equality deletes, as a change-data-capture writer leaves them)
#   ... eq-append  the RF1 inserts, after the equality deletes (a later sequence number)
#   ... refresh    the ten refresh pairs (position deletes) on tables already loaded, e.g. by
#                  tpch_load_chunk.py at scale factors too large to generate in one piece
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

warehouse, source, mode = sys.argv[1:4]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName(f"tpch-setup-full-{mode}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
props = ("'format-version'='2','write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
         "'write.merge.mode'='merge-on-read','write.target-file-size-bytes'='134217728',"
         "'write.parquet.compression-codec'='zstd'")
tables = {"lineitem": "l_orderkey", "orders": "o_orderkey", "partsupp": "ps_partkey", "part": "p_partkey",
          "customer": "c_custkey", "supplier": "s_suppkey", "nation": "n_nationkey", "region": "r_regionkey"}
T = lambda t: f"local.{namespace}.{t}"
REFRESHES = range(10)


def inserts():
    """The RF1 rows of every refresh (copies of orders % 1000 in 600..609, keyed + 8), written once to
    <warehouse>/_rf1 and read back: deterministic whatever the refreshes do to the tables after."""
    rf1 = os.path.join(warehouse, "_rf1")
    spark.table(T("orders")).where((F.col("o_orderkey") % 1000 >= 600) & (F.col("o_orderkey") % 1000 < 610)) \
        .withColumn("o_orderkey", F.col("o_orderkey") + 8).write.mode("overwrite").parquet(os.path.join(rf1, "orders"))
    spark.table(T("lineitem")).where((F.col("l_orderkey") % 1000 >= 600) & (F.col("l_orderkey") % 1000 < 610)) \
        .withColumn("l_orderkey", F.col("l_orderkey") + 8).write.mode("overwrite").parquet(os.path.join(rf1, "lineitem"))
    return spark.read.parquet(os.path.join(rf1, "orders")), spark.read.parquet(os.path.join(rf1, "lineitem"))


if mode in ("pos", "eq-load", "refresh"):
    if mode != "refresh":
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
        for table, order in tables.items():
            spark.read.parquet(os.path.join(source, table)).createOrReplaceTempView("src")
            spark.sql(f"DROP TABLE IF EXISTS {T(table)}")
            spark.sql(f"CREATE TABLE {T(table)} USING iceberg TBLPROPERTIES ({props}) AS SELECT * FROM src ORDER BY {order}")
    orders, lineitem = inserts()
    if mode in ("pos", "refresh"):
        for p in REFRESHES:
            spark.sql(f"DELETE FROM {T('lineitem')} WHERE l_orderkey % 1000 = {p}")
            spark.sql(f"DELETE FROM {T('orders')} WHERE o_orderkey % 1000 = {p}")
            o = orders.where(F.col("o_orderkey") % 1000 == 608 + p)
            l = lineitem.where(F.col("l_orderkey") % 1000 == 608 + p)
            o.writeTo(T("orders")).append()
            l.writeTo(T("lineitem")).append()
    else:
        keys = os.path.join(warehouse, "_keys")
        os.makedirs(keys, exist_ok=True)
        deleted = [r[0] for r in spark.sql(
            f"SELECT o_orderkey FROM {T('orders')} WHERE o_orderkey % 1000 < 10 ORDER BY 1").collect()]
        for name in ("orders", "lineitem"):
            with open(os.path.join(keys, f"{name}.txt"), "w") as f:
                f.write("\n".join(str(k) for k in deleted))
        print(f"TPCH {namespace}: {len(deleted)} orders to delete by equality", flush=True)
elif mode == "eq-append":
    spark.read.parquet(os.path.join(warehouse, "_rf1", "orders")).writeTo(T("orders")).append()
    spark.read.parquet(os.path.join(warehouse, "_rf1", "lineitem")).writeTo(T("lineitem")).append()

if mode != "eq-load":
    for table in tables:
        n = spark.sql(f"SELECT count(*) FROM {T(table)}").collect()[0][0]
        deletes = spark.sql(f"SELECT count(*) FROM {T(table)}.delete_files").collect()[0][0]
        print(f"TPCH {namespace}.{table}: {n} rows, {deletes} delete files", flush=True)
