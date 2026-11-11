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
# One chunk of TPC-H (DuckDB's dbgen with children/step: every chunk holds a slice of every table)
# into Iceberg v2 merge-on-read tables: the first chunk creates the tables, every later one appends,
# each sorted by its table's key. Large scale factors are generated and loaded one chunk at a time,
# so the disk holds the tables plus one chunk. Then tpch_setup_full.py ... refresh.
#   BENCH_SCRIPT=tpch_load_chunk.py run.sh spark <warehouse>/<namespace> <chunk dir> <first|next>
import os
import sys

from pyspark.sql import SparkSession

warehouse, source, which = sys.argv[1:4]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName(f"tpch-load-chunk-{which}").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
props = ("'format-version'='2','write.delete.mode'='merge-on-read','write.update.mode'='merge-on-read',"
         "'write.merge.mode'='merge-on-read','write.target-file-size-bytes'='134217728',"
         "'write.parquet.compression-codec'='zstd'")
tables = {"lineitem": "l_orderkey", "orders": "o_orderkey", "partsupp": "ps_partkey", "part": "p_partkey",
          "customer": "c_custkey", "supplier": "s_suppkey", "nation": "n_nationkey", "region": "r_regionkey"}
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS local.{namespace}")
for table, order in tables.items():
    path = os.path.join(source, table)
    if not os.path.isdir(path) or not os.listdir(path):
        continue  # a late chunk can hold no rows of the smallest tables
    spark.read.parquet(path).createOrReplaceTempView("src")
    name = f"local.{namespace}.{table}"
    if which == "first":
        spark.sql(f"DROP TABLE IF EXISTS {name}")
        spark.sql(f"CREATE TABLE {name} USING iceberg TBLPROPERTIES ({props}) AS SELECT * FROM src ORDER BY {order}")
    else:
        spark.sql(f"INSERT INTO {name} SELECT * FROM src ORDER BY {order}")
for table in tables:
    n = spark.sql(f"SELECT count(*) FROM local.{namespace}.{table}").collect()[0][0]
    print(f"TPCH {namespace}.{table}: {n} rows so far", flush=True)
