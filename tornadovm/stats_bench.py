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
# Iceberg's compute_table_stats in Spark, timed: the baseline of DeviceMergeOnReadScan.computeTableStats.
#   BENCH_SCRIPT=stats_bench.py run.sh spark <warehouse> <columns,comma-separated> <runs> <table>...
import sys
import time
import json
from pyspark.sql import SparkSession

warehouse, columns, runs, tables = sys.argv[1], sys.argv[2].split(","), int(sys.argv[3]), sys.argv[4:]
namespace = warehouse.rstrip("/").split("/")[-1]
spark = SparkSession.builder.appName("stats-bench").getOrCreate()
cols = ", ".join(f"'{c}'" for c in columns)
for table in tables:
    for run in range(runs):
        start = time.time()
        rows = spark.sql(
            f"CALL local.system.compute_table_stats(table => '{namespace}.{table}', columns => array({cols}))").collect()
        millis = (time.time() - start) * 1000
        # The NDV estimates, from the statistics file the call wrote.
        ndv = {}
        stats = spark.sql(f"SELECT * FROM local.{namespace}.{table}.metadata_log_entries LIMIT 1").collect()
        print(f"{table},spark,stats,{run},{'yes' if run == 0 else 'no'},{millis:.1f},{json.dumps(str(rows[0]))}", flush=True)
