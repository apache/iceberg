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
# Deleting rows by key in Spark without equality deletes: MERGE ... WHEN MATCHED THEN DELETE in
# merge-on-read mode on a v3 table, which writes deletion vectors. The baseline of deleteKeys.
#   BENCH_SCRIPT=spark_delete_keys.py run.sh spark <warehouse> <table> <runs> <key count>...
# Each run rolls back to the snapshot it started from. Keys: distinct random existing ids, the same
# seeds as NewOpsBench.
import random
import sys
import time
from pyspark.sql import SparkSession

warehouse, table, runs, counts = sys.argv[1], sys.argv[2], int(sys.argv[3]), [int(c) for c in sys.argv[4:]]
namespace = warehouse.rstrip("/").split("/")[-1]
name = f"local.{namespace}.{table}"
spark = SparkSession.builder.appName("delete-keys").getOrCreate()
spark.sql(f"ALTER TABLE {name} SET TBLPROPERTIES ('write.merge.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')")
# The table's first snapshot is its data files appended, with nothing deleted: every run starts
# from it.
start_snapshot = spark.sql(
    f"SELECT snapshot_id FROM {name}.history ORDER BY made_current_at ASC LIMIT 1").first()[0]
spark.sql(f"CALL local.system.rollback_to_snapshot('{namespace}.{table}', {start_snapshot})")
rows = spark.table(name).count()
for count in counts:
    for run in range(runs):
        rng = random.Random(count * 31 + run)
        keys = set()
        while len(keys) < count:
            keys.add(int(rng.random() * rows))
        spark.createDataFrame([(k,) for k in keys], "id long").createOrReplaceTempView("keys")
        spark.table("keys").cache().count()
        start = time.time()
        spark.sql(f"MERGE INTO {name} t USING keys k ON t.id = k.id WHEN MATCHED THEN DELETE")
        millis = (time.time() - start) * 1000
        after = spark.table(name).count()
        assert after == rows - count, (after, rows, count)
        print(f"deletekeys,{table},{count},{run},{'yes' if run == 0 else 'no'},{millis:.1f},{rows - after}", flush=True)
        spark.sql(f"CALL local.system.rollback_to_snapshot('{namespace}.{table}', {start_snapshot})")
        spark.catalog.dropTempView("keys")
