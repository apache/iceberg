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
# Iceberg's own rewrite_data_files (bin-pack, rewrite-all) on Spark's runner and on the GPU's
# (GpuFileRewriteRunnerProvider), alternating in one warm JVM: the live Spark setting
# spark.iceberg.gpu.enabled picks the runner. Before each run the table is rolled back to its
# snapshot before any compaction, so every run rewrites the same files; after it, the table must
# read back the same rows -- count, a checksum over every numeric column -- with no delete file.
#
#   BENCH_SCRIPT=compact_bench.py run.sh spark-gpu <warehouse>/<namespace> <runs> <table>...
#   BENCH_SCRIPT=compact_bench.py run.sh rapids    <warehouse>/<namespace> <runs> <table>...
#     (Spark's runner with the RAPIDS Accelerator off and on, as engines spark and rapids)
import os
import sys
import time

from pyspark.sql import SparkSession

warehouse, runs, tables = sys.argv[1], int(sys.argv[2]), sys.argv[3:]
namespace = os.path.basename(warehouse.rstrip("/"))
spark = SparkSession.builder.appName("compact-bench").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
jvm = spark.sparkContext._jvm
env_conf = jvm.org.apache.spark.SparkEnv.get().conf()


RAPIDS = os.environ.get("ENGINE") == "rapids"
GPU_ENGINE = "rapids" if RAPIDS else "gpu"


def gpu(on):
    if RAPIDS:
        # Spark's own runner either way; the RAPIDS Accelerator runs its read and write on the GPU.
        spark.conf.set("spark.rapids.sql.enabled", "true" if on else "false")
    else:
        env_conf.set("spark.iceberg.gpu.enabled", "true" if on else "false")


def fingerprint(name):
    """Rows and a checksum over every numeric column, read on the CPU."""
    gpu(False)
    df = spark.table(name)
    numeric = [f.name for f in df.schema.fields
               if f.dataType.typeName() in ("integer", "long", "double", "float", "decimal", "date")]
    exprs = ", ".join(f"sum(hash({c}))" for c in numeric)
    row = spark.sql(f"SELECT count(*), {exprs} FROM {name}").collect()[0]
    return tuple(row)


def delete_files(name):
    return spark.sql(f"SELECT count(*) FROM {name}.files WHERE content > 0").collect()[0][0]


print("table,engine,run,millis,rewritten,added,rows_ok,deletes_left", flush=True)
for t in tables:
    name = f"local.{namespace}.{t}"
    table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    start_snapshot = table.currentSnapshot().snapshotId()
    expected = fingerprint(name)
    for run in range(runs + 1):  # run 0 warms both up
        for engine in ("spark", GPU_ENGINE):
            table.refresh()
            table.manageSnapshots().rollbackTo(start_snapshot).commit()
            table.refresh()
            gpu(engine == GPU_ENGINE)
            begin = time.perf_counter()
            result = (jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
                      .rewriteDataFiles(table).option("rewrite-all", "true").execute())
            millis = (time.perf_counter() - begin) * 1000
            ok = fingerprint(name) == expected
            left = delete_files(name)
            # Roll back now and expire the rewrite's snapshot with the files it wrote: runs would
            # otherwise fill the disk with rewrites nothing references.
            table.refresh()
            rewritten = table.currentSnapshot().snapshotId()
            if rewritten != start_snapshot:
                table.manageSnapshots().rollbackTo(start_snapshot).commit()
                table.refresh()
                table.expireSnapshots().expireSnapshotId(rewritten).commit()
            print(f"{t},{engine},{run},{millis:.0f},{result.rewrittenDataFilesCount()},"
                  f"{result.addedDataFilesCount()},{ok},{left}", flush=True)
    table.refresh()
    table.manageSnapshots().rollbackTo(start_snapshot).commit()
