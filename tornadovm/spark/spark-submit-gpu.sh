#!/usr/bin/env bash
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
# spark-submit with Iceberg on the GPU: reads, grouped aggregates and bin-pack compaction
# (rewrite_data_files) run on an NVIDIA GPU through TornadoVM and cuDF; everything else is Spark's.
# Local mode only. Arguments are spark-submit's; GPU_DEVICE_MEMORY (default 20GB) caps TornadoVM's
# device memory, GPU_JAVA_OPTS adds driver JVM options.
#
#   spark-submit-gpu --driver-memory 24g [--conf ...] app.py|--class ... app.jar
#   SPARK_MASTER=spark://host:7077 spark-submit-gpu ...    (cluster: one executor per GPU)
#
# Needs SPARK_HOME (Spark 4.2, Scala 2.13), JAVA_HOME (JDK 21), and libcudf and TornadoVM's native
# libraries on LD_LIBRARY_PATH (see the README).
set -euo pipefail
: "${SPARK_HOME:?set SPARK_HOME to a Spark 4.2 distribution}"
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CP=$(ls "$HERE"/jars/*.jar | paste -sd: -)
GPU_JVM="--enable-preview @$HERE/tornado-argfile --enable-native-access=ALL-UNNAMED -Dtornado.device.memory=${GPU_DEVICE_MEMORY:-20GB} -da:tornado... -da:uk.ac.manchester.tornado... ${GPU_JAVA_OPTS:-}"
MASTER="${SPARK_MASTER:-local[*]}"
EXECUTORS=()
if [[ "$MASTER" != local\[* && "$MASTER" != local ]]; then
  # Cluster: every executor runs GPU work, so it gets the driver's class path, JVM options and
  # native libraries. The bundle and the native libraries must be at these paths on every node,
  # and each executor needs a GPU of its own (one executor per GPU).
  EXECUTORS=(
    --conf spark.executor.extraClassPath="$CP"
    --conf spark.executor.extraJavaOptions="$GPU_JVM"
    --conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
    --conf spark.executor.memory="${GPU_EXECUTOR_MEMORY:-24g}"
  )
fi
exec "$SPARK_HOME/bin/spark-submit" \
  --master "$MASTER" \
  --driver-class-path "$CP" \
  --conf spark.driver.extraJavaOptions="$GPU_JVM" \
  "${EXECUTORS[@]}" \
  --conf spark.iceberg.gpu.enabled=true \
  --conf spark.sql.iceberg.partition-reader-factory-provider=tornadovm \
  --conf spark.sql.iceberg.rewrite-runner-provider=tornadovm \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.spark.sql.iceberg.gpu.GpuAggregateExtensions \
  "$@"
