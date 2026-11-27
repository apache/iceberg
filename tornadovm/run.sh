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
# Merge-on-read benchmarks: the GPU scan, the same algorithm on the CPU, Iceberg's own reader, Spark.
#
#   run.sh gen      <warehouse> <files> <rowsPerFile>
#   run.sh gpu      <warehouse> <q1|q2|q3|q4> <runs> <table>...     TornadoVM JVM, CUDA device
#   run.sh cpu      <warehouse> <q1|q2|q3|q4> <runs> <table>...     plain JVM, all cores (THREADS=n);
#                                                                    MAIN=<class> runs another benchmark there
#   run.sh generics <warehouse> <q1|q2|q3|q4> <runs> <table>...     plain JVM, IcebergGenerics, 1 thread
#   run.sh spark    <warehouse> <q1|q2|q3|q4> <runs> <table>...     Spark local[*] + iceberg-spark-runtime
#   BENCH_SCRIPT=tpcds_bench.py run.sh spark|rapids|spark-gpu <warehouse>/<namespace> <query dir> <runs> [q...]
#                                   TPC-DS (tables from tpcds_setup.py) instead of spark_bench.py
#
# Needs: TORNADOVM_HOME (a TornadoVM SDK with the cuDF shim), JAVA_HOME (JDK 21), libcudf on
# LD_LIBRARY_PATH; for spark, SPARK_HOME and ICEBERG_SPARK_JAR.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(dirname "$HERE")"
: "${TORNADOVM_HOME:?set TORNADOVM_HOME to a TornadoVM SDK}"
: "${JAVA_HOME:?set JAVA_HOME to JDK 21}"
HEAP="${HEAP:-40g}"

classpath() {
    local file="$HERE/build/runtime-classpath.txt"
    (cd "$ROOT" && ./gradlew -q :iceberg-tornadovm:printRuntimeClasspath) > "$file.tmp"
    mv "$file.tmp" "$file"
    cat "$file"
}

mode="${1:?mode}"; shift
case "$mode" in
    gen)
        CP="$(classpath):$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-{api,cudf}-*.jar | tr '\n' ':')"
        exec "$JAVA_HOME/bin/java" -Xmx"$HEAP" --enable-preview -cp "$CP" \
            org.apache.iceberg.tornadovm.bench.GenerateTables "$@"
        ;;
    addcomposite)
        CP="$(classpath):$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-{api,cudf}-*.jar | tr '\n' ':')"
        exec "$JAVA_HOME/bin/java" -Xmx"$HEAP" --enable-preview -cp "$CP" \
            org.apache.iceberg.tornadovm.bench.AddCompositeTable "$@"
        ;;
    addequality)
        CP="$(classpath):$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-{api,cudf}-*.jar | tr '\n' ':')"
        exec "$JAVA_HOME/bin/java" -Xmx"$HEAP" --enable-preview -cp "$CP" \
            org.apache.iceberg.tornadovm.bench.AddEqualityDeletes "$@"
        ;;
    gpu)
        # MAIN=<class> runs another main class (a probe, say) with the same JVM settings.
        CP="$(classpath)"
        # libcudf decompresses a ZSTD page with one thread block; a large page (a dictionary of
        # high-cardinality keys, TPC-DS's ss_ticket_number) takes 100+ ms that way. HYBRID gives
        # the largest pages to host threads: equality-delete conversion and reads on TPC-DS 2x
        # faster, the 100M-row suite and sorted compaction unchanged.
        export LIBCUDF_HOST_DECOMPRESSION="${LIBCUDF_HOST_DECOMPRESSION:-HYBRID}"
        # The argfile puts TornadoVM on the module path; -da for TornadoVM packages, see build.gradle.
        # tornado.device.memory is TornadoVM's own device budget (4 GB by default); buffers a closed
        # plan released stay counted against it until reused by the same access type.
        # The heap stays under 32 GB: TornadoVM serializes object arguments (the path holders)
        # assuming compressed oops, which a larger heap turns off.
        # SDKMAN installs ship only the argfile's template, with ${TORNADOVM_HOME} to expand.
        ARGFILE="$TORNADOVM_HOME/tornado-argfile"
        if [ ! -f "$ARGFILE" ]; then
            ARGFILE="${TMPDIR:-/tmp}/tornado-argfile.$$"
            sed "s|\${TORNADOVM_HOME}|$TORNADOVM_HOME|g" "$TORNADOVM_HOME/tornado-argfile.template" > "$ARGFILE"
        fi
        exec "$JAVA_HOME/bin/java" @"$ARGFILE" --enable-native-access=ALL-UNNAMED -Xmx"${GPU_HEAP:-24g}" \
            -Dtornado.device.memory="${TORNADO_DEVICE_MEMORY:-20GB}" --add-opens=java.base/java.nio=ALL-UNNAMED \
            -da:tornado... -da:uk.ac.manchester.tornado... -cp "$CP" \
            ${MAIN:-org.apache.iceberg.tornadovm.bench.MorBench} $( [ -n "${MAIN:-}" ] && echo "$@" || echo "$1 gpu ${*:2}" )
        ;;
    cpu|generics)
        CP="$(classpath):$(ls "$TORNADOVM_HOME"/share/java/tornado/tornado-{api,cudf}-*.jar | tr '\n' ':')"
        # MAIN=<class> runs another benchmark (ConvertBench, say) in this plain JVM, whose heap is not
        # capped for TornadoVM; its arguments are passed as given.
        if [ -n "${MAIN:-}" ]; then
            exec "$JAVA_HOME/bin/java" -Xmx"$HEAP" --enable-preview --add-opens=java.base/java.nio=ALL-UNNAMED -Dthreads="${THREADS:-$(nproc)}" -cp "$CP" \
                "$MAIN" "$@"
        fi
        exec "$JAVA_HOME/bin/java" -Xmx"$HEAP" --enable-preview --add-opens=java.base/java.nio=ALL-UNNAMED -Dthreads="${THREADS:-$(nproc)}" -cp "$CP" \
            org.apache.iceberg.tornadovm.bench.MorBench "$1" "$mode" "${@:2}"
        ;;
    spark)
        : "${SPARK_HOME:?set SPARK_HOME}"; : "${ICEBERG_SPARK_JAR:?set ICEBERG_SPARK_JAR}"
        exec "$SPARK_HOME/bin/spark-submit" --master "local[${THREADS:-$(nproc)}]" \
            --driver-memory "$HEAP" --jars "$ICEBERG_SPARK_JAR" ${SPARK_CONF:-} \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse="$(dirname "$1")" \
            --conf spark.ui.enabled=false \
            "$HERE/${BENCH_SCRIPT:-spark_bench.py}" "$@"
        ;;
    rapids)
        # Spark with the RAPIDS Accelerator on the same GPU, with the newest Iceberg runtime RAPIDS
        # supports (ICEBERG_RAPIDS_JAR; 26.08 takes 1.6, 1.9, 1.10); NOT_ON_GPU explains every operator it
        # leaves on the CPU.
        : "${SPARK_HOME:?set SPARK_HOME}"; : "${ICEBERG_SPARK_JAR:?set ICEBERG_SPARK_JAR}"; : "${RAPIDS_JAR:?set RAPIDS_JAR}"
        ENGINE=rapids exec "$SPARK_HOME/bin/spark-submit" --master "local[${THREADS:-$(nproc)}]" \
            --driver-memory "$HEAP" --jars "${ICEBERG_RAPIDS_JAR:-$ICEBERG_SPARK_JAR},$RAPIDS_JAR" \
            --conf spark.plugins=com.nvidia.spark.SQLPlugin \
            --conf spark.rapids.sql.enabled=true \
            --conf spark.rapids.sql.explain="${RAPIDS_EXPLAIN:-NOT_ON_GPU}" \
            --conf spark.rapids.sql.concurrentGpuTasks="${RAPIDS_TASKS:-2}" \
            --conf spark.rapids.memory.pinnedPool.size=4g \
            --conf spark.rapids.sql.format.iceberg.enabled=true ${RAPIDS_CONF:-} \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse="$(dirname "$1")" \
            --conf spark.ui.enabled=false \
            "$HERE/${BENCH_SCRIPT:-spark_bench.py}" "$@"
        ;;
    spark-gpu)
        # Spark with Iceberg's reader on the GPU (module iceberg-tornadovm-spark): Iceberg's unshaded
        # modules and the GPU path on the driver class path, ahead of Spark's jars, and TornadoVM in
        # the driver JVM through an argfile whose module path leaves out the jars that clash with
        # Spark's (see SPARK_GPU_ARGFILE). The GPU JVM keeps its heap under 32 GB.
        # Defaults: the bundle of :iceberg-tornadovm-spark:sparkGpuBundle (spark/build/spark-gpu).
        # SPARK_GPU_CONF adds Spark options (e.g. "--conf spark.iceberg.gpu.scanLog=/tmp/scans").
        : "${SPARK_HOME:?set SPARK_HOME}"
        BUNDLE="$HERE/spark/build/spark-gpu"
        SPARK_GPU_CP="${SPARK_GPU_CP:-$(ls "$BUNDLE"/jars/*.jar 2>/dev/null | paste -sd: -)}"
        SPARK_GPU_ARGFILE="${SPARK_GPU_ARGFILE:-$BUNDLE/tornado-argfile}"
        : "${SPARK_GPU_CP:?run :iceberg-tornadovm-spark:sparkGpuBundle or set SPARK_GPU_CP}"
        # TornadoVM serializes object arguments assuming compressed oops; with 16-byte object alignment
        # the JVM keeps them up to a 64 GB heap, so the GPU JVM gets the same heap as plain Spark.
        GPU_JVM="--enable-preview @$SPARK_GPU_ARGFILE --enable-native-access=ALL-UNNAMED -XX:ObjectAlignmentInBytes=16 -Dtornado.device.memory=${GPU_DEVICE_MEMORY:-20GB} -da:tornado... -da:uk.ac.manchester.tornado... ${SPARK_GPU_JAVA_OPTS:-}"
        MASTER="${SPARK_MASTER:-local[${THREADS:-$(nproc)}]}"
        EXECUTORS=()
        if [[ "$MASTER" != local\[* && "$MASTER" != local ]]; then
            # Cluster (e.g. SPARK_MASTER=local-cluster[1,16,32768] to test on one machine): the
            # executors run the GPU work, with the driver's class path, JVM options and libraries.
            EXECUTORS=(--conf spark.executor.extraClassPath="$SPARK_GPU_CP"
                       --conf spark.executor.extraJavaOptions="$GPU_JVM"
                       --conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}"
                       --conf spark.executor.memory="${GPU_EXECUTOR_MEMORY:-24g}")
        fi
        ENGINE=spark-gpu exec "$SPARK_HOME/bin/spark-submit" --master "$MASTER" "${EXECUTORS[@]}" \
            --driver-memory "${GPU_HEAP:-48g}" --driver-class-path "$SPARK_GPU_CP" \
            --conf spark.driver.extraJavaOptions="$GPU_JVM" \
            --conf spark.iceberg.gpu.enabled=true \
            --conf spark.sql.iceberg.partition-reader-factory-provider=tornadovm \
            --conf spark.sql.iceberg.rewrite-runner-provider=tornadovm \
            --conf spark.iceberg.gpu.aggregate.enabled="${SPARK_GPU_AGGREGATE:-true}" ${SPARK_GPU_CONF:-} \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.spark.sql.iceberg.gpu.GpuAggregateExtensions \
            --conf spark.sql.catalog.default_iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.default_iceberg.type=hadoop \
            --conf spark.sql.catalog.default_iceberg.warehouse="$(dirname "$1")" \
            --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.local.type=hadoop \
            --conf spark.sql.catalog.local.warehouse="$(dirname "$1")" \
            --conf spark.driver.maxResultSize=0 \
            --conf spark.ui.enabled=false \
            "$HERE/${BENCH_SCRIPT:-spark_bench.py}" "$@"
        ;;
    *)
        echo "unknown mode $mode" >&2; exit 2 ;;
esac
