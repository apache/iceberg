/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.source.parquet;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.GpuBenchmarks;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link IcebergSourceParquetPosDeleteBenchmark} on the GPU: the same table, data file, delete
 * files and parameters, read by the TornadoVM + cuDF merge-on-read scan instead of Spark. Every
 * live row is materialized on the host with all seven columns and every value is read, as Spark's
 * {@code toRdd().count()} builds every row. To run it: <code>
 *   ./gradlew -DsparkVersions=4.2 :iceberg-spark:iceberg-spark-4.2_2.13:jmh
 *       -PjmhIncludeRegex=IcebergSourceParquetPosDeleteGpuBenchmark.readIcebergGpu
 *       -PjmhJvmArgsAppend="@$TORNADOVM_HOME/tornado-argfile -Xmx24g ..."
 * </code>
 */
public class IcebergSourceParquetPosDeleteGpuBenchmark
    extends IcebergSourceParquetPosDeleteBenchmark {
  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergSourceParquetPosDeleteGpuBenchmark.class);

  private DeviceMergeOnReadScan scan;

  /**
   * One scan for the trial, after the superclass wrote its table: like Spark's warm JVM, it keeps
   * its compiled kernels and plans across iterations, and the warm-up iterations pay the JIT.
   */
  @Setup(Level.Trial)
  public void setupGpu() {
    table().refresh();
    scan = DeviceMergeOnReadScan.of(table());
    // The scan must agree with Spark on the same table before it is timed.
    long spark = spark().read().format("iceberg").load(table().location()).count();
    long gpu = GpuBenchmarks.materializeAll(scan, table().schema(), new AtomicLong());
    if (gpu != spark) {
      throw new IllegalStateException("GPU scan read " + gpu + " live rows, Spark " + spark);
    }
    LOG.info("live rows: GPU {}, Spark {}", gpu, spark);
  }

  @TearDown(Level.Trial)
  public void tearDownGpu() {
    scan.close();
  }

  @Benchmark
  @Threads(1)
  public void readIcebergGpu(Blackhole blackhole) {
    AtomicLong checksum = new AtomicLong();
    blackhole.consume(GpuBenchmarks.materializeAll(scan, table().schema(), checksum));
    blackhole.consume(checksum.get());
  }
}
