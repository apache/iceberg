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
package org.apache.iceberg.tornadovm.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.DeviceMergeOnReadScan;
import org.apache.iceberg.tornadovm.bench.MorTables;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkEnv;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spark SQL over a merge-on-read table, with the GPU reader on and off: the same rows, the same
 * answers, and the GPU path really taken.
 */
public class TestSparkGpuReads {
  private static SparkSession spark;

  @TempDir private Path temp;

  @BeforeAll
  public static void startSpark() {
    assumeThat(DeviceMergeOnReadScan.isAvailable()).as("TornadoVM with the cuDF shim").isTrue();
    spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            // Path-based reads go through Iceberg's default catalog: a Hadoop catalog, no Hive.
            .config("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.default_iceberg.type", "hadoop")
            .config("spark.sql.iceberg.partition-reader-factory-provider", "tornadovm")
            .config("spark.sql.iceberg.rewrite-runner-provider", "tornadovm")
            .config(
                "spark.sql.catalog.default_iceberg.warehouse",
                System.getProperty("java.io.tmpdir") + "/iceberg-gpu-spark-test")
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static void gpu(boolean on) {
    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.ENABLED, Boolean.toString(on));
    // These tables are small: read them on the device anyway.
    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.MIN_ROWS, "0");
  }

  private static List<String> sorted(List<Row> rows) {
    List<String> out = Lists.newArrayList();
    for (Row row : rows) {
      out.add(row.toString());
    }
    out.sort(String::compareTo);
    return out;
  }

  private static long[] positions(long rows, double fraction, long seed) {
    Random random = new Random(seed);
    return LongStream.range(0, rows).filter(i -> random.nextDouble() < fraction).toArray();
  }

  @Test
  public void testSameRowsAndAnswersWithTheGpuReader() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "longCol", Types.LongType.get()),
            Types.NestedField.required(2, "intCol", Types.IntegerType.get()),
            Types.NestedField.required(3, "floatCol", Types.FloatType.get()),
            Types.NestedField.optional(4, "doubleCol", Types.DoubleType.get()),
            Types.NestedField.optional(6, "dateCol", Types.DateType.get()),
            Types.NestedField.optional(7, "timestampCol", Types.TimestampType.withZone()),
            Types.NestedField.optional(8, "stringCol", Types.StringType.get()));
    String location = temp.resolve("t").toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              5000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                long id = index * 1_000_000L + i;
                record.setField("longCol", id);
                record.setField("intCol", (int) (i % 97));
                record.setField("floatCol", (float) id / 7);
                record.setField("doubleCol", i % 9 == 0 ? null : id * 0.5);
                record.setField("dateCol", java.time.LocalDate.of(2026, 10, 1).plusDays(i % 30));
                record.setField(
                    "timestampCol",
                    i % 13 == 0
                        ? null
                        : java.time.OffsetDateTime.of(
                                2026, 10, 9, 18, 30, 0, 0, java.time.ZoneOffset.UTC)
                            .plusSeconds(i));
                record.setField(
                    "stringCol",
                    i % 11 == 0 ? null : "row-" + id + "-é-" + "x".repeat((int) (i % 25)));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(positions(5000, 0.1, 1), positions(5000, 0.3, 2), new long[0]));
    MorTables.deleteEqual(table, "longCol", ImmutableList.of(3L, 1_000_004L, 2_000_005L));

    String all = "SELECT * FROM t";
    String grouped =
        "SELECT intCol, count(*), sum(doubleCol), min(stringCol), max(timestampCol), sum(floatCol)"
            + " FROM t WHERE longCol % 3 <> 1 GROUP BY intCol";
    // Pushed to the scan: each task's residual filters on the device (the string term cannot,
    // and Spark applies all of it again).
    String filtered =
        "SELECT * FROM t WHERE dateCol >= DATE '2026-10-05' AND dateCol < DATE '2026-10-20'"
            + " AND doubleCol BETWEEN 100 AND 1500000 AND intCol < 50 AND floatCol > 10"
            + " AND timestampCol IS NOT NULL";
    String stringFiltered =
        "SELECT longCol FROM t WHERE stringCol > 'row-1' AND intCol IN (1, 2, 3)";
    spark.read().format("iceberg").load(location).createOrReplaceTempView("t");

    gpu(false);
    List<String> cpuRows = sorted(spark.sql(all).collectAsList());
    List<String> cpuGroups = sorted(spark.sql(grouped).collectAsList());
    List<String> cpuFiltered = sorted(spark.sql(filtered).collectAsList());
    List<String> cpuStringFiltered = sorted(spark.sql(stringFiltered).collectAsList());

    gpu(true);
    long before = GpuPartitionReader.TASKS.get();
    List<String> gpuRows = sorted(spark.sql(all).collectAsList());
    List<String> gpuGroups = sorted(spark.sql(grouped).collectAsList());
    List<String> gpuFiltered = sorted(spark.sql(filtered).collectAsList());
    List<String> gpuStringFiltered = sorted(spark.sql(stringFiltered).collectAsList());
    long gpuTasks = GpuPartitionReader.TASKS.get() - before;
    gpu(false);

    assertThat(gpuTasks).as("Spark tasks read on the GPU").isGreaterThan(0);
    assertThat(cpuRows).isNotEmpty();
    assertThat(gpuRows).containsExactlyElementsOf(cpuRows);
    assertThat(gpuGroups).containsExactlyElementsOf(cpuGroups);
    assertThat(cpuFiltered).isNotEmpty().hasSizeLessThan(cpuRows.size());
    assertThat(gpuFiltered).containsExactlyElementsOf(cpuFiltered);
    assertThat(cpuStringFiltered).isNotEmpty();
    assertThat(gpuStringFiltered).containsExactlyElementsOf(cpuStringFiltered);
  }

  @Test
  public void testReadPolicyLeavesPositionDeletesToIcebergsReader() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "longCol", Types.LongType.get()),
            Types.NestedField.required(2, "intCol", Types.IntegerType.get()));
    String location = temp.resolve("positions-only").toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 2; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              5000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("longCol", index * 1_000_000L + i);
                record.setField("intCol", (int) (i % 97));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(positions(5000, 0.2, 3), positions(5000, 0.05, 4)));
    spark.read().format("iceberg").load(location).createOrReplaceTempView("p");
    String all = "SELECT * FROM p";

    gpu(false);
    List<String> cpuRows = sorted(spark.sql(all).collectAsList());

    gpu(true);
    long before = GpuPartitionReader.TASKS.get();
    List<String> autoRows = sorted(spark.sql(all).collectAsList());
    long autoTasks = GpuPartitionReader.TASKS.get() - before;

    SparkEnv.get().conf().set(GpuPartitionReaderFactoryProvider.READ_POLICY, "always");
    before = GpuPartitionReader.TASKS.get();
    List<String> alwaysRows = sorted(spark.sql(all).collectAsList());
    long alwaysTasks = GpuPartitionReader.TASKS.get() - before;
    SparkEnv.get().conf().remove(GpuPartitionReaderFactoryProvider.READ_POLICY);
    gpu(false);

    assertThat(autoTasks).as("position deletes stay on Iceberg's reader under auto").isZero();
    assertThat(alwaysTasks).as("Spark tasks read on the GPU with always").isGreaterThan(0);
    assertThat(cpuRows)
        .hasSize(10000 - positions(5000, 0.2, 3).length - positions(5000, 0.05, 4).length);
    assertThat(autoRows).containsExactlyElementsOf(cpuRows);
    assertThat(alwaysRows).containsExactlyElementsOf(cpuRows);
  }

  /**
   * A task group the device refuses as it plans it -- here every file's strings beyond their share
   * of a lowered string buffer, as one file of ClickBench's hits with equality deletes was of the
   * real one in q24 -- is read by Iceberg's reader instead, with the same rows, and the query does
   * not fail.
   */
  @Test
  public void testGroupTheDeviceRefusesIsReadByIcebergsReader() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "longCol", Types.LongType.get()),
            Types.NestedField.optional(2, "url", Types.StringType.get()),
            Types.NestedField.required(3, "title", Types.StringType.get()));
    String location = temp.resolve("refused").toString();
    Table table = MorTables.create(location, schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              4000,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("longCol", index * 1_000_000L + i);
                record.setField(
                    "url", i % 7 == 0 ? null : "https://example.com/" + "u".repeat((int) (i % 60)));
                record.setField("title", "title " + index + " " + i);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deleteEqual(table, "longCol", ImmutableList.of(5L, 1_000_006L, 2_000_007L));
    spark.read().format("iceberg").load(location).createOrReplaceTempView("r");
    String all = "SELECT * FROM r";

    gpu(false);
    List<String> cpuRows = sorted(spark.sql(all).collectAsList());

    gpu(true);
    long saved = org.apache.iceberg.tornadovm.DeviceMergeOnReadScan.stringBuffer;
    // Smaller than any file's strings: the device refuses every group.
    org.apache.iceberg.tornadovm.DeviceMergeOnReadScan.stringBuffer = 8192;
    long before = GpuPartitionReader.REFUSED.get();
    List<String> refusedRows;
    try {
      refusedRows = sorted(spark.sql(all).collectAsList());
    } finally {
      org.apache.iceberg.tornadovm.DeviceMergeOnReadScan.stringBuffer = saved;
    }
    long refused = GpuPartitionReader.REFUSED.get() - before;
    before = GpuPartitionReader.TASKS.get();
    List<String> gpuRows = sorted(spark.sql(all).collectAsList());
    long gpuTasks = GpuPartitionReader.TASKS.get() - before;
    gpu(false);

    assertThat(cpuRows).hasSize(3 * 4000 - 3);
    assertThat(refused).as("Spark tasks the device refused").isGreaterThan(0);
    assertThat(refusedRows).containsExactlyElementsOf(cpuRows);
    assertThat(gpuTasks).as("Spark tasks read on the GPU with the whole buffer").isGreaterThan(0);
    assertThat(gpuRows).containsExactlyElementsOf(cpuRows);
  }
}
