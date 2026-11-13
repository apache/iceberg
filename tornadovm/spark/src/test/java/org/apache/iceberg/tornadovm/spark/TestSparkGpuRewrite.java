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

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.actions.SparkActions;
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
 * Iceberg's own {@code rewrite_data_files} action, bin-pack, with the GPU on: the device rewrites
 * the file groups of a table it can write, Spark those of one it cannot, and either way the table
 * reads back the same with no delete file left.
 */
public class TestSparkGpuRewrite {
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
  }

  private static List<String> rows(String location) {
    List<String> out = Lists.newArrayList();
    for (Row row : spark.read().format("iceberg").load(location).collectAsList()) {
      out.add(row.toString());
    }
    out.sort(String::compareTo);
    return out;
  }

  /** {@code withBoolean}: a column type the device does not write, so Spark's runner takes it. */
  private static Table table(Path temp, String name, boolean withBoolean) {
    List<Types.NestedField> fields =
        Lists.newArrayList(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "qty", Types.IntegerType.get()),
            Types.NestedField.required(3, "price", Types.DecimalType.of(7, 2)),
            Types.NestedField.optional(4, "net", Types.DecimalType.of(15, 2)),
            Types.NestedField.optional(5, "ratio", Types.FloatType.get()),
            Types.NestedField.optional(6, "score", Types.DoubleType.get()),
            Types.NestedField.optional(7, "day", Types.DateType.get()),
            Types.NestedField.optional(8, "note", Types.StringType.get()));
    if (withBoolean) {
      fields.add(Types.NestedField.optional(9, "flag", Types.BooleanType.get()));
    }
    Schema schema = new Schema(fields);
    Table table = MorTables.create(temp.resolve(name).toString(), schema, 2);
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 4; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              5000,
              i -> {
                long id = index * 1_000_000L + i;
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", id);
                record.setField("qty", i % 9 == 0 ? null : (int) (id % 100));
                record.setField(
                    "price", BigDecimal.valueOf((id * 7919) % 9_999_999 - 4_999_999, 2));
                record.setField(
                    "net", i % 5 == 0 ? null : BigDecimal.valueOf(9_999_999_999L - id * 1_234L, 2));
                record.setField("ratio", i % 7 == 0 ? null : (float) id / 3);
                record.setField("score", id * 0.5);
                record.setField("day", LocalDate.of(2026, 10, 1).plusDays(i % 40));
                record.setField(
                    "note", i % 8 == 0 ? null : "note " + id + " Ωé " + "x".repeat((int) (i % 20)));
                if (withBoolean) {
                  record.setField("flag", i % 2 == 0);
                }
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table,
        files,
        ImmutableList.of(
            LongStream.range(0, 5000).filter(i -> i % 6 == 0).toArray(),
            new long[] {1, 2, 3},
            new long[0],
            LongStream.range(0, 5000).filter(i -> i % 13 == 0).toArray()));
    MorTables.deleteEqual(table, "id", ImmutableList.of(10L, 2_000_011L));
    table.refresh();
    return table;
  }

  private static void assertNoDeletes(Table table) throws java.io.IOException {
    table.refresh();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.deletes()).as("delete files after the rewrite").isEmpty();
      }
    }
  }

  @Test
  public void testRewriteOnTheDeviceAndFallback() throws Exception {
    Table onDevice = table(temp, "device", false);
    Table onSpark = table(temp, "spark", true);
    assertThat(GpuFileRewriteRunnerProvider.unsupported(onDevice)).isNull();
    assertThat(GpuFileRewriteRunnerProvider.unsupported(onSpark)).contains("flag");

    List<String> deviceBefore = rows(onDevice.location());
    List<String> sparkBefore = rows(onSpark.location());

    gpu(true);
    long groups = GpuFileRewriteRunnerProvider.GROUPS.get();
    RewriteDataFiles.Result deviceResult;
    long deviceGroups;
    try {
      deviceResult =
          SparkActions.get(spark)
              .rewriteDataFiles(onDevice)
              .option("rewrite-all", "true")
              .execute();
      deviceGroups = GpuFileRewriteRunnerProvider.GROUPS.get() - groups;
      SparkActions.get(spark).rewriteDataFiles(onSpark).option("rewrite-all", "true").execute();
    } finally {
      gpu(false);
    }

    assertThat(deviceGroups).as("file groups rewritten on the device").isGreaterThan(0);
    assertThat(GpuFileRewriteRunnerProvider.GROUPS.get() - groups)
        .as("no group of the boolean table on the device")
        .isEqualTo(deviceGroups);
    assertThat(deviceResult.rewrittenDataFilesCount()).isEqualTo(4);
    assertNoDeletes(onDevice);
    assertNoDeletes(onSpark);
    assertThat(rows(onDevice.location())).containsExactlyElementsOf(deviceBefore);
    assertThat(rows(onSpark.location())).containsExactlyElementsOf(sparkBefore);
  }
}
