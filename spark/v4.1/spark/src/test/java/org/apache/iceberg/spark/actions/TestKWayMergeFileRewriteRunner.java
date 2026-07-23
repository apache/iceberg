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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestKWayMergeFileRewriteRunner extends TestBase {

  @TempDir private File tableDir;

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "c1", Types.IntegerType.get()),
          Types.NestedField.optional(2, "c2", Types.StringType.get()),
          Types.NestedField.optional(3, "c3", Types.StringType.get()));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("c1").build();

  private String tableLocation = null;

  @BeforeAll
  static void setupSpark() {
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  void setupTableLocation() {
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  void testKWayMergeUnpartitionedTable() {
    Table table = createSortedTable(4);
    int initialFiles = fileCount(table);
    assertThat(initialFiles).as("Table should have multiple files").isGreaterThanOrEqualTo(4);
    List<Object[]> expectedRecords = currentData();

    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .kWayMerge()
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite all data files")
        .isEqualTo(initialFiles);
    assertThat(result.addedDataFilesCount())
        .as("Action should add at least 1 data file")
        .isGreaterThanOrEqualTo(1);

    table.refresh();
    assertThat(fileCount(table))
        .as("File count should decrease or stay same")
        .isLessThanOrEqualTo(initialFiles);
    List<Object[]> actual = currentData();
    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  void testKWayMergePartitionedTable() {
    Table table = createSortedTablePartitioned(4, 2);
    int initialFiles = fileCount(table);
    assertThat(initialFiles).as("Table should have multiple files").isGreaterThanOrEqualTo(8);
    List<Object[]> expectedRecords = currentData();

    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .kWayMerge()
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite data files")
        .isEqualTo(initialFiles);
    assertThat(result.addedDataFilesCount()).as("Action should add files").isGreaterThan(0);

    List<Object[]> actual = currentData();
    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  void testKWayMergePreservesSort() {
    Table table = createSortedTable(3);
    assertThat(fileCount(table)).as("Table should have multiple files").isGreaterThanOrEqualTo(3);

    actions()
        .rewriteDataFiles(table)
        .kWayMerge()
        .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
        .execute();

    table.refresh();

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.file().sortOrderId())
            .as("Output files must have valid sort order ID")
            .isGreaterThan(0);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testKWayMergeWithFilter() {
    Table table = createSortedTablePartitioned(4, 2);
    assertThat(fileCount(table)).as("Table should have multiple files").isGreaterThanOrEqualTo(8);
    List<Object[]> expectedRecords = currentData();

    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .kWayMerge()
            .filter(Expressions.equal("c1", 1))
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .execute();

    assertThat(result.rewrittenDataFilesCount())
        .as("Action should rewrite files in filtered partition")
        .isGreaterThan(0);

    List<Object[]> actual = currentData();
    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  void testKWayMergeRejectsUnsortedTable() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, options, tableLocation);

    writeUnsortedRecords(table, 3);
    table.refresh();
    assertThat(fileCount(table)).as("Table should have files").isGreaterThanOrEqualTo(3);

    assertThatThrownBy(
            () ->
                actions()
                    .rewriteDataFiles(table)
                    .kWayMerge()
                    .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("K-way merge requires a table sort order");
  }

  @Test
  void testKWayMergeRejectsUnsortedFiles() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);

    writeUnsortedRecords(table, 3);
    table.refresh();
    assertThat(fileCount(table)).as("Table should have files").isGreaterThanOrEqualTo(3);

    assertThatThrownBy(
            () ->
                actions()
                    .rewriteDataFiles(table)
                    .kWayMerge()
                    .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
                    .execute())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("K-way merge requires all files to be pre-sorted");
  }

  @Test
  void testKWayMergeValidOptions() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);

    SparkKWayMergeFileRewriteRunner runner = new SparkKWayMergeFileRewriteRunner(spark, table);

    assertThat(runner.validOptions())
        .as("Runner must report all supported options")
        .contains(
            SparkKWayMergeFileRewriteRunner.RANGE_PARALLELISM_ENABLED,
            SparkKWayMergeFileRewriteRunner.RANGES_PER_GROUP,
            SparkKWayMergeFileRewriteRunner.MIN_FILES_FOR_RANGE_PARALLELISM);
  }

  @Test
  void testKWayMergeInvalidOptionRangesPerGroup() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);

    SparkKWayMergeFileRewriteRunner runner = new SparkKWayMergeFileRewriteRunner(spark, table);

    assertThatThrownBy(
            () ->
                runner.init(ImmutableMap.of(SparkKWayMergeFileRewriteRunner.RANGES_PER_GROUP, "0")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must be > 0");
  }

  @Test
  void testKWayMergeDisabledRangeParallelism() {
    Table table = createSortedTable(3);
    assertThat(fileCount(table)).as("Table should have files").isGreaterThanOrEqualTo(3);
    List<Object[]> expectedRecords = currentData();

    RewriteDataFiles.Result result =
        actions()
            .rewriteDataFiles(table)
            .kWayMerge()
            .option(SizeBasedFileRewritePlanner.MIN_INPUT_FILES, "1")
            .option(SparkKWayMergeFileRewriteRunner.RANGE_PARALLELISM_ENABLED, "false")
            .execute();

    assertThat(result.rewrittenDataFilesCount()).as("Action should rewrite files").isGreaterThan(0);

    List<Object[]> actual = currentData();
    assertEquals("Rows must match", expectedRecords, actual);
  }

  @Test
  void testKWayMergeDescription() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);

    SparkKWayMergeFileRewriteRunner runner = new SparkKWayMergeFileRewriteRunner(spark, table);
    assertThat(runner.description()).isEqualTo("K-WAY-MERGE");
  }

  private SparkActions actions() {
    return SparkActions.get();
  }

  private Table createSortedTable(int numFiles) {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(20 * 1024))
        .commit();

    writeSortedRecords(table, numFiles);
    table.refresh();
    return table;
  }

  private Table createSortedTablePartitioned(int partitions, int filesPerPartition) {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Map<String, String> options = ImmutableMap.of(TableProperties.FORMAT_VERSION, "2");
    Table table = TABLES.create(SCHEMA, spec, SORT_ORDER, options, tableLocation);
    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, Integer.toString(20 * 1024))
        .commit();

    for (int p = 0; p < partitions; p++) {
      for (int f = 0; f < filesPerPartition; f++) {
        int partition = p;
        List<ThreeColumnRecord> records =
            IntStream.range(f * 100, (f + 1) * 100)
                .mapToObj(i -> new ThreeColumnRecord(partition, "foo" + i, "bar" + i))
                .collect(Collectors.toList());
        Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(1);
        df.select("c1", "c2", "c3")
            .sortWithinPartitions("c1", "c2")
            .write()
            .format("iceberg")
            .mode("append")
            .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "true")
            .save(tableLocation);
      }
    }

    table.refresh();
    return table;
  }

  private void writeSortedRecords(Table table, int numFiles) {
    for (int f = 0; f < numFiles; f++) {
      List<ThreeColumnRecord> records =
          IntStream.range(f * 200, (f + 1) * 200)
              .mapToObj(i -> new ThreeColumnRecord(i, "foo" + i, "bar" + i))
              .collect(Collectors.toList());
      Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(1);
      df.select("c1", "c2", "c3")
          .sortWithinPartitions("c1", "c2")
          .write()
          .format("iceberg")
          .mode("append")
          .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "true")
          .save(tableLocation);
    }
  }

  private void writeUnsortedRecords(Table table, int numFiles) {
    List<ThreeColumnRecord> records =
        IntStream.range(0, 500)
            .mapToObj(i -> new ThreeColumnRecord(500 - i, "foo" + i, "bar" + i))
            .collect(Collectors.toList());
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).repartition(numFiles);
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
        .save(tableLocation);
  }

  private List<Object[]> currentData() {
    Dataset<Row> ds = spark.read().format("iceberg").load(tableLocation);
    return ds.orderBy("c1", "c2", "c3").collectAsList().stream()
        .map(
            row ->
                new Object[] {
                  row.isNullAt(0) ? null : row.getInt(0), row.getString(1), row.getString(2)
                })
        .collect(Collectors.toList());
  }

  private int fileCount(Table table) {
    table.refresh();
    int numFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        numFiles++;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return numFiles;
  }
}
