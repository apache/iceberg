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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestMetadataTableScans extends MetadataTableScanTestBase {

  public TestMetadataTableScans(int formatVersion) {
    super(formatVersion);
  }

  private void preparePartitionedTable() {
    preparePartitionedTableData();

    if (formatVersion == 2) {
      table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_C2_DELETES).commit();
      table.newRowDelta().addDeletes(FILE_D2_DELETES).commit();
    }
  }

  private void preparePartitionedTableData() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_C).commit();
    table.newFastAppend().appendFile(FILE_D).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
  }

  @Test
  public void testManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testManifestsTableAlwaysIgnoresResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);

    TableScan scan = manifestsTable.newScan().filter(Expressions.lessThan("length", 10000L));

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (FileScanTask task : tasks) {
        Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
      }
    }
  }

  @Test
  public void testDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan1 = dataFilesTable.newScan().filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        dataFilesTable.newScan().filter(Expressions.equal("record_count", 1)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);

    TableScan scan1 = manifestEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        manifestEntriesTable
            .newScan()
            .filter(Expressions.equal("snapshot_id", 1L))
            .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);

    TableScan scan1 = allDataFilesTable.newScan().filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allDataFilesTable.newScan().filter(Expressions.equal("record_count", 1)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);

    TableScan scan1 = allEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllManifestsTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan1 = allManifestsTable.newScan().filter(Expressions.lessThan("length", 10000L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allManifestsTable
            .newScan()
            .filter(Expressions.lessThan("length", 10000L))
            .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testPartitionsTableScanNoFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    Types.StructType expected =
        new Schema(
                required(
                    1,
                    "partition",
                    Types.StructType.of(optional(1000, "data_bucket", Types.IntegerType.get()))))
            .asStruct();

    TableScan scanNoFilter = partitionsTable.newScan().select("partition.data_bucket");
    Assert.assertEquals(expected, scanNoFilter.schema().asStruct());
    CloseableIterable<FileScanTask> tasksNoFilter =
        PartitionsTable.planFiles((StaticTableScan) scanNoFilter);
    Assert.assertEquals(4, Iterators.size(tasksNoFilter.iterator()));
    validateIncludesPartitionScan(tasksNoFilter, 0);
    validateIncludesPartitionScan(tasksNoFilter, 1);
    validateIncludesPartitionScan(tasksNoFilter, 2);
    validateIncludesPartitionScan(tasksNoFilter, 3);
  }

  @Test
  public void testPartitionsTableScanWithProjection() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    Types.StructType expected =
        new Schema(required(3, "file_count", Types.IntegerType.get())).asStruct();

    TableScan scanWithProjection = partitionsTable.newScan().select("file_count");
    Assert.assertEquals(expected, scanWithProjection.schema().asStruct());
    CloseableIterable<FileScanTask> tasksWithProjection =
        PartitionsTable.planFiles((StaticTableScan) scanWithProjection);
    Assert.assertEquals(4, Iterators.size(tasksWithProjection.iterator()));
    validateIncludesPartitionScan(tasksWithProjection, 0);
    validateIncludesPartitionScan(tasksWithProjection, 1);
    validateIncludesPartitionScan(tasksWithProjection, 2);
    validateIncludesPartitionScan(tasksWithProjection, 3);
  }

  @Test
  public void testPartitionsTableScanNoStats() {
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    CloseableIterable<FileScanTask> tasksAndEq =
        PartitionsTable.planFiles((StaticTableScan) partitionsTable.newScan());
    for (FileScanTask fileTask : tasksAndEq) {
      Assert.assertNull(fileTask.file().columnSizes());
      Assert.assertNull(fileTask.file().valueCounts());
      Assert.assertNull(fileTask.file().nullValueCounts());
      Assert.assertNull(fileTask.file().lowerBounds());
      Assert.assertNull(fileTask.file().upperBounds());
    }
  }

  @Test
  public void testPartitionsTableScanAndFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression andEquals =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<FileScanTask> tasksAndEq =
        PartitionsTable.planFiles((StaticTableScan) scanAndEq);
    Assert.assertEquals(1, Iterators.size(tasksAndEq.iterator()));
    validateIncludesPartitionScan(tasksAndEq, 0);
  }

  @Test
  public void testPartitionsTableScanLtFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression ltAnd =
        Expressions.and(
            Expressions.lessThan("partition.data_bucket", 2),
            Expressions.greaterThan("record_count", 0));
    TableScan scanLtAnd = partitionsTable.newScan().filter(ltAnd);
    CloseableIterable<FileScanTask> tasksLtAnd =
        PartitionsTable.planFiles((StaticTableScan) scanLtAnd);
    Assert.assertEquals(2, Iterators.size(tasksLtAnd.iterator()));
    validateIncludesPartitionScan(tasksLtAnd, 0);
    validateIncludesPartitionScan(tasksLtAnd, 1);
  }

  @Test
  public void testPartitionsTableScanOrFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression or =
        Expressions.or(
            Expressions.equal("partition.data_bucket", 2),
            Expressions.greaterThan("record_count", 0));
    TableScan scanOr = partitionsTable.newScan().filter(or);
    CloseableIterable<FileScanTask> tasksOr = PartitionsTable.planFiles((StaticTableScan) scanOr);
    Assert.assertEquals(4, Iterators.size(tasksOr.iterator()));
    validateIncludesPartitionScan(tasksOr, 0);
    validateIncludesPartitionScan(tasksOr, 1);
    validateIncludesPartitionScan(tasksOr, 2);
    validateIncludesPartitionScan(tasksOr, 3);
  }

  @Test
  public void testPartitionsScanNotFilter() {
    preparePartitionedTable();
    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression not = Expressions.not(Expressions.lessThan("partition.data_bucket", 2));
    TableScan scanNot = partitionsTable.newScan().filter(not);
    CloseableIterable<FileScanTask> tasksNot = PartitionsTable.planFiles((StaticTableScan) scanNot);
    Assert.assertEquals(2, Iterators.size(tasksNot.iterator()));
    validateIncludesPartitionScan(tasksNot, 2);
    validateIncludesPartitionScan(tasksNot, 3);
  }

  @Test
  public void testPartitionsTableScanInFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression set = Expressions.in("partition.data_bucket", 2, 3);
    TableScan scanSet = partitionsTable.newScan().filter(set);
    CloseableIterable<FileScanTask> tasksSet = PartitionsTable.planFiles((StaticTableScan) scanSet);
    Assert.assertEquals(2, Iterators.size(tasksSet.iterator()));
    validateIncludesPartitionScan(tasksSet, 2);
    validateIncludesPartitionScan(tasksSet, 3);
  }

  @Test
  public void testPartitionsTableScanNotNullFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression unary = Expressions.notNull("partition.data_bucket");
    TableScan scanUnary = partitionsTable.newScan().filter(unary);
    CloseableIterable<FileScanTask> tasksUnary =
        PartitionsTable.planFiles((StaticTableScan) scanUnary);
    Assert.assertEquals(4, Iterators.size(tasksUnary.iterator()));
    validateIncludesPartitionScan(tasksUnary, 0);
    validateIncludesPartitionScan(tasksUnary, 1);
    validateIncludesPartitionScan(tasksUnary, 2);
    validateIncludesPartitionScan(tasksUnary, 3);
  }

  @Test
  public void testFilesTableScanWithDroppedPartition() throws IOException {
    preparePartitionedTable();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    // Here we need to specify target name as 'data_bucket_16'. If unspecified a default name will
    // be generated. As per
    // https://github.com/apache/iceberg/pull/4868 there's an inconsistency of doing this: in V2,
    // the above removed
    // data_bucket would be recycled in favor of data_bucket_16. By specifying the target name, we
    // explicitly require
    // data_bucket not to be recycled.
    table.updateSpec().addField("data_bucket_16", Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);
    TableScan scan = dataFilesTable.newScan();

    Schema schema = dataFilesTable.schema();
    Types.StructType actualType = schema.findField(DataFile.PARTITION_ID).type().asStructType();
    Types.StructType expectedType =
        Types.StructType.of(
            Types.NestedField.optional(1000, "data_bucket", Types.IntegerType.get()),
            Types.NestedField.optional(1001, "data_bucket_16", Types.IntegerType.get()),
            Types.NestedField.optional(1002, "data_trunc_2", Types.StringType.get()));
    Assert.assertEquals("Partition type must match", expectedType, actualType);
    Accessor<StructLike> accessor = schema.accessorForField(1000);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Set<Integer> results =
          StreamSupport.stream(tasks.spliterator(), false)
              .flatMap(fileScanTask -> Streams.stream(fileScanTask.asDataTask().rows()))
              .map(accessor::get)
              .map(i -> (Integer) i)
              .collect(Collectors.toSet());
      Assert.assertEquals("Partition value must match", Sets.newHashSet(0, 1, 2, 3), results);
    }
  }

  @Test
  public void testDeleteFilesTableSelection() throws IOException {
    Assume.assumeTrue("Only V2 Tables Support Deletes", formatVersion >= 2);

    table.newFastAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_A2_DELETES).commit();

    Table deleteFilesTable = new DeleteFilesTable(table.ops(), table);

    TableScan scan =
        deleteFilesTable
            .newScan()
            .filter(Expressions.equal("record_count", 1))
            .select("content", "record_count");
    validateTaskScanResiduals(scan, false);
    Types.StructType expected =
        new Schema(
                optional(
                    134,
                    "content",
                    Types.IntegerType.get(),
                    "Contents of the file: 0=data, 1=position deletes, 2=equality deletes"),
                required(
                    103, "record_count", Types.LongType.get(), "Number of records in the file"))
            .asStruct();
    Assert.assertEquals(expected, scan.schema().asStruct());
  }

  @Test
  public void testFilesTableReadableMetricsSchema() {
    Table filesTable = new FilesTable(table.ops(), table);
    Types.StructType actual = filesTable.newScan().schema().select("readable_metrics").asStruct();
    int highestId = filesTable.schema().highestFieldId();

    Types.StructType expected =
        Types.StructType.of(
            optional(
                highestId,
                "readable_metrics",
                Types.StructType.of(
                    Types.NestedField.optional(
                        highestId - 14,
                        "data",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                highestId - 13,
                                "column_size",
                                Types.LongType.get(),
                                "Total size on disk"),
                            Types.NestedField.optional(
                                highestId - 12,
                                "value_count",
                                Types.LongType.get(),
                                "Total count, including null and NaN"),
                            Types.NestedField.optional(
                                highestId - 11,
                                "null_value_count",
                                Types.LongType.get(),
                                "Null value count"),
                            Types.NestedField.optional(
                                highestId - 10,
                                "nan_value_count",
                                Types.LongType.get(),
                                "NaN value count"),
                            Types.NestedField.optional(
                                highestId - 9,
                                "lower_bound",
                                Types.StringType.get(),
                                "Lower bound"),
                            Types.NestedField.optional(
                                highestId - 8,
                                "upper_bound",
                                Types.StringType.get(),
                                "Upper bound")),
                        "Metrics for column data"),
                    Types.NestedField.optional(
                        highestId - 7,
                        "id",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                highestId - 6,
                                "column_size",
                                Types.LongType.get(),
                                "Total size on disk"),
                            Types.NestedField.optional(
                                highestId - 5,
                                "value_count",
                                Types.LongType.get(),
                                "Total count, including null and NaN"),
                            Types.NestedField.optional(
                                highestId - 4,
                                "null_value_count",
                                Types.LongType.get(),
                                "Null value count"),
                            Types.NestedField.optional(
                                highestId - 3,
                                "nan_value_count",
                                Types.LongType.get(),
                                "NaN value count"),
                            Types.NestedField.optional(
                                highestId - 2,
                                "lower_bound",
                                Types.IntegerType.get(),
                                "Lower bound"),
                            Types.NestedField.optional(
                                highestId - 1,
                                "upper_bound",
                                Types.IntegerType.get(),
                                "Upper bound")),
                        "Metrics for column id")),
                "Column metrics in readable form"));

    Assert.assertEquals("Dynamic schema for readable_metrics should match", actual, expected);
  }

  @Test
  public void testPartitionSpecEvolutionAdditive() {
    preparePartitionedTable();

    // Change spec and add two data files
    table.updateSpec().addField("id").commit();
    PartitionSpec newSpec = table.spec();

    // Add two data files with new spec
    PartitionKey data10Key = new PartitionKey(newSpec, table.schema());
    data10Key.set(0, 0); // data=0
    data10Key.set(1, 10); // id=10
    DataFile data10 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data10Key)
            .build();
    PartitionKey data11Key = new PartitionKey(newSpec, table.schema());
    data11Key.set(0, 1); // data=0
    data10Key.set(1, 11); // id=11
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data11Key)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    Table metadataTable = new PartitionsTable(table.ops(), table);
    Expression filter =
        Expressions.and(
            Expressions.equal("partition.id", 10), Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = PartitionsTable.planFiles((StaticTableScan) scan);

    // Four data files of old spec, one new data file of new spec
    Assert.assertEquals(5, Iterables.size(tasks));

    filter =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    scan = metadataTable.newScan().filter(filter);
    tasks = PartitionsTable.planFiles((StaticTableScan) scan);

    // 1 original data file written by old spec, plus 1 new data file written by new spec
    Assert.assertEquals(2, Iterables.size(tasks));
  }

  @Test
  public void testPartitionSpecEvolutionRemoval() {
    preparePartitionedTable();

    // Remove partition field
    table.updateSpec().removeField(Expressions.bucket("data", 16)).addField("id").commit();
    PartitionSpec newSpec = table.spec();

    // Add two data files with new spec
    // Partition Fields are replaced in V1 with void and actually removed in V2
    int partIndex = (formatVersion == 1) ? 1 : 0;
    PartitionKey data10Key = new PartitionKey(newSpec, table.schema());
    data10Key.set(partIndex, 10);
    DataFile data10 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data10Key)
            .build();
    PartitionKey data11Key = new PartitionKey(newSpec, table.schema());
    data11Key.set(partIndex, 11);
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data11Key)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    Table metadataTable = new PartitionsTable(table.ops(), table);
    Expression filter =
        Expressions.and(
            Expressions.equal("partition.id", 10), Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = PartitionsTable.planFiles((StaticTableScan) scan);

    // Four original files of original spec, one data file written by new spec
    Assert.assertEquals(5, Iterables.size(tasks));

    // Filter for a dropped partition spec field.  Correct behavior is that only old partitions are
    // returned.
    filter =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    scan = metadataTable.newScan().filter(filter);
    tasks = PartitionsTable.planFiles((StaticTableScan) scan);

    if (formatVersion == 1) {
      // 1 original data file written by old spec
      Assert.assertEquals(1, Iterables.size(tasks));
    } else {
      // 1 original data/delete files written by old spec, plus both of new data file/delete file
      // written by new spec
      //
      // Unlike in V1, V2 does not write (data=null) on newer files' partition data, so these cannot
      // be filtered out
      // early in scan planning here.
      //
      // However, these partition rows are filtered out later in Spark data filtering, as the newer
      // partitions
      // will have 'data=null' field added as part of normalization to the Partitions table final
      // schema.
      // The Partitions table final schema is a union of fields of all specs, including dropped
      // fields.
      Assert.assertEquals(3, Iterables.size(tasks));
    }
  }

  @Test
  public void testPartitionColumnNamedPartition() throws Exception {
    TestTables.clearTables();
    this.tableDir = temp.newFolder();
    tableDir.delete();

    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "partition", Types.IntegerType.get()));
    this.metadataDir = new File(tableDir, "metadata");
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("partition").build();

    DataFile par0 =
        DataFiles.builder(spec)
            .withPath("/path/to/data-0.parquet")
            .withFileSizeInBytes(10)
            .withPartition(TestHelpers.Row.of(0))
            .withRecordCount(1)
            .build();
    DataFile par1 =
        DataFiles.builder(spec)
            .withPath("/path/to/data-0.parquet")
            .withFileSizeInBytes(10)
            .withPartition(TestHelpers.Row.of(1))
            .withRecordCount(1)
            .build();
    DataFile par2 =
        DataFiles.builder(spec)
            .withPath("/path/to/data-0.parquet")
            .withFileSizeInBytes(10)
            .withPartition(TestHelpers.Row.of(2))
            .withRecordCount(1)
            .build();

    this.table = create(schema, spec);
    table.newFastAppend().appendFile(par0).commit();
    table.newFastAppend().appendFile(par1).commit();
    table.newFastAppend().appendFile(par2).commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression andEquals =
        Expressions.and(
            Expressions.equal("partition.partition", 0),
            Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<FileScanTask> tasksAndEq =
        PartitionsTable.planFiles((StaticTableScan) scanAndEq);
    Assert.assertEquals(1, Iterators.size(tasksAndEq.iterator()));
    validateIncludesPartitionScan(tasksAndEq, 0);
  }

  @Test
  public void testAllDataFilesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan =
        allDataFilesTable
            .newScan()
            .planWith(
                Executors.newFixedThreadPool(
                    1,
                    runnable -> {
                      Thread thread = new Thread(runnable);
                      thread.setName("plan-" + planThreadsIndex.getAndIncrement());
                      thread.setDaemon(
                          true); // daemon threads will be terminated abruptly when the JVM exits
                      return thread;
                    }));
    Assert.assertEquals(1, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testAllEntriesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan =
        allEntriesTable
            .newScan()
            .planWith(
                Executors.newFixedThreadPool(
                    1,
                    runnable -> {
                      Thread thread = new Thread(runnable);
                      thread.setName("plan-" + planThreadsIndex.getAndIncrement());
                      thread.setDaemon(
                          true); // daemon threads will be terminated abruptly when the JVM exits
                      return thread;
                    }));
    Assert.assertEquals(1, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testPartitionsTableScanWithPlanExecutor() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan =
        partitionsTable
            .newScan()
            .planWith(
                Executors.newFixedThreadPool(
                    1,
                    runnable -> {
                      Thread thread = new Thread(runnable);
                      thread.setName("plan-" + planThreadsIndex.getAndIncrement());
                      thread.setDaemon(
                          true); // daemon threads will be terminated abruptly when the JVM exits
                      return thread;
                    }));
    CloseableIterable<FileScanTask> tasks = PartitionsTable.planFiles((StaticTableScan) scan);
    Assert.assertEquals(4, Iterables.size(tasks));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testAllManifestsTableSnapshotGt() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.greaterThan("reference_snapshot_id", 2));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 3L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotGte() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.greaterThanOrEqual("reference_snapshot_id", 3));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 3L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotLt() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.lessThan("reference_snapshot_id", 3));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 2L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotLte() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.lessThanOrEqual("reference_snapshot_id", 2));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 2L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotEq() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.equal("reference_snapshot_id", 2));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 2L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotNotEq() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.notEqual("reference_snapshot_id", 2));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 3L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotIn() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.in("reference_snapshot_id", 1, 3));
    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 3L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotNotIn() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.notIn("reference_snapshot_id", 1, 3));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 2L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotAnd() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(
                Expressions.and(
                    Expressions.equal("reference_snapshot_id", 2),
                    Expressions.greaterThan("length", 0)));
    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 2L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotOr() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(
                Expressions.or(
                    Expressions.equal("reference_snapshot_id", 2),
                    Expressions.equal("reference_snapshot_id", 4)));
    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 2L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }

  @Test
  public void testAllManifestsTableSnapshotNot() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(Expressions.not(Expressions.equal("reference_snapshot_id", 2)));

    Assert.assertEquals(
        "Expected snapshots do not match",
        expectedManifestListPaths(table.snapshots(), 1L, 3L, 4L),
        actualManifestListPaths(manifestsTableScan));
  }
}
