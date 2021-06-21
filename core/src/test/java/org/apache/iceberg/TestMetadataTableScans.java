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

import java.io.IOException;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMetadataTableScans extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMetadataTableScans(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testManifestsTableAlwaysIgnoresResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);

    TableScan scan = manifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (FileScanTask task : tasks) {
        Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
      }
    }
  }

  @Test
  public void testDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan1 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);

    TableScan scan1 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);

    TableScan scan1 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);

    TableScan scan1 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllManifestsTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan1 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testPartitionsTableScanNoFilter() {
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    Types.StructType expected = new Schema(
        required(1, "partition", Types.StructType.of(
            optional(1000, "data_bucket", Types.IntegerType.get())))).asStruct();

    TableScan scanNoFilter = partitionsTable.newScan().select("partition.data_bucket");
    Assert.assertEquals(expected, scanNoFilter.schema().asStruct());
    CloseableIterable<FileScanTask> tasksNoFilter = PartitionsTable.planFiles((StaticTableScan) scanNoFilter);
    Assert.assertEquals(4, Iterators.size(tasksNoFilter.iterator()));
    validateIncludesPartitionScan(tasksNoFilter, 0);
    validateIncludesPartitionScan(tasksNoFilter, 1);
    validateIncludesPartitionScan(tasksNoFilter, 2);
    validateIncludesPartitionScan(tasksNoFilter, 3);
  }

  @Test
  public void testPartitionsTableScanAndFilter() {
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression andEquals = Expressions.and(
        Expressions.equal("partition.data_bucket", 0),
        Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<FileScanTask> tasksAndEq = PartitionsTable.planFiles((StaticTableScan) scanAndEq);
    Assert.assertEquals(1, Iterators.size(tasksAndEq.iterator()));
    validateIncludesPartitionScan(tasksAndEq, 0);
  }

  @Test
  public void testPartitionsTableScanLtFilter() {
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression ltAnd = Expressions.and(
        Expressions.lessThan("partition.data_bucket", 2),
        Expressions.greaterThan("record_count", 0));
    TableScan scanLtAnd = partitionsTable.newScan().filter(ltAnd);
    CloseableIterable<FileScanTask> tasksLtAnd = PartitionsTable.planFiles((StaticTableScan) scanLtAnd);
    Assert.assertEquals(2, Iterators.size(tasksLtAnd.iterator()));
    validateIncludesPartitionScan(tasksLtAnd, 0);
    validateIncludesPartitionScan(tasksLtAnd, 1);
  }

  @Test
  public void testPartitionsTableScanOrFilter() {
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression or = Expressions.or(
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
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();
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
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

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
    table.newFastAppend()
        .appendFile(FILE_PARTITION_0)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_1)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_2)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_PARTITION_3)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression unary = Expressions.notNull("partition.data_bucket");
    TableScan scanUnary = partitionsTable.newScan().filter(unary);
    CloseableIterable<FileScanTask> tasksUnary = PartitionsTable.planFiles((StaticTableScan) scanUnary);
    Assert.assertEquals(4, Iterators.size(tasksUnary.iterator()));
    validateIncludesPartitionScan(tasksUnary, 0);
    validateIncludesPartitionScan(tasksUnary, 1);
    validateIncludesPartitionScan(tasksUnary, 2);
    validateIncludesPartitionScan(tasksUnary, 3);
  }

  @Test
  public void testDataFilesTableSelection() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .select("content", "record_count");
    validateTaskScanResiduals(scan, false);
    Types.StructType expected = new Schema(
        optional(134, "content", Types.IntegerType.get(),
                 "Contents of the file: 0=data, 1=position deletes, 2=equality deletes"),
        required(103, "record_count", Types.LongType.get(), "Number of records in the file")
    ).asStruct();
    Assert.assertEquals(expected, scan.schema().asStruct());
  }

  private void validateTaskScanResiduals(TableScan scan, boolean ignoreResiduals) throws IOException {
    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          if (ignoreResiduals) {
            Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
          } else {
            Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
          }
        }
      }
    }
  }

  private void validateIncludesPartitionScan(CloseableIterable<FileScanTask> tasks, int partValue) {
    Assert.assertTrue("File scan tasks do not include correct file",
        StreamSupport.stream(tasks.spliterator(), false).anyMatch(
            a -> a.file().partition().get(0, Object.class).equals(partValue)));
  }
}
