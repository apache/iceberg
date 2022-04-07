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

import java.util.stream.StreamSupport;
import org.apache.iceberg.BaseFilesTable.ManifestReadTask;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMetadataTableFilters extends TableTestBase {

  private final MetadataTableType type;

  @Parameterized.Parameters(name = "table_type = {0}, format = {1}")
  public static Object[][] parameters() {
    return new Object[][] {
        { MetadataTableType.DATA_FILES, 1 },
        { MetadataTableType.DATA_FILES, 2 },
        { MetadataTableType.DELETE_FILES, 2 },
        { MetadataTableType.FILES, 1 },
        { MetadataTableType.FILES, 2 },
        { MetadataTableType.ALL_DATA_FILES, 1 },
        { MetadataTableType.ALL_DATA_FILES, 2 }
    };
  }

  public TestMetadataTableFilters(MetadataTableType type, int formatVersion) {
    super(formatVersion);
    this.type = type;
  }

  @Before
  @Override
  public void setupTable() throws Exception {
    super.setupTable();
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_D)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    if (formatVersion == 2) {
      table.newRowDelta()
          .addDeletes(FILE_A_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_B_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_C2_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_D2_DELETES)
          .commit();
    }

    if (type.equals(MetadataTableType.ALL_DATA_FILES)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous files
      table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();  // Moves file entries to DELETED state
      table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();  // Removes all entries
      Assert.assertEquals("Current snapshot should be made empty",
          0, table.currentSnapshot().allManifests().size());
    }
  }

  private Table createMetadataTable() {
    switch (type) {
      case FILES:
        return new FilesTable(table.ops(), table);
      case DATA_FILES:
        return new DataFilesTable(table.ops(), table);
      case DELETE_FILES:
        return new DeleteFilesTable(table.ops(), table);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(table.ops(), table);
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }

  private int expectedScanTaskCount(int partitions) {
    switch (type) {
      case FILES:
        if (formatVersion == 1) {
          return partitions;
        } else {
          return partitions * 2; // Delete File and Data File per partition
        }
      case DATA_FILES:
      case DELETE_FILES:
        return partitions;
      case ALL_DATA_FILES:
        return partitions * 2; // ScanTask for Data Manifest in DELETED and ADDED states
      default:
        throw new IllegalArgumentException("Unsupported metadata table type:" + type);
    }
  }

  @Test
  public void testNoFilter() {
    Table metadataTable = createMetadataTable();
    Types.StructType expected = new Schema(
        required(102, "partition", Types.StructType.of(
            optional(1000, "data_bucket", Types.IntegerType.get())),
            "Partition data tuple, schema based on the partition spec")).asStruct();

    TableScan scan = metadataTable.newScan().select("partition.data_bucket");
    Assert.assertEquals(expected, scan.schema().asStruct());
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    Assert.assertEquals(expectedScanTaskCount(4), Iterables.size(tasks));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @Test
  public void testAnd() {
    Table metadataTable = createMetadataTable();

    Expression and = Expressions.and(
        Expressions.equal("partition.data_bucket", 0),
        Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(and);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    Assert.assertEquals(expectedScanTaskCount(1), Iterables.size(tasks));
    validateFileScanTasks(tasks, 0);
  }

  @Test
  public void testLt() {
    Table metadataTable = createMetadataTable();

    Expression lt = Expressions.lessThan("partition.data_bucket", 2);
    TableScan scan = metadataTable.newScan().filter(lt);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Assert.assertEquals(expectedScanTaskCount(2), Iterables.size(tasks));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
  }

  @Test
  public void testOr() {
    Table metadataTable = createMetadataTable();

    Expression or = Expressions.or(
        Expressions.equal("partition.data_bucket", 2),
        Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(or);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    Assert.assertEquals(expectedScanTaskCount(4), Iterables.size(tasks));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @Test
  public void testNot() {
    Table metadataTable = createMetadataTable();

    Expression not = Expressions.not(Expressions.lessThan("partition.data_bucket", 2));
    TableScan scan = metadataTable.newScan().filter(not);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Assert.assertEquals(expectedScanTaskCount(2), Iterables.size(tasks));
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @Test
  public void testIn() {
    Table metadataTable = createMetadataTable();

    Expression set = Expressions.in("partition.data_bucket", 2, 3);
    TableScan scan = metadataTable.newScan().filter(set);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Assert.assertEquals(expectedScanTaskCount(2), Iterables.size(tasks));

    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @Test
  public void testNotNull() {
    Table metadataTable = createMetadataTable();
    Expression unary = Expressions.notNull("partition.data_bucket");
    TableScan scan = metadataTable.newScan().filter(unary);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    Assert.assertEquals(expectedScanTaskCount(4), Iterables.size(tasks));

    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @Test
  public void testPlanTasks() {
    Table metadataTable = createMetadataTable();

    Expression and = Expressions.and(
        Expressions.equal("partition.data_bucket", 0),
        Expressions.greaterThan("record_count", 0));

    TableScan scan = metadataTable.newScan().filter(and);
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    Assert.assertEquals(1, Iterables.size(tasks));
    validateCombinedScanTasks(tasks, 0);
  }

  private void validateFileScanTasks(CloseableIterable<FileScanTask> fileScanTasks, int partValue) {
    Assert.assertTrue("File scan tasks do not include correct file",
        StreamSupport.stream(fileScanTasks.spliterator(), false).anyMatch(t -> {
          ManifestFile mf = ((ManifestReadTask) t).manifest();
          return manifestHasPartition(mf, partValue);
        }));
  }

  private void validateCombinedScanTasks(CloseableIterable<CombinedScanTask> tasks, int partValue) {
    StreamSupport.stream(tasks.spliterator(), false)
        .flatMap(c -> c.files().stream().map(t -> ((ManifestReadTask) t).manifest()))
        .anyMatch(m -> manifestHasPartition(m, partValue));
  }

  private boolean manifestHasPartition(ManifestFile mf, int partValue) {
    int lower = Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).lowerBound());
    int upper = Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).upperBound());
    return (lower <= partValue) && (upper >= partValue);
  }
}
