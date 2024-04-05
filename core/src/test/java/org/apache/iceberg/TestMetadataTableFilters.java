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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetadataTableFilters extends MetadataTableFiltersCommon {

  public TestMetadataTableFilters(MetadataTableType type, int formatVersion) {
    super(type, formatVersion);
  }

  @TestTemplate
  public void testNoFilter() {
    Table metadataTable = createMetadataTable();

    TableScan scan = metadataTable.newScan().select(partitionColumn("data_bucket"));
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    assertThat(tasks).hasSize(expectedScanTaskCount(4));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @TestTemplate
  public void testAnd() {
    Table metadataTable = createMetadataTable();

    Expression and =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(and);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    assertThat(tasks).hasSize(expectedScanTaskCount(1));
    validateFileScanTasks(tasks, 0);
  }

  @TestTemplate
  public void testLt() {
    Table metadataTable = createMetadataTable();

    Expression lt = Expressions.lessThan(partitionColumn("data_bucket"), 2);
    TableScan scan = metadataTable.newScan().filter(lt);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    assertThat(tasks).hasSize(expectedScanTaskCount(2));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
  }

  @TestTemplate
  public void testOr() {
    Table metadataTable = createMetadataTable();

    Expression or =
        Expressions.or(Expressions.equal(partitionColumn("data_bucket"), 2), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(or);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    assertThat(tasks).hasSize(expectedScanTaskCount(4));
    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @TestTemplate
  public void testNot() {
    Table metadataTable = createMetadataTable();

    Expression not = Expressions.not(Expressions.lessThan(partitionColumn("data_bucket"), 2));
    TableScan scan = metadataTable.newScan().filter(not);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    assertThat(tasks).hasSize(expectedScanTaskCount(2));
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @TestTemplate
  public void testIn() {
    Table metadataTable = createMetadataTable();

    Expression set = Expressions.in(partitionColumn("data_bucket"), 2, 3);
    TableScan scan = metadataTable.newScan().filter(set);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    assertThat(tasks).hasSize(expectedScanTaskCount(2));

    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @TestTemplate
  public void testNotNull() {
    Table metadataTable = createMetadataTable();
    Expression unary = Expressions.notNull(partitionColumn("data_bucket"));
    TableScan scan = metadataTable.newScan().filter(unary);

    CloseableIterable<FileScanTask> tasks = scan.planFiles();
    assertThat(tasks).hasSize(expectedScanTaskCount(4));

    validateFileScanTasks(tasks, 0);
    validateFileScanTasks(tasks, 1);
    validateFileScanTasks(tasks, 2);
    validateFileScanTasks(tasks, 3);
  }

  @TestTemplate
  public void testPlanTasks() {
    Table metadataTable = createMetadataTable();

    Expression and =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());

    TableScan scan = metadataTable.newScan().filter(and);
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    assertThat(tasks).hasSize(1);
    validateCombinedScanTasks(tasks, 0);
  }

  @TestTemplate
  public void testPartitionSpecEvolutionRemovalV1() {
    assumeThat(formatVersion).isEqualTo(1);

    // Change spec and add two data files
    table.updateSpec().removeField(Expressions.bucket("data", 16)).addField("id").commit();
    PartitionSpec newSpec = table.spec();

    // Add two data files with new spec
    PartitionKey data10Key = new PartitionKey(newSpec, table.schema());
    data10Key.set(1, 10);
    DataFile data10 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data10Key)
            .build();
    PartitionKey data11Key = new PartitionKey(newSpec, table.schema());
    data10Key.set(1, 11);
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data11Key)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    if (isAggFileTable(type)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous
      // files
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Moves file entries to DELETED state
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Removes all entries
      assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
    }

    Table metadataTable = createMetadataTable();
    Expression filter =
        Expressions.and(Expressions.equal(partitionColumn("id"), 10), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    // All 4 original data files written by old spec, plus one data file written by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(5));

    filter =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());
    scan = metadataTable.newScan().filter(filter);
    tasks = scan.planFiles();

    // 1 original data file written by old spec (V1 filters out new specs which don't have this
    // value)
    assertThat(tasks).hasSize(expectedScanTaskCount(1));
  }

  @TestTemplate
  public void testPartitionSpecEvolutionRemovalV2() {
    assumeThat(formatVersion).isEqualTo(2);

    // Change spec and add two data and delete files each
    table.updateSpec().removeField(Expressions.bucket("data", 16)).addField("id").commit();
    PartitionSpec newSpec = table.spec();

    // Add two data files and two delete files with new spec
    DataFile data10 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("id=10")
            .build();
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("id=11")
            .build();

    DeleteFile delete10 =
        FileMetadata.deleteFileBuilder(newSpec)
            .ofPositionDeletes()
            .withPath("/path/to/data-10-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("id=10")
            .withRecordCount(1)
            .build();
    DeleteFile delete11 =
        FileMetadata.deleteFileBuilder(newSpec)
            .ofPositionDeletes()
            .withPath("/path/to/data-11-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("id=11")
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    if (formatVersion == 2) {
      table.newRowDelta().addDeletes(delete10).commit();
      table.newRowDelta().addDeletes(delete11).commit();
    }

    if (isAggFileTable(type)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous
      // files
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Moves file entries to DELETED state
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Removes all entries
      assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
    }

    Table metadataTable = createMetadataTable();
    Expression filter =
        Expressions.and(Expressions.equal(partitionColumn("id"), 10), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    // All 4 original data/delete files written by old spec, plus one new data file/delete file
    // written by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(5));

    filter =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());
    scan = metadataTable.newScan().filter(filter);
    tasks = scan.planFiles();

    // 1 original data/delete files written by old spec, plus both of new data file/delete file
    // written by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(3));
  }

  @TestTemplate
  public void testPartitionSpecEvolutionAdditiveV1() {
    assumeThat(formatVersion).isEqualTo(1);

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

    if (isAggFileTable(type)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous
      // files
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Moves file entries to DELETED state
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Removes all entries
      assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
    }

    Table metadataTable = createMetadataTable();
    Expression filter =
        Expressions.and(Expressions.equal(partitionColumn("id"), 10), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    // All 4 original data/delete files written by old spec, plus one new data file written by new
    // spec
    assertThat(tasks).hasSize(expectedScanTaskCount(5));

    filter =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());
    scan = metadataTable.newScan().filter(filter);
    tasks = scan.planFiles();

    // 1 original data file written by old spec, plus 1 new data file written by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(2));
  }

  @TestTemplate
  public void testPartitionSpecEvolutionAdditiveV2() {
    assumeThat(formatVersion).isEqualTo(2);

    // Change spec and add two data and delete files each
    table.updateSpec().addField("id").commit();
    PartitionSpec newSpec = table.spec();

    // Add two data files and two delete files with new spec
    DataFile data10 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0/id=10")
            .build();
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1/id=11")
            .build();

    DeleteFile delete10 =
        FileMetadata.deleteFileBuilder(newSpec)
            .ofPositionDeletes()
            .withPath("/path/to/data-10-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0/id=10")
            .withRecordCount(1)
            .build();
    DeleteFile delete11 =
        FileMetadata.deleteFileBuilder(newSpec)
            .ofPositionDeletes()
            .withPath("/path/to/data-11-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1/id=11")
            .withRecordCount(1)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    if (formatVersion == 2) {
      table.newRowDelta().addDeletes(delete10).commit();
      table.newRowDelta().addDeletes(delete11).commit();
    }

    if (isAggFileTable(type)) {
      // Clear all files from current snapshot to test whether 'all' Files tables scans previous
      // files
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Moves file entries to DELETED state
      table
          .newDelete()
          .deleteFromRowFilter(Expressions.alwaysTrue())
          .commit(); // Removes all entries
      assertThat(table.currentSnapshot().allManifests(table.io())).isEmpty();
    }

    Table metadataTable = createMetadataTable();
    Expression filter =
        Expressions.and(Expressions.equal(partitionColumn("id"), 10), dummyExpression());
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<FileScanTask> tasks = scan.planFiles();

    // All 4 original data/delete files written by old spec, plus one new data file/delete file
    // written by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(5));

    filter =
        Expressions.and(Expressions.equal(partitionColumn("data_bucket"), 0), dummyExpression());
    scan = metadataTable.newScan().filter(filter);
    tasks = scan.planFiles();

    // 1 original data/delete files written by old spec, plus 1 of new data file/delete file written
    // by new spec
    assertThat(tasks).hasSize(expectedScanTaskCount(2));
  }

  private void validateCombinedScanTasks(CloseableIterable<CombinedScanTask> tasks, int partValue) {
    assertThat(tasks)
        .as("File scan tasks do not include correct partition value")
        .allSatisfy(
            task -> {
              assertThat(task.files())
                  .map(this::manifest)
                  .anyMatch(m -> manifestHasPartition(m, partValue));
            });
  }
}
