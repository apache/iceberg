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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestMetadataTableScansWithPartitionEvolution extends MetadataTableScanTestBase {
  public TestMetadataTableScansWithPartitionEvolution(int formatVersion) {
    super(formatVersion);
  }

  @Before
  public void createTable() throws IOException {
    TestTables.clearTables();
    this.tableDir = temp.newFolder();
    tableDir.delete();

    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(
                2,
                "nested",
                Types.StructType.of(Types.NestedField.required(3, "id", Types.IntegerType.get()))));
    this.metadataDir = new File(tableDir, "metadata");
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("id").build();

    this.table = create(schema, spec);
    table.newFastAppend().appendFile(newDataFile("id=0")).appendFile(newDataFile("id=1")).commit();

    table.updateSpec().addField("nested.id").commit();
    table
        .newFastAppend()
        .appendFile(newDataFile("id=2/nested.id=2"))
        .appendFile(newDataFile("id=3/nested.id=3"))
        .commit();
  }

  @Test
  public void testManifestsTableWithAddPartitionOnNestedField() throws IOException {
    Table manifestsTable = new ManifestsTable(table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(1);
      Assertions.assertThat(allRows(tasks)).hasSize(2);
    }
  }

  @Test
  public void testDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table dataFilesTable = new DataFilesTable(table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testManifestEntriesWithAddPartitionOnNestedField() throws IOException {
    Table manifestEntriesTable = new ManifestEntriesTable(table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table allDataFilesTable = new AllDataFilesTable(table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllEntriesTableWithAddPartitionOnNestedField() throws IOException {
    Table allEntriesTable = new AllEntriesTable(table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllManifestsTableWithAddPartitionOnNestedField() throws IOException {
    Table allManifestsTable = new AllManifestsTable(table);
    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(3);
    }
  }

  @Test
  public void testPartitionsTableScanWithAddPartitionOnNestedField() throws IOException {
    Table partitionsTable = new PartitionsTable(table);
    Types.StructType idPartition =
        new Schema(
                required(
                    1,
                    "partition",
                    Types.StructType.of(
                        optional(1000, "id", Types.IntegerType.get()),
                        optional(1001, "nested.id", Types.IntegerType.get()))))
            .asStruct();

    TableScan scanNoFilter = partitionsTable.newScan().select("partition");
    Assert.assertEquals(idPartition, scanNoFilter.schema().asStruct());
    CloseableIterable<ContentFile<?>> files =
        PartitionsTable.planFiles((StaticTableScan) scanNoFilter);
    Assert.assertEquals(4, Iterators.size(files.iterator()));
    validatePartition(files, 0, 0);
    validatePartition(files, 0, 1);
    validatePartition(files, 0, 2);
    validatePartition(files, 0, 3);
    validatePartition(files, 1, 2);
    validatePartition(files, 1, 3);
  }

  @Test
  public void testPositionDeletesPartitionSpecRemoval() {
    Assume.assumeTrue("Position deletes supported only for v2 tables", formatVersion == 2);

    table.updateSpec().removeField("id").commit();

    DeleteFile deleteFile = newDeleteFile(table.ops().current().spec().specId(), "nested.id=1");
    table.newRowDelta().addDeletes(deleteFile).commit();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);

    Expression expression =
        Expressions.and(
            Expressions.equal("partition.nested.id", 1), Expressions.greaterThan("pos", 0));
    BatchScan scan = positionDeletesTable.newBatchScan().filter(expression);

    assertThat(scan).isInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);

    List<ScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).hasSize(1);

    ScanTask task = tasks.get(0);
    assertThat(task).isInstanceOf(PositionDeletesScanTask.class);

    Types.StructType partitionType = Partitioning.partitionType(table);
    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    int filePartition = posDeleteTask.file().partition().get(0, Integer.class);
    Assert.assertEquals("Expected correct partition on task", 1, filePartition);

    // Constant partition struct is common struct that includes even deleted partition column
    int taskConstantPartition =
        ((StructLike)
                constantsMap(posDeleteTask, partitionType).get(MetadataColumns.PARTITION_COLUMN_ID))
            .get(1, Integer.class);
    Assert.assertEquals("Expected correct partition on constant column", 1, taskConstantPartition);

    Assert.assertEquals(
        "Expected correct partition field id on task's spec",
        table.ops().current().spec().partitionType().fields().get(0).fieldId(),
        posDeleteTask.spec().fields().get(0).fieldId());

    Assert.assertEquals(
        "Expected correct partition spec id on task",
        table.ops().current().spec().specId(),
        posDeleteTask.file().specId());
    Assert.assertEquals(
        "Expected correct partition spec id on constant column",
        table.ops().current().spec().specId(),
        constantsMap(posDeleteTask, partitionType).get(MetadataColumns.SPEC_ID.fieldId()));

    Assert.assertEquals(
        "Expected correct delete file on task", deleteFile.path(), posDeleteTask.file().path());
    Assert.assertEquals(
        "Expected correct delete file on constant column",
        deleteFile.path(),
        constantsMap(posDeleteTask, partitionType).get(MetadataColumns.FILE_PATH.fieldId()));
  }

  @Test
  public void testPartitionSpecEvolutionToUnpartitioned() throws IOException {
    // Remove all the partition fields
    table.updateSpec().removeField("id").removeField("nested.id").commit();

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-10.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .build();
    table.newFastAppend().appendFile(dataFile).commit();

    PartitionsTable partitionsTable = new PartitionsTable(table);
    // must contain the partition column even when the current spec is non-partitioned.
    Assertions.assertThat(partitionsTable.schema().findField("partition")).isNotNull();

    try (CloseableIterable<ContentFile<?>> files =
        PartitionsTable.planFiles((StaticTableScan) partitionsTable.newScan())) {
      // four partitioned data files and one non-partitioned data file.
      Assertions.assertThat(files).hasSize(5);

      // check for null partition value.
      Assertions.assertThat(StreamSupport.stream(files.spliterator(), false))
          .anyMatch(
              file -> {
                StructLike partition = file.partition();
                return Objects.equals(null, partition.get(0, Object.class));
              });
    }
  }

  private Stream<StructLike> allRows(Iterable<FileScanTask> tasks) {
    return Streams.stream(tasks).flatMap(task -> Streams.stream(task.asDataTask().rows()));
  }
}
