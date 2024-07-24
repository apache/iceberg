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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetadataTableScansWithPartitionEvolution extends MetadataTableScanTestBase {

  @BeforeEach
  public void createTable() throws IOException {
    TestTables.clearTables();
    this.tableDir = Files.createTempDirectory(temp, "junit").toFile();
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

  @TestTemplate
  public void testManifestsTableWithAddPartitionOnNestedField() throws IOException {
    Table manifestsTable = new ManifestsTable(table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
      assertThat(allRows(tasks)).hasSize(2);
    }
  }

  @TestTemplate
  public void testDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table dataFilesTable = new DataFilesTable(table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(2);
      assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @TestTemplate
  public void testManifestEntriesWithAddPartitionOnNestedField() throws IOException {
    Table manifestEntriesTable = new ManifestEntriesTable(table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(2);
      assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @TestTemplate
  public void testAllDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table allDataFilesTable = new AllDataFilesTable(table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(2);
      assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @TestTemplate
  public void testAllEntriesTableWithAddPartitionOnNestedField() throws IOException {
    Table allEntriesTable = new AllEntriesTable(table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(2);
      assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @TestTemplate
  public void testAllManifestsTableWithAddPartitionOnNestedField() throws IOException {
    Table allManifestsTable = new AllManifestsTable(table);
    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(2);
      assertThat(allRows(tasks)).hasSize(3);
    }
  }

  @TestTemplate
  public void testPartitionsTableScanWithAddPartitionOnNestedField() {
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
    assertThat(scanNoFilter.schema().asStruct()).isEqualTo(idPartition);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanNoFilter);
    assertThat(entries).hasSize(4);
    validatePartition(entries, 0, 0);
    validatePartition(entries, 0, 1);
    validatePartition(entries, 0, 2);
    validatePartition(entries, 0, 3);
    validatePartition(entries, 1, 2);
    validatePartition(entries, 1, 3);
  }

  @TestTemplate
  public void testPositionDeletesPartitionSpecRemoval() {
    assumeThat(formatVersion).as("Position deletes not supported by V1 Tables").isNotEqualTo(1);    table.updateSpec().removeField("id").commit();

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

    Types.StructType partitionType = positionDeletesTable.spec().partitionType();
    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    int filePartition = posDeleteTask.file().partition().get(0, Integer.class);
    assertThat(filePartition).as("Expected correct partition on task").isEqualTo(1);

    // Constant partition struct is common struct that includes even deleted partition column
    int taskConstantPartition =
        ((StructLike)
                constantsMap(posDeleteTask, partitionType).get(MetadataColumns.PARTITION_COLUMN_ID))
            .get(0, Integer.class);
    assertThat(taskConstantPartition)
        .as("Expected correct partition on constant column")
        .isEqualTo(1);
    assertThat(posDeleteTask.spec().fields().get(0).fieldId())
        .as("Expected correct partition field id on task's spec")
        .isEqualTo(partitionType.fields().get(0).fieldId());
    assertThat(posDeleteTask.file().specId())
        .as("Expected correct partition spec id on task")
        .isEqualTo(table.ops().current().spec().specId());
    assertThat((Map<Integer, Integer>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct partition spec id on constant column")
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), table.ops().current().spec().specId());
    assertThat(posDeleteTask.file().path())
        .as("Expected correct delete file on task")
        .isEqualTo(deleteFile.path());
    assertThat((Map<Integer, String>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct delete file on constant column")
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), deleteFile.path().toString());
  }

  @TestTemplate
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
    assertThat(partitionsTable.schema().findField("partition")).isNotNull();

    try (CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) partitionsTable.newScan())) {
      // four partitioned data files and one non-partitioned data file.
      assertThat(entries).hasSize(5);

      // check for null partition value.
      assertThat(entries)
          .anySatisfy(
              entry -> {
                StructLike partition = entry.file().partition();
                assertThat(partition.get(0, Object.class)).isNull();
              });
    }
  }

  private Stream<StructLike> allRows(Iterable<FileScanTask> tasks) {
    return Streams.stream(tasks).flatMap(task -> Streams.stream(task.asDataTask().rows()));
  }
}
