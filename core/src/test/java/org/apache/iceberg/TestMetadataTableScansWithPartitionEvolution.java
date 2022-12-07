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
import java.util.stream.Stream;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
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
    Table manifestsTable = new ManifestsTable(table.ops(), table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(1);
      Assertions.assertThat(allRows(tasks)).hasSize(2);
    }
  }

  @Test
  public void testDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table dataFilesTable = new DataFilesTable(table.ops(), table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testManifestEntriesWithAddPartitionOnNestedField() throws IOException {
    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllDataFilesTableWithAddPartitionOnNestedField() throws IOException {
    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllEntriesTableWithAddPartitionOnNestedField() throws IOException {
    Table allEntriesTable = new AllEntriesTable(table.ops(), table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(4);
    }
  }

  @Test
  public void testAllManifestsTableWithAddPartitionOnNestedField() throws IOException {
    Table allManifestsTable = new AllManifestsTable(table.ops(), table);
    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assertions.assertThat(tasks).hasSize(2);
      Assertions.assertThat(allRows(tasks)).hasSize(3);
    }
  }

  @Test
  public void testPartitionsTableScanWithAddPartitionOnNestedField() throws IOException {
    Table partitionsTable = new PartitionsTable(table.ops(), table);
    Types.StructType idPartition =
        new Schema(
                required(
                    1,
                    "partition",
                    Types.StructType.of(
                        optional(10000, "id", Types.IntegerType.get()),
                        optional(10001, "nested.id", Types.IntegerType.get()))))
            .asStruct();

    TableScan scanNoFilter = partitionsTable.newScan().select("partition");
    Assert.assertEquals(idPartition, scanNoFilter.schema().asStruct());
    try (CloseableIterable<FileScanTask> tasksNoFilter =
        PartitionsTable.planFiles((StaticTableScan) scanNoFilter)) {
      Assertions.assertThat(tasksNoFilter).hasSize(4);
      validateIncludesPartitionScan(tasksNoFilter, 0);
      validateIncludesPartitionScan(tasksNoFilter, 1);
      validateIncludesPartitionScan(tasksNoFilter, 2);
      validateIncludesPartitionScan(tasksNoFilter, 3);
      validateIncludesPartitionScan(tasksNoFilter, 1, 2);
      validateIncludesPartitionScan(tasksNoFilter, 1, 3);
    }
  }

  private Stream<StructLike> allRows(Iterable<FileScanTask> tasks) {
    return Streams.stream(tasks).flatMap(task -> Streams.stream(task.asDataTask().rows()));
  }
}
