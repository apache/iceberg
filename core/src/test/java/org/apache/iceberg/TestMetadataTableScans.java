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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.puffin.StandardBlobTypes;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetadataTableScans extends MetadataTableScanTestBase {

  private void preparePartitionedTable(boolean transactional) {
    preparePartitionedTableData(transactional);

    if (formatVersion >= 2) {
      if (transactional) {
        table
            .newRowDelta()
            .addDeletes(fileADeletes())
            .addDeletes(fileBDeletes())
            .addDeletes(FILE_C2_DELETES)
            .addDeletes(FILE_D2_DELETES)
            .commit();
      } else {
        table.newRowDelta().addDeletes(fileADeletes()).commit();
        table.newRowDelta().addDeletes(fileBDeletes()).commit();
        table.newRowDelta().addDeletes(FILE_C2_DELETES).commit();
        table.newRowDelta().addDeletes(FILE_D2_DELETES).commit();
      }
    }
  }

  private void preparePartitionedTable() {
    preparePartitionedTable(false);
  }

  private void preparePartitionedTableData(boolean transactional) {
    if (transactional) {
      table
          .newFastAppend()
          .appendFile(FILE_A)
          .appendFile(FILE_B)
          .appendFile(FILE_C)
          .appendFile(FILE_D)
          .commit();
    } else {
      table.newFastAppend().appendFile(FILE_A).commit();
      table.newFastAppend().appendFile(FILE_C).commit();
      table.newFastAppend().appendFile(FILE_D).commit();
      table.newFastAppend().appendFile(FILE_B).commit();
    }
  }

  private void preparePartitionedTableData() {
    preparePartitionedTableData(false);
  }

  @TestTemplate
  public void testManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestsTable = new ManifestsTable(table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testManifestsTableAlwaysIgnoresResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table manifestsTable = new ManifestsTable(table);

    TableScan scan = manifestsTable.newScan().filter(Expressions.lessThan("length", 10000L));

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).as("Tasks should not be empty").hasSizeGreaterThan(0);
      for (FileScanTask task : tasks) {
        assertThat(task.residual())
            .as("Residuals must be ignored")
            .isEqualTo(Expressions.alwaysTrue());
      }
    }
  }

  @TestTemplate
  public void testMetadataTableUUID() {
    Table manifestsTable = new ManifestsTable(table);

    assertThat(manifestsTable.uuid())
        .as("UUID should be consistent on multiple calls")
        .isEqualTo(manifestsTable.uuid());
    assertThat(manifestsTable.uuid())
        .as("Metadata table UUID should be different from the base table UUID")
        .isNotEqualTo(table.uuid());
  }

  @TestTemplate
  public void testDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table dataFilesTable = new DataFilesTable(table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table dataFilesTable = new DataFilesTable(table);

    TableScan scan1 = dataFilesTable.newScan().filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        dataFilesTable.newScan().filter(Expressions.equal("record_count", 1)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @TestTemplate
  public void testManifestEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table);

    TableScan scan1 = manifestEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        manifestEntriesTable
            .newScan()
            .filter(Expressions.equal("snapshot_id", 1L))
            .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @TestTemplate
  public void testManifestEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testEntriesTableDataFileContentEq() {
    preparePartitionedTable();

    Table entriesTable = new ManifestEntriesTable(table);

    Expression dataOnly = Expressions.equal("data_file.content", 0);
    TableScan entriesTableScan = entriesTable.newScan().filter(dataOnly);
    Set<String> expected =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());

    assertThat(scannedPaths(entriesTableScan))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expected);

    assertThat(
            scannedPaths(entriesTable.newScan().filter(Expressions.equal("data_file.content", 3))))
        .as("Expected manifest filter by data file content does not match")
        .isEmpty();
  }

  @TestTemplate
  public void testEntriesTableDateFileContentNotEq() {
    preparePartitionedTable();

    Table entriesTable = new ManifestEntriesTable(table);

    Expression notData = Expressions.notEqual("data_file.content", 0);
    TableScan entriesTableScan = entriesTable.newScan().filter(notData);
    Set<String> expected =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());

    assertThat(scannedPaths(entriesTableScan))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expected);

    Set<String> allManifests =
        table.currentSnapshot().allManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    assertThat(
            scannedPaths(
                entriesTable.newScan().filter(Expressions.notEqual("data_file.content", 3))))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(allManifests);
  }

  @TestTemplate
  public void testEntriesTableDataFileContentIn() {
    preparePartitionedTable();
    Table entriesTable = new ManifestEntriesTable(table);

    Expression in0 = Expressions.in("data_file.content", 0);
    TableScan scan1 = entriesTable.newScan().filter(in0);
    Set<String> expectedDataManifestPath =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    assertThat(scannedPaths(scan1))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expectedDataManifestPath);

    Expression in12 = Expressions.in("data_file.content", 1, 2);
    TableScan scan2 = entriesTable.newScan().filter(in12);
    Set<String> expectedDeleteManifestPath =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    assertThat(scannedPaths(scan2))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expectedDeleteManifestPath);

    Expression inAll = Expressions.in("data_file.content", 0, 1, 2);
    Set<String> allManifests = Sets.union(expectedDataManifestPath, expectedDeleteManifestPath);
    assertThat(scannedPaths(entriesTable.newScan().filter(inAll)))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(allManifests);

    Expression inNeither = Expressions.in("data_file.content", 3, 4);
    assertThat(scannedPaths(entriesTable.newScan().filter(inNeither)))
        .as("Expected manifest filter by data file content does not match")
        .isEmpty();
  }

  @TestTemplate
  public void testEntriesTableDataFileContentNotIn() {
    preparePartitionedTable();
    Table entriesTable = new ManifestEntriesTable(table);

    Expression notIn0 = Expressions.notIn("data_file.content", 0);
    TableScan scan1 = entriesTable.newScan().filter(notIn0);
    Set<String> expectedDeleteManifestPath =
        table.currentSnapshot().deleteManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    assertThat(scannedPaths(scan1))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expectedDeleteManifestPath);

    Expression notIn12 = Expressions.notIn("data_file.content", 1, 2);
    TableScan scan2 = entriesTable.newScan().filter(notIn12);
    Set<String> expectedDataManifestPath =
        table.currentSnapshot().dataManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    assertThat(scannedPaths(scan2))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(expectedDataManifestPath);

    Expression notInNeither = Expressions.notIn("data_file.content", 3);
    Set<String> allManifests = Sets.union(expectedDataManifestPath, expectedDeleteManifestPath);
    assertThat(scannedPaths(entriesTable.newScan().filter(notInNeither)))
        .as("Expected manifest filter by data file content does not match")
        .isEqualTo(allManifests);

    Expression notInAll = Expressions.notIn("data_file.content", 0, 1, 2);
    assertThat(scannedPaths(entriesTable.newScan().filter(notInAll)))
        .as("Expected manifest filter by data file content does not match")
        .isEmpty();
  }

  @TestTemplate
  public void testAllDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allDataFilesTable = new AllDataFilesTable(table);

    TableScan scan1 = allDataFilesTable.newScan().filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allDataFilesTable.newScan().filter(Expressions.equal("record_count", 1)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @TestTemplate
  public void testAllDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allDataFilesTable = new AllDataFilesTable(table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testAllEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allEntriesTable = new AllEntriesTable(table);

    TableScan scan1 = allEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allEntriesTable.newScan().filter(Expressions.equal("snapshot_id", 1L)).ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @TestTemplate
  public void testAllEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allEntriesTable = new AllEntriesTable(table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testAllManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allManifestsTable = new AllManifestsTable(table);

    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      assertThat(tasks).hasSize(1);
    }
  }

  @TestTemplate
  public void testAllManifestsTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allManifestsTable = new AllManifestsTable(table);

    TableScan scan1 = allManifestsTable.newScan().filter(Expressions.lessThan("length", 10000L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 =
        allManifestsTable
            .newScan()
            .filter(Expressions.lessThan("length", 10000L))
            .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @TestTemplate
  public void testPartitionsTableScanNoFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);
    Types.StructType expected =
        new Schema(
                required(
                    1,
                    "partition",
                    Types.StructType.of(optional(1000, "data_bucket", Types.IntegerType.get()))))
            .asStruct();
    TableScan scanNoFilter = partitionsTable.newScan().select("partition.data_bucket");
    assertThat(scanNoFilter.schema().asStruct()).isEqualTo(expected);

    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanNoFilter);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(8);
    } else {
      assertThat(entries).hasSize(4);
    }

    validateSingleFieldPartition(entries, 0);
    validateSingleFieldPartition(entries, 1);
    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
  public void testPartitionsTableScanWithProjection() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);
    Types.StructType expected =
        new Schema(required(3, "file_count", Types.IntegerType.get(), "Count of data files"))
            .asStruct();

    TableScan scanWithProjection = partitionsTable.newScan().select("file_count");
    assertThat(scanWithProjection.schema().asStruct()).isEqualTo(expected);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanWithProjection);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(8);
    } else {
      assertThat(entries).hasSize(4);
    }

    validateSingleFieldPartition(entries, 0);
    validateSingleFieldPartition(entries, 1);
    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
  public void testPartitionsTableReusesPartitionDataSchema() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);
    StaticTableScan scan = (StaticTableScan) partitionsTable.newScan();
    List<org.apache.avro.Schema> partitionSchemas = Lists.newArrayList();

    for (PartitionsTable.Partition partition : PartitionsTable.partitions(table, scan)) {
      partitionSchemas.add(partition.partitionData().getSchema());
    }

    assertThat(partitionSchemas).hasSizeGreaterThanOrEqualTo(2);
    org.apache.avro.Schema expectedSchema = partitionSchemas.get(0);
    assertThat(partitionSchemas).allSatisfy(schema -> assertThat(schema).isSameAs(expectedSchema));
  }

  @TestTemplate
  public void testPartitionsTableScanNoStats() {
    table.newFastAppend().appendFile(FILE_WITH_STATS).commit();

    Table partitionsTable = new PartitionsTable(table);
    CloseableIterable<ManifestEntry<?>> tasksAndEq =
        PartitionsTable.planEntries((StaticTableScan) partitionsTable.newScan());
    for (ManifestEntry<? extends ContentFile<?>> task : tasksAndEq) {
      assertThat(task.file().columnSizes()).isNull();
      assertThat(task.file().valueCounts()).isNull();
      assertThat(task.file().nullValueCounts()).isNull();
      assertThat(task.file().nanValueCounts()).isNull();
      assertThat(task.file().lowerBounds()).isNull();
      assertThat(task.file().upperBounds()).isNull();
    }
  }

  @TestTemplate
  public void testPartitionsTableScanAndFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression andEquals =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanAndEq);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(2);
    } else {
      assertThat(entries).hasSize(1);
    }

    validateSingleFieldPartition(entries, 0);
  }

  @TestTemplate
  public void testPartitionsTableScanLtFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression ltAnd =
        Expressions.and(
            Expressions.lessThan("partition.data_bucket", 2),
            Expressions.greaterThan("record_count", 0));
    TableScan scanLtAnd = partitionsTable.newScan().filter(ltAnd);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanLtAnd);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(4);
    } else {
      assertThat(entries).hasSize(2);
    }

    validateSingleFieldPartition(entries, 0);
    validateSingleFieldPartition(entries, 1);
  }

  @TestTemplate
  public void testPartitionsTableScanOrFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression or =
        Expressions.or(
            Expressions.equal("partition.data_bucket", 2),
            Expressions.greaterThan("record_count", 0));
    TableScan scanOr = partitionsTable.newScan().filter(or);

    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanOr);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(8);
    } else {
      assertThat(entries).hasSize(4);
    }

    validateSingleFieldPartition(entries, 0);
    validateSingleFieldPartition(entries, 1);
    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
  public void testPartitionsScanNotFilter() {
    preparePartitionedTable();
    Table partitionsTable = new PartitionsTable(table);

    Expression not = Expressions.not(Expressions.lessThan("partition.data_bucket", 2));
    TableScan scanNot = partitionsTable.newScan().filter(not);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanNot);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(4);
    } else {
      assertThat(entries).hasSize(2);
    }

    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
  public void testPartitionsTableScanInFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression set = Expressions.in("partition.data_bucket", 2, 3);
    TableScan scanSet = partitionsTable.newScan().filter(set);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanSet);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(4);
    } else {
      assertThat(entries).hasSize(2);
    }

    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
  public void testPartitionsTableScanNotNullFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);

    Expression unary = Expressions.notNull("partition.data_bucket");
    TableScan scanUnary = partitionsTable.newScan().filter(unary);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanUnary);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(8);
    } else {
      assertThat(entries).hasSize(4);
    }

    validateSingleFieldPartition(entries, 0);
    validateSingleFieldPartition(entries, 1);
    validateSingleFieldPartition(entries, 2);
    validateSingleFieldPartition(entries, 3);
  }

  @TestTemplate
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

    Table dataFilesTable = new DataFilesTable(table);
    TableScan scan = dataFilesTable.newScan();

    Schema schema = dataFilesTable.schema();
    Types.StructType actualType = schema.findField(DataFile.PARTITION_ID).type().asStructType();
    Types.StructType expectedType =
        Types.StructType.of(
            Types.NestedField.optional(1000, "data_bucket", Types.IntegerType.get()),
            Types.NestedField.optional(1001, "data_bucket_16", Types.IntegerType.get()),
            Types.NestedField.optional(1002, "data_trunc_2", Types.StringType.get()));
    assertThat(actualType).as("Partition type must match").isEqualTo(expectedType);
    Accessor<StructLike> accessor = schema.accessorForField(1000);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Set<Integer> results =
          StreamSupport.stream(tasks.spliterator(), false)
              .flatMap(fileScanTask -> Streams.stream(fileScanTask.asDataTask().rows()))
              .map(accessor::get)
              .map(i -> (Integer) i)
              .collect(Collectors.toSet());
      assertThat(results).as("Partition value must match").containsExactly(0, 1, 2, 3);
    }
  }

  @TestTemplate
  public void testDeleteFilesTableSelection() throws IOException {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newRowDelta().addDeletes(fileADeletes()).addDeletes(FILE_A2_DELETES).commit();

    Table deleteFilesTable = new DeleteFilesTable(table);

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
    assertThat(scan.schema().asStruct()).isEqualTo(expected);
  }

  @TestTemplate
  public void testFilesTableReadableMetricsSchema() {
    Table filesTable = new FilesTable(table);
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

    assertThat(actual).as("Dynamic schema for readable_metrics should match").isEqualTo(expected);
  }

  @TestTemplate
  public void testEntriesTableReadableMetricsSchema() {
    Table entriesTable = new ManifestEntriesTable(table);
    Types.StructType actual = entriesTable.newScan().schema().select("readable_metrics").asStruct();
    int highestId = entriesTable.schema().highestFieldId();

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

    assertThat(actual).as("Dynamic schema for readable_metrics should match").isEqualTo(expected);
  }

  @TestTemplate
  public void testPuffinFilesTableSchema() {
    Table puffinFilesTable = new PuffinFilesTable(table);
    Types.StructType actual = puffinFilesTable.newScan().schema().asStruct();
    Types.StructType expected =
        new Schema(
                required(1, "snapshot_id", Types.LongType.get(), "ID of the selected snapshot"),
                required(
                    2,
                    "file_path",
                    Types.StringType.get(),
                    "Fully qualified location of the Puffin file"),
                required(
                    3,
                    "source",
                    Types.StringType.get(),
                    "Metadata source that associates the Puffin file with the selected snapshot"),
                required(
                    4,
                    "file_size_in_bytes",
                    Types.LongType.get(),
                    "Total size of the Puffin file in bytes"),
                required(
                    5,
                    "referenced_blob_count",
                    Types.IntegerType.get(),
                    "Number of distinct blobs in the Puffin file referenced by the selected snapshot"),
                required(
                    6,
                    "referenced_blob_types",
                    Types.ListType.ofRequired(7, Types.StringType.get()),
                    "Distinct types of blobs referenced by the selected snapshot"),
                required(
                    8,
                    "referenced_fields",
                    Types.ListType.ofRequired(
                        9,
                        Types.StructType.of(
                            required(
                                10,
                                "field_id",
                                Types.IntegerType.get(),
                                "Field ID referenced by at least one blob"),
                            optional(
                                11,
                                "current_field_name",
                                Types.StringType.get(),
                                "Name currently assigned to the field ID; null if no longer present"))),
                    "Distinct fields referenced across the selected blobs"))
            .asStruct();

    assertThat(actual).isEqualTo(expected);
  }

  @TestTemplate
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
    data11Key.set(0, 1); // data=1
    data11Key.set(1, 11); // id=11
    DataFile data11 =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-11.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartition(data11Key)
            .build();

    table.newFastAppend().appendFile(data10).commit();
    table.newFastAppend().appendFile(data11).commit();

    Table metadataTable = new PartitionsTable(table);
    Expression filter =
        Expressions.and(
            Expressions.equal("partition.id", 10), Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scan);
    if (formatVersion >= 2) {
      // Four data files and delete files of old spec, one new data file of new spec
      assertThat(entries).hasSize(9);
    } else {
      // Four data files of old spec, one new data file of new spec
      assertThat(entries).hasSize(5);
    }

    filter =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    scan = metadataTable.newScan().filter(filter);
    entries = PartitionsTable.planEntries((StaticTableScan) scan);

    if (formatVersion >= 2) {
      // 1 original data file and delete file written by old spec, plus 1 new data file written by
      // new spec
      assertThat(entries).hasSize(3);
    } else {
      // 1 original data file written by old spec, plus 1 new data file written by new spec
      assertThat(entries).hasSize(2);
    }
  }

  @TestTemplate
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

    Table metadataTable = new PartitionsTable(table);
    Expression filter =
        Expressions.and(
            Expressions.equal("partition.id", 10), Expressions.greaterThan("record_count", 0));
    TableScan scan = metadataTable.newScan().filter(filter);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scan);

    if (formatVersion >= 2) {
      // Four data and delete files of original spec, one data file written by new spec
      assertThat(entries).hasSize(9);
    } else {
      // Four data files of original spec, one data file written by new spec
      assertThat(entries).hasSize(5);
    }

    // Filter for a dropped partition spec field.  Correct behavior is that only old partitions are
    // returned.
    filter =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 0),
            Expressions.greaterThan("record_count", 0));
    scan = metadataTable.newScan().filter(filter);
    entries = PartitionsTable.planEntries((StaticTableScan) scan);

    if (formatVersion == 1) {
      // 1 original data file written by old spec
      assertThat(entries).hasSize(1);
    } else {
      // 1 original data and 1 delete files written by old spec, plus both of new data file/delete
      // file written by new spec
      //
      // Unlike in V1, V2 does not write (data=null) on newer files' partition data, so these cannot
      // be filtered out
      // early in scan planning here.
      //
      // However, these partition rows are filtered out later in Spark data filtering, as the newer
      // partitions
      // will have 'data=null' field added as part of normalization to the Partition table final
      // schema.
      // The Partition table final schema is a union of fields of all specs, including dropped
      // fields.
      assertThat(entries).hasSize(4);
    }
  }

  @TestTemplate
  public void testPartitionColumnNamedPartition() throws Exception {
    TestTables.clearTables();

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

    Table partitionsTable = new PartitionsTable(table);

    Expression andEquals =
        Expressions.and(
            Expressions.equal("partition.partition", 0),
            Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scanAndEq);
    assertThat(entries).hasSize(1);
    validateSingleFieldPartition(entries, 0);
  }

  @TestTemplate
  public void testAllDataFilesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allDataFilesTable = new AllDataFilesTable(table);
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
    assertThat(scan.planFiles()).hasSize(1);
    assertThat(planThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
  public void testAllEntriesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table allEntriesTable = new AllEntriesTable(table);
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
    assertThat(scan.planFiles()).hasSize(1);
    assertThat(planThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
  public void testPartitionsTableScanWithPlanExecutor() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table);
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
    CloseableIterable<ManifestEntry<?>> entries =
        PartitionsTable.planEntries((StaticTableScan) scan);
    if (formatVersion >= 2) {
      assertThat(entries).hasSize(8);
    } else {
      assertThat(entries).hasSize(4);
    }

    assertThat(planThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotGt() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.greaterThan("reference_snapshot_id", 2));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 3L, 4L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotGte() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.greaterThanOrEqual("reference_snapshot_id", 3));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 3L, 4L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotLt() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.lessThan("reference_snapshot_id", 3));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 1L, 2L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotLte() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.lessThanOrEqual("reference_snapshot_id", 2));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 1L, 2L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotEq() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.equal("reference_snapshot_id", 2));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 2L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotNotEq() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.notEqual("reference_snapshot_id", 2));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 1L, 3L, 4L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotIn() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);

    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.in("reference_snapshot_id", 1, 3));
    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 1L, 3L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotNotIn() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable.newScan().filter(Expressions.notIn("reference_snapshot_id", 1, 3));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 2L, 4L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotAnd() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);

    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(
                Expressions.and(
                    Expressions.equal("reference_snapshot_id", 2),
                    Expressions.greaterThan("length", 0)));
    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 2L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotOr() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);

    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(
                Expressions.or(
                    Expressions.equal("reference_snapshot_id", 2),
                    Expressions.equal("reference_snapshot_id", 4)));
    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 2L, 4L));
  }

  @TestTemplate
  public void testAllManifestsTableSnapshotNot() {
    // Snapshots 1,2,3,4
    preparePartitionedTableData();

    Table manifestsTable = new AllManifestsTable(table);
    TableScan manifestsTableScan =
        manifestsTable
            .newScan()
            .filter(Expressions.not(Expressions.equal("reference_snapshot_id", 2)));

    assertThat(scannedPaths(manifestsTableScan))
        .as("Expected snapshots do not match")
        .isEqualTo(expectedManifestListPaths(table.snapshots(), 1L, 3L, 4L));
  }

  @TestTemplate
  public void testPositionDeletesWithFilter() {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    preparePartitionedTable();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);

    Expression expression =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 1), Expressions.greaterThan("pos", 0));
    BatchScan scan = positionDeletesTable.newBatchScan().filter(expression);
    assertThat(scan).isExactlyInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);
    PositionDeletesTable.PositionDeletesBatchScan deleteScan =
        (PositionDeletesTable.PositionDeletesBatchScan) scan;

    List<ScanTask> tasks = Lists.newArrayList(scan.planFiles());

    assertThat(deleteScan.scanMetrics().scannedDeleteManifests().value())
        .as("Expected to scan one delete manifest")
        .isEqualTo(1);
    assertThat(deleteScan.scanMetrics().skippedDeleteManifests().value())
        .as("Expected to skip three delete manifests")
        .isEqualTo(3);

    assertThat(tasks).hasSize(1);

    ScanTask task = tasks.get(0);
    assertThat(task).isInstanceOf(PositionDeletesScanTask.class);

    Types.StructType partitionType = positionDeletesTable.spec().partitionType();
    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    int filePartition = posDeleteTask.file().partition().get(0, Integer.class);
    StructLike taskPartitionStruct =
        (StructLike)
            constantsMap(posDeleteTask, partitionType).get(MetadataColumns.PARTITION_COLUMN_ID);
    int taskPartition = taskPartitionStruct.get(0, Integer.class);
    assertThat(filePartition).as("Expected correct partition on task's file").isEqualTo(1);
    assertThat(taskPartition).as("Expected correct partition on task's column").isEqualTo(1);

    assertThat(posDeleteTask.file().specId())
        .as("Expected correct partition spec id on task")
        .isEqualTo(0);
    assertThat((Map<Integer, Integer>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct partition spec id on constant column")
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), 0);
    assertThat(posDeleteTask.file().location())
        .as("Expected correct delete file on task")
        .isEqualTo(fileBDeletes().location());
    assertThat((Map<Integer, String>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct delete file on constant column")
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), fileBDeletes().location());
  }

  @TestTemplate
  public void testPositionDeletesBaseTableFilterManifestLevel() {
    testPositionDeletesBaseTableFilter(false);
  }

  @TestTemplate
  public void testPositionDeletesBaseTableFilterEntriesLevel() {
    testPositionDeletesBaseTableFilter(true);
  }

  private void testPositionDeletesBaseTableFilter(boolean transactional) {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    preparePartitionedTable(transactional);

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);

    Expression expression =
        Expressions.and(
            Expressions.equal("data", "u"), // hashes to bucket 0
            Expressions.greaterThan("id", 0));
    BatchScan scan =
        ((PositionDeletesTable.PositionDeletesBatchScan) positionDeletesTable.newBatchScan())
            .baseTableFilter(expression);
    assertThat(scan).isExactlyInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);
    PositionDeletesTable.PositionDeletesBatchScan deleteScan =
        (PositionDeletesTable.PositionDeletesBatchScan) scan;

    List<ScanTask> tasks = Lists.newArrayList(scan.planFiles());

    assertThat(deleteScan.scanMetrics().scannedDeleteManifests().value())
        .as("Expected to scan one delete manifest")
        .isEqualTo(1);
    int expectedSkippedManifests = transactional ? 0 : 3;
    assertThat(deleteScan.scanMetrics().skippedDeleteManifests().value())
        .as("Wrong number of manifests skipped")
        .isEqualTo(expectedSkippedManifests);

    assertThat(tasks).hasSize(1);

    ScanTask task = tasks.get(0);
    assertThat(task).isInstanceOf(PositionDeletesScanTask.class);

    Types.StructType partitionType = positionDeletesTable.spec().partitionType();
    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    // base table filter should only be used to evaluate partitions
    posDeleteTask.residual().isEquivalentTo(Expressions.alwaysTrue());

    int filePartition = posDeleteTask.file().partition().get(0, Integer.class);
    StructLike taskPartitionStruct =
        (StructLike)
            constantsMap(posDeleteTask, partitionType).get(MetadataColumns.PARTITION_COLUMN_ID);
    int taskPartition = taskPartitionStruct.get(0, Integer.class);
    assertThat(filePartition).as("Expected correct partition on task's file").isEqualTo(0);
    assertThat(taskPartition).as("Expected correct partition on task's column").isEqualTo(0);

    assertThat(posDeleteTask.file().specId())
        .as("Expected correct partition spec id on task")
        .isEqualTo(0);
    assertThat((Map<Integer, Integer>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct partition spec id on constant column")
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), 0);
    assertThat(posDeleteTask.file().location())
        .as("Expected correct delete file on task")
        .isEqualTo(fileADeletes().location());
    assertThat((Map<Integer, String>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct delete file on constant column")
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), fileADeletes().location());
  }

  @TestTemplate
  public void testPositionDeletesWithBaseTableFilterNot() {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isEqualTo(2);
    // use identity rather than bucket partition spec,
    // as bucket.project does not support projecting notEq
    table.updateSpec().removeField("data_bucket").addField("id").commit();
    PartitionSpec spec = table.spec();

    String path0 = "/path/to/data-0-deletes.parquet";
    PartitionData partitionData0 = new PartitionData(spec.partitionType());
    partitionData0.set(0, 0);
    DeleteFile deleteFileA =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath(path0)
            .withFileSizeInBytes(10)
            .withPartition(partitionData0)
            .withRecordCount(1)
            .build();

    String path1 = "/path/to/data-1-deletes.parquet";
    PartitionData partitionData1 = new PartitionData(spec.partitionType());
    partitionData1.set(0, 1);
    DeleteFile deleteFileB =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath(path1)
            .withFileSizeInBytes(10)
            .withPartition(partitionData1)
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(deleteFileA).addDeletes(deleteFileB).commit();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);

    Expression expression = Expressions.not(Expressions.equal("id", 0));
    BatchScan scan =
        ((PositionDeletesTable.PositionDeletesBatchScan) positionDeletesTable.newBatchScan())
            .baseTableFilter(expression);
    assertThat(scan).isExactlyInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);
    PositionDeletesTable.PositionDeletesBatchScan deleteScan =
        (PositionDeletesTable.PositionDeletesBatchScan) scan;

    List<ScanTask> tasks = Lists.newArrayList(scan.planFiles());

    assertThat(deleteScan.scanMetrics().scannedDeleteManifests().value())
        .as("Expected to scan one delete manifest")
        .isEqualTo(1);
    assertThat(tasks).hasSize(1);

    ScanTask task = tasks.get(0);
    assertThat(task).isInstanceOf(PositionDeletesScanTask.class);

    Types.StructType partitionType = positionDeletesTable.spec().partitionType();
    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    // base table filter should only be used to evaluate partitions
    posDeleteTask.residual().isEquivalentTo(Expressions.alwaysTrue());

    int filePartition = posDeleteTask.file().partition().get(0, Integer.class);
    StructLike taskPartitionStruct =
        (StructLike)
            constantsMap(posDeleteTask, partitionType).get(MetadataColumns.PARTITION_COLUMN_ID);
    int taskPartition =
        taskPartitionStruct.get(0, Integer.class); // new partition field in position 0
    assertThat(filePartition).as("Expected correct partition on task's file").isEqualTo(1);
    assertThat(taskPartition).as("Expected correct partition on task's column").isEqualTo(1);

    assertThat(posDeleteTask.file().specId())
        .as("Expected correct partition spec id on task")
        .isEqualTo(1);
    assertThat((Map<Integer, Integer>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct partition spec id on constant column")
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), 1);

    assertThat(posDeleteTask.file().location())
        .as("Expected correct delete file on task")
        .isEqualTo(path1);
    assertThat((Map<Integer, String>) constantsMap(posDeleteTask, partitionType))
        .as("Expected correct delete file on constant column")
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), path1);
  }

  @TestTemplate
  public void testPositionDeletesResiduals() {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    preparePartitionedTable();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);

    Expression expression =
        Expressions.and(
            Expressions.equal("partition.data_bucket", 1), Expressions.greaterThan("pos", 1));
    BatchScan scan = positionDeletesTable.newBatchScan().filter(expression);

    assertThat(scan).isExactlyInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);

    List<ScanTask> tasks = Lists.newArrayList(scan.planFiles());
    assertThat(tasks).hasSize(1);

    ScanTask task = tasks.get(0);
    assertThat(task).isInstanceOf(PositionDeletesScanTask.class);

    PositionDeletesScanTask posDeleteTask = (PositionDeletesScanTask) task;

    Expression residual = posDeleteTask.residual();
    UnboundPredicate<?> residualPred =
        TestHelpers.assertAndUnwrap(residual, UnboundPredicate.class);
    assertThat(residualPred.op()).isEqualTo(Expression.Operation.GT);
    assertThat(residualPred.literal()).isEqualTo(Literal.of(1));
  }

  @TestTemplate
  public void testPositionDeletesUnpartitioned() {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    table.updateSpec().removeField(Expressions.bucket("data", BUCKETS_NUMBER)).commit();

    assertThat(table.spec().fields()).as("Table should now be unpartitioned").hasSize(0);

    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    DeleteFile delete1 = newDeletes(dataFile1);
    DeleteFile delete2 = newDeletes(dataFile2);
    table.newRowDelta().addDeletes(delete1).addDeletes(delete2).commit();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);
    BatchScan scan = positionDeletesTable.newBatchScan();
    assertThat(scan).isInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);
    PositionDeletesTable.PositionDeletesBatchScan deleteScan =
        (PositionDeletesTable.PositionDeletesBatchScan) scan;

    List<PositionDeletesScanTask> scanTasks =
        Lists.newArrayList(
            Iterators.transform(
                deleteScan.planFiles().iterator(),
                task -> {
                  assertThat(task).isInstanceOf(PositionDeletesScanTask.class);
                  return (PositionDeletesScanTask) task;
                }));

    assertThat(deleteScan.scanMetrics().scannedDeleteManifests().value())
        .as("Expected to scan 1 manifest")
        .isEqualTo(1);

    assertThat(scanTasks).hasSize(2);
    scanTasks.sort(Comparator.comparing(f -> f.file().pos()));
    assertThat(scanTasks.get(0).file().location()).isEqualTo(delete1.location());
    assertThat(scanTasks.get(1).file().location()).isEqualTo(delete2.location());

    Types.StructType partitionType = Partitioning.partitionType(table);

    assertThat((Map<Integer, String>) constantsMap(scanTasks.get(0), partitionType))
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), delete1.location());
    assertThat((Map<Integer, String>) constantsMap(scanTasks.get(1), partitionType))
        .containsEntry(MetadataColumns.FILE_PATH.fieldId(), delete2.location());
    assertThat((Map<Integer, Integer>) constantsMap(scanTasks.get(0), partitionType))
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), 1);
    assertThat((Map<Integer, Integer>) constantsMap(scanTasks.get(1), partitionType))
        .containsEntry(MetadataColumns.SPEC_ID.fieldId(), 1);

    StructLikeWrapper wrapper = StructLikeWrapper.forType(Partitioning.partitionType(table));

    // Check for null partition values
    PartitionData partitionData = new PartitionData(Partitioning.partitionType(table));
    StructLikeWrapper expected = wrapper.set(partitionData);
    StructLike scanTask1PartitionStruct =
        (StructLike)
            constantsMap(scanTasks.get(0), partitionType).get(MetadataColumns.PARTITION_COLUMN_ID);
    StructLikeWrapper scanTask1Partition = wrapper.copyFor(scanTask1PartitionStruct);

    StructLike scanTask2PartitionStruct =
        (StructLike)
            constantsMap(scanTasks.get(1), partitionType).get(MetadataColumns.PARTITION_COLUMN_ID);
    StructLikeWrapper scanTask2Partition = wrapper.copyFor(scanTask2PartitionStruct);

    assertThat(scanTask1Partition).isEqualTo(expected);
    assertThat(scanTask2Partition).isEqualTo(expected);
  }

  @TestTemplate
  public void testPositionDeletesManyColumns() {
    assumeThat(formatVersion).as("Position deletes are not supported by V1 Tables").isNotEqualTo(1);
    UpdateSchema updateSchema = table.updateSchema();
    for (int i = 0; i <= 2000; i++) {
      updateSchema.addColumn(String.valueOf(i), Types.IntegerType.get());
    }
    updateSchema.commit();

    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data2.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    DeleteFile delete1 = newDeletes(dataFile1);
    DeleteFile delete2 = newDeletes(dataFile2);
    table.newRowDelta().addDeletes(delete1).addDeletes(delete2).commit();

    PositionDeletesTable positionDeletesTable = new PositionDeletesTable(table);
    int expectedIds =
        formatVersion >= 3
            ? 2012 // partition col + 8 columns + 2003 ids inside the deleted row column
            : 2010; // partition col + 6 columns + 2003 ids inside the deleted row column
    assertThat(TypeUtil.indexById(positionDeletesTable.schema().asStruct()).size())
        .isEqualTo(expectedIds);

    BatchScan scan = positionDeletesTable.newBatchScan();
    assertThat(scan).isInstanceOf(PositionDeletesTable.PositionDeletesBatchScan.class);
    PositionDeletesTable.PositionDeletesBatchScan deleteScan =
        (PositionDeletesTable.PositionDeletesBatchScan) scan;

    List<PositionDeletesScanTask> scanTasks =
        Lists.newArrayList(
            Iterators.transform(
                deleteScan.planFiles().iterator(),
                task -> {
                  assertThat(task).isInstanceOf(PositionDeletesScanTask.class);
                  return (PositionDeletesScanTask) task;
                }));
    assertThat(scanTasks).hasSize(2);
    scanTasks.sort(Comparator.comparing(f -> f.file().pos()));
    assertThat(scanTasks.get(0).file().location()).isEqualTo(delete1.location());
    assertThat(scanTasks.get(1).file().location()).isEqualTo(delete2.location());
  }

  @TestTemplate
  public void testFilesTableEstimateSize() throws Exception {
    preparePartitionedTable(true);

    assertEstimatedRowCount(new DataFilesTable(table), 4);
    assertEstimatedRowCount(new AllDataFilesTable(table), 4);
    assertEstimatedRowCount(new AllFilesTable(table), 4);

    if (formatVersion >= 2) {
      assertEstimatedRowCount(new DeleteFilesTable(table), 4);
      assertEstimatedRowCount(new AllDeleteFilesTable(table), 4);
    }
  }

  @TestTemplate
  public void testEntriesTableEstimateSize() throws Exception {
    preparePartitionedTable(true);

    assertEstimatedRowCount(new ManifestEntriesTable(table), 4);
    assertEstimatedRowCount(new AllEntriesTable(table), 4);
  }

  @TestTemplate
  public void testMetadataTableScansOnBranch() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.manageSnapshots().createBranch("testBranch").commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    Table entriesTable = new ManifestEntriesTable(table);
    assertThat(
            rowCount(
                entriesTable
                    .newScan()
                    .useRef("testBranch")
                    .select("status")
                    .filter(Expressions.lessThan("data_file.partition.data_bucket", 0))))
        .as("ManifestEntriesTable on branch should have no entry satisfying the filter")
        .isEqualTo(0);
    assertThat(rowCount(entriesTable.newScan()))
        .as("ManifestEntriesTable on main should have 2 entries")
        .isEqualTo(2);

    Table dataFilesTable = new DataFilesTable(table);
    assertThat(rowCount(dataFilesTable.newScan().useRef("testBranch")))
        .as("DataFilesTable on branch should have 1 file")
        .isEqualTo(1);
    assertThat(rowCount(dataFilesTable.newScan()))
        .as("DataFilesTable on main should have 2 files")
        .isEqualTo(2);

    Table manifestsTable = new ManifestsTable(table);
    assertThat(rowCount(manifestsTable.newScan().useRef("testBranch")))
        .as("ManifestsTable on branch should have 1 manifest")
        .isEqualTo(1);
    assertThat(rowCount(manifestsTable.newScan()))
        .as("ManifestsTable on main should have 2 manifests")
        .isEqualTo(2);

    Table filesTable = new FilesTable(table);
    assertThat(rowCount(filesTable.newScan().useRef("testBranch")))
        .as("FilesTable on branch should have 1 file")
        .isEqualTo(1);
    assertThat(rowCount(filesTable.newScan()))
        .as("FilesTable on main should have 2 files")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testDeleteFilesTableScanOnBranch() throws IOException {
    assumeThat(formatVersion).as("Delete files are not supported by V1 Tables").isGreaterThan(1);

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(fileADeletes()).commit();
    table.manageSnapshots().createBranch("testBranch").commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(fileBDeletes()).commit();

    Table deleteFilesTable = new DeleteFilesTable(table);
    assertThat(rowCount(deleteFilesTable.newScan().useRef("testBranch")))
        .as("DeleteFilesTable on branch should have 1 delete file")
        .isEqualTo(1);
    assertThat(rowCount(deleteFilesTable.newScan()))
        .as("DeleteFilesTable on main should have 2 delete files")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testPuffinFilesTableNoPuffinFiles() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Table puffinFilesTable = new PuffinFilesTable(table);

    assertThat(rowCount(puffinFilesTable.newScan()))
        .as("Puffin files table should have no rows without statistics files or deletion vectors")
        .isEqualTo(0);
  }

  @TestTemplate
  public void testPuffinFilesTableStatisticsFile() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "test",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot, StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, ImmutableList.of(1)),
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(2))));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());
    assertThat(rows).hasSize(1);

    StructLike row = rows.get(0);
    assertThat(row.get(0, Long.class)).isEqualTo(snapshot.snapshotId());
    assertThat(row.get(1, String.class)).isEqualTo(statisticsFile.path());
    assertThat(row.get(2, String.class)).isEqualTo("statistics");
    assertThat(row.get(3, Long.class)).isEqualTo(617L);
    assertThat(row.get(4, Integer.class)).isEqualTo(2);
    assertThat(row.get(5, List.class))
        .containsExactly(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1);
    assertReferencedFields(row, referencedField(1, "id"), referencedField(2, "data"));
  }

  @TestTemplate
  public void testPuffinFilesTableProjection() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "projected",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(1))));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    TableScan scan =
        new PuffinFilesTable(table).newScan().select("file_path", "referenced_blob_count");

    Types.StructType expected =
        new Schema(
                required(
                    2,
                    "file_path",
                    Types.StringType.get(),
                    "Fully qualified location of the Puffin file"),
                required(
                    5,
                    "referenced_blob_count",
                    Types.IntegerType.get(),
                    "Number of distinct blobs in the Puffin file referenced by the selected snapshot"))
            .asStruct();

    assertThat(scan.schema().asStruct()).isEqualTo(expected);

    List<StructLike> rows = rows(scan);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).get(0, String.class)).isEqualTo(statisticsFile.path());
    assertThat(rows.get(0).get(1, Integer.class)).isEqualTo(1);
  }

  @TestTemplate
  public void testPuffinFilesTableAggregatesReferencedMetadata() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "mixed",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot, StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, ImmutableList.of(1)),
                newBlobMetadata(
                    snapshot, StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, ImmutableList.of(1)),
                newBlobMetadata(snapshot, "test-custom-blob-v1", ImmutableList.of(2))));

    table.updateStatistics().setStatistics(statisticsFile).commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());
    assertThat(rows).hasSize(1);

    StructLike row = rows.get(0);
    assertThat(row.get(4, Integer.class)).isEqualTo(3);
    assertThat(row.get(5, List.class))
        .containsExactly(StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1, "test-custom-blob-v1");
    assertReferencedFields(row, referencedField(1, "id"), referencedField(2, "data"));
  }

  @TestTemplate
  public void testPuffinFilesTableResolvesRenamedFieldNames() throws IOException {
    table.updateSchema().addColumn("renamed_column", Types.IntegerType.get()).commit();
    int fieldId = table.schema().findField("renamed_column").fieldId();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "rename",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(fieldId))));
    table.updateStatistics().setStatistics(statisticsFile).commit();

    table.updateSchema().renameColumn("renamed_column", "new_name").commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());
    assertThat(rows).hasSize(1);
    assertReferencedFields(rows.get(0), referencedField(fieldId, "new_name"));
  }

  @TestTemplate
  public void testPuffinFilesTableStatisticsTimeTravel() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();
    StatisticsFile firstStatistics =
        newStatisticsFile(
            firstSnapshot,
            "first",
            ImmutableList.of(
                newBlobMetadata(
                    firstSnapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(1))));
    table.updateStatistics().setStatistics(firstStatistics).commit();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot secondSnapshot = table.currentSnapshot();
    StatisticsFile secondStatistics =
        newStatisticsFile(
            secondSnapshot,
            "second",
            ImmutableList.of(
                newBlobMetadata(
                    secondSnapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(2))));
    table.updateStatistics().setStatistics(secondStatistics).commit();

    Table puffinFilesTable = new PuffinFilesTable(table);

    List<StructLike> firstRows =
        rows(puffinFilesTable.newScan().useSnapshot(firstSnapshot.snapshotId()));
    assertThat(firstRows).hasSize(1);
    assertThat(firstRows.get(0).get(0, Long.class)).isEqualTo(firstSnapshot.snapshotId());
    assertThat(firstRows.get(0).get(1, String.class)).isEqualTo(firstStatistics.path());

    List<StructLike> secondRows =
        rows(puffinFilesTable.newScan().useSnapshot(secondSnapshot.snapshotId()));
    assertThat(secondRows).hasSize(1);
    assertThat(secondRows.get(0).get(0, Long.class)).isEqualTo(secondSnapshot.snapshotId());
    assertThat(secondRows.get(0).get(1, String.class)).isEqualTo(secondStatistics.path());
  }

  @TestTemplate
  public void testPuffinFilesTableUsesCurrentStatisticsRegistrationForTimeTravel()
      throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshot = table.currentSnapshot();

    StatisticsFile firstStatistics =
        newStatisticsFile(
            snapshot,
            "registered-first",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(1))));
    table.updateStatistics().setStatistics(firstStatistics).commit();

    StatisticsFile replacementStatistics =
        newStatisticsFile(
            snapshot,
            "registered-replacement",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(2))));
    table.updateStatistics().setStatistics(replacementStatistics).commit();

    Table puffinFilesTable = new PuffinFilesTable(table);
    List<StructLike> replacementRows =
        rows(puffinFilesTable.newScan().useSnapshot(snapshot.snapshotId()));
    assertThat(replacementRows).hasSize(1);
    assertThat(replacementRows.get(0).get(1, String.class)).isEqualTo(replacementStatistics.path());

    table.updateStatistics().removeStatistics(snapshot.snapshotId()).commit();

    assertThat(rows(puffinFilesTable.newScan().useSnapshot(snapshot.snapshotId()))).isEmpty();
  }

  @TestTemplate
  public void testPuffinFilesTableIgnoresNonPuffinDeleteFiles() throws IOException {
    assumeThat(formatVersion).as("This test requires a V2 table").isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(fileADeletes()).commit();

    assertThat(rows(new PuffinFilesTable(table).newScan())).isEmpty();
  }

  @TestTemplate
  public void testPuffinFilesTableIncludesStatisticsAndDeletionVectors() throws IOException {
    assumeThat(formatVersion).as("Deletion vectors require V3 tables").isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).commit();

    String puffinPath = table.location() + "/data/current-dv.puffin";
    DeleteFile deletionVector = newDeletionVector(puffinPath, 100L, FILE_A, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(deletionVector).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "current",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(1))));
    table.updateStatistics().setStatistics(statisticsFile).commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());
    assertThat(rows).hasSize(2);

    Map<String, StructLike> rowsBySource =
        rows.stream().collect(Collectors.toMap(row -> row.get(2, String.class), row -> row));
    assertThat(rowsBySource).containsOnlyKeys("statistics", "deletion_vector");
    assertThat(rowsBySource.get("statistics").get(1, String.class))
        .isEqualTo(statisticsFile.path());
    assertThat(rowsBySource.get("deletion_vector").get(1, String.class)).isEqualTo(puffinPath);
  }

  @TestTemplate
  public void testPuffinFilesTableAggregatesDeletionVectorsByPuffinPath() throws IOException {
    assumeThat(formatVersion).as("Deletion vectors require V3 tables").isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String puffinPath = table.location() + "/data/shared-dvs.puffin";
    DeleteFile firstDV = newDeletionVector(puffinPath, 200L, FILE_A, 4L, 42L, 1L);
    DeleteFile secondDV = newDeletionVector(puffinPath, 200L, FILE_B, 46L, 44L, 2L);
    table.newRowDelta().addDeletes(firstDV).addDeletes(secondDV).commit();

    Snapshot snapshot = table.currentSnapshot();
    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());

    assertThat(rows).hasSize(1);
    StructLike row = rows.get(0);
    assertThat(row.get(0, Long.class)).isEqualTo(snapshot.snapshotId());
    assertThat(row.get(1, String.class)).isEqualTo(puffinPath);
    assertThat(row.get(2, String.class)).isEqualTo("deletion_vector");
    assertThat(row.get(3, Long.class)).isEqualTo(200L);
    assertThat(row.get(4, Integer.class)).isEqualTo(2);
    assertThat(row.get(5, List.class)).containsExactly(StandardBlobTypes.DV_V1);
    assertReferencedFields(
        row,
        referencedField(
            MetadataColumns.ROW_POSITION.fieldId(), MetadataColumns.ROW_POSITION.name()));
  }

  @TestTemplate
  public void testPuffinFilesTableKeepsIdenticalBlobRangesInDifferentFiles() throws IOException {
    assumeThat(formatVersion).as("Deletion vectors require V3 tables").isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String firstPuffinPath = table.location() + "/data/first-dv.puffin";
    String secondPuffinPath = table.location() + "/data/second-dv.puffin";
    DeleteFile firstDV = newDeletionVector(firstPuffinPath, 100L, FILE_A, 4L, 44L, 1L);
    DeleteFile secondDV = newDeletionVector(secondPuffinPath, 100L, FILE_B, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(firstDV).addDeletes(secondDV).commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());
    assertThat(rows).hasSize(2);

    Map<String, StructLike> rowsByPath =
        rows.stream().collect(Collectors.toMap(row -> row.get(1, String.class), row -> row));
    assertThat(rowsByPath).containsOnlyKeys(firstPuffinPath, secondPuffinPath);
    assertThat(rowsByPath.get(firstPuffinPath).get(4, Integer.class)).isEqualTo(1);
    assertThat(rowsByPath.get(secondPuffinPath).get(4, Integer.class)).isEqualTo(1);
  }

  @TestTemplate
  public void testPuffinFilesTableDeletionVectorTimeTravel() throws IOException {
    assumeThat(formatVersion).as("Deletion vectors require V3 tables").isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String firstPuffinPath = table.location() + "/data/first-snapshot-dv.puffin";
    DeleteFile firstDV = newDeletionVector(firstPuffinPath, 100L, FILE_A, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(firstDV).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    String secondPuffinPath = table.location() + "/data/second-snapshot-dv.puffin";
    DeleteFile secondDV = newDeletionVector(secondPuffinPath, 100L, FILE_B, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(secondDV).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    Table puffinFilesTable = new PuffinFilesTable(table);

    List<StructLike> firstRows =
        rows(puffinFilesTable.newScan().useSnapshot(firstSnapshot.snapshotId()));
    assertThat(firstRows).hasSize(1);
    assertThat(firstRows.get(0).get(0, Long.class)).isEqualTo(firstSnapshot.snapshotId());
    assertThat(firstRows.get(0).get(1, String.class)).isEqualTo(firstPuffinPath);

    List<StructLike> secondRows =
        rows(puffinFilesTable.newScan().useSnapshot(secondSnapshot.snapshotId()));
    assertThat(secondRows).hasSize(2);
    assertThat(secondRows.stream().map(row -> row.get(0, Long.class)).collect(Collectors.toSet()))
        .containsOnly(secondSnapshot.snapshotId());
    assertThat(secondRows.stream().map(row -> row.get(1, String.class)).collect(Collectors.toSet()))
        .containsExactlyInAnyOrder(firstPuffinPath, secondPuffinPath);
  }

  @TestTemplate
  public void testPuffinFilesTableScanOnBranch() throws IOException {
    assumeThat(formatVersion).as("Deletion vectors require V3 tables").isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    String branchPuffinPath = table.location() + "/data/branch-dv.puffin";
    DeleteFile branchDV = newDeletionVector(branchPuffinPath, 100L, FILE_A, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(branchDV).commit();
    table.manageSnapshots().createBranch("testBranch").commit();

    String mainPuffinPath = table.location() + "/data/main-dv.puffin";
    DeleteFile mainDV = newDeletionVector(mainPuffinPath, 100L, FILE_B, 4L, 44L, 1L);
    table.newRowDelta().addDeletes(mainDV).commit();

    Table puffinFilesTable = new PuffinFilesTable(table);

    List<StructLike> branchRows = rows(puffinFilesTable.newScan().useRef("testBranch"));
    assertThat(branchRows).hasSize(1);
    assertThat(branchRows.get(0).get(1, String.class)).isEqualTo(branchPuffinPath);

    List<StructLike> mainRows = rows(puffinFilesTable.newScan());
    assertThat(mainRows).hasSize(2);
    assertThat(mainRows.stream().map(row -> row.get(1, String.class)).collect(Collectors.toSet()))
        .containsExactlyInAnyOrder(branchPuffinPath, mainPuffinPath);
  }

  @TestTemplate
  public void testPuffinFilesTableAllowsNullNamesForDroppedFields() throws IOException {
    table.updateSchema().addColumn("dropped_column", Types.IntegerType.get()).commit();
    int fieldId = table.schema().findField("dropped_column").fieldId();

    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snapshot = table.currentSnapshot();
    StatisticsFile statisticsFile =
        newStatisticsFile(
            snapshot,
            "dropped",
            ImmutableList.of(
                newBlobMetadata(
                    snapshot,
                    StandardBlobTypes.APACHE_DATASKETCHES_THETA_V1,
                    ImmutableList.of(fieldId))));

    table.updateStatistics().setStatistics(statisticsFile).commit();
    table.updateSchema().deleteColumn("dropped_column").commit();

    List<StructLike> rows = rows(new PuffinFilesTable(table).newScan());

    assertThat(rows).hasSize(1);
    assertReferencedFields(rows.get(0), referencedField(fieldId, null));
  }

  @TestTemplate
  public void testPuffinFilesTableEmptyTable() throws IOException {
    Table puffinFilesTable = new PuffinFilesTable(table);

    assertThat(rowCount(puffinFilesTable.newScan()))
        .as("Puffin files table should be empty when the table has no snapshot")
        .isEqualTo(0);
  }

  @SuppressWarnings("unchecked")
  private static void assertReferencedFields(StructLike row, ReferencedField... expectedFields) {
    List<StructLike> actualFields = row.get(6, List.class);
    assertThat(actualFields).hasSize(expectedFields.length);

    for (int index = 0; index < expectedFields.length; index += 1) {
      ReferencedField expectedField = expectedFields[index];
      StructLike actualField = actualFields.get(index);

      assertThat(actualField.get(0, Integer.class))
          .as("Referenced field ID at position %s", index)
          .isEqualTo(expectedField.fieldId());
      assertThat(actualField.get(1, String.class))
          .as("Current field name at position %s", index)
          .isEqualTo(expectedField.currentFieldName());
    }
  }

  private static ReferencedField referencedField(int fieldId, String currentFieldName) {
    return new ReferencedField(fieldId, currentFieldName);
  }

  private static class ReferencedField {
    private final int fieldId;
    private final String currentFieldName;

    private ReferencedField(int fieldId, String currentFieldName) {
      this.fieldId = fieldId;
      this.currentFieldName = currentFieldName;
    }

    private int fieldId() {
      return fieldId;
    }

    private String currentFieldName() {
      return currentFieldName;
    }
  }

  private StatisticsFile newStatisticsFile(
      Snapshot snapshot, String suffix, List<BlobMetadata> blobMetadata) {
    String statisticsPath =
        table.location() + "/metadata/" + snapshot.snapshotId() + "-" + suffix + ".stats";
    return new GenericStatisticsFile(
        snapshot.snapshotId(), statisticsPath, 617L, 523L, blobMetadata);
  }

  private BlobMetadata newBlobMetadata(Snapshot snapshot, String type, List<Integer> fieldIds) {
    return new GenericBlobMetadata(
        type,
        snapshot.snapshotId(),
        snapshot.sequenceNumber(),
        fieldIds,
        ImmutableMap.of("ndv", "2"));
  }

  private DeleteFile newDeletionVector(
      String puffinPath,
      long puffinFileSize,
      DataFile referencedDataFile,
      long contentOffset,
      long contentSizeInBytes,
      long recordCount) {
    return FileMetadata.deleteFileBuilder(table.spec())
        .ofPositionDeletes()
        .withFormat(FileFormat.PUFFIN)
        .withPath(puffinPath)
        .withPartition(referencedDataFile.partition())
        .withFileSizeInBytes(puffinFileSize)
        .withReferencedDataFile(referencedDataFile.location())
        .withContentOffset(contentOffset)
        .withContentSizeInBytes(contentSizeInBytes)
        .withRecordCount(recordCount)
        .build();
  }

  private static List<StructLike> rows(TableScan scan) throws IOException {
    ImmutableList.Builder<StructLike> rows = ImmutableList.builder();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task).isInstanceOf(DataTask.class);

        try (CloseableIterable<StructLike> taskRows = task.asDataTask().rows()) {
          for (StructLike row : taskRows) {
            rows.add(copyRow(row));
          }
        }
      }
    }

    return rows.build();
  }

  private static StructLike copyRow(StructLike row) {
    Object[] values = new Object[row.size()];

    for (int pos = 0; pos < row.size(); pos += 1) {
      values[pos] = row.get(pos, Object.class);
    }

    return TestHelpers.Row.of(values);
  }

  private int rowCount(TableScan scan) throws IOException {
    int count = 0;
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<StructLike> rows = task.asDataTask().rows()) {
          count += Iterators.size(rows.iterator());
        }
      }
    }
    return count;
  }

  private void assertEstimatedRowCount(Table metadataTable, int size) throws Exception {
    TableScan scan = metadataTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      List<FileScanTask> taskList = Lists.newArrayList(tasks);
      assertThat(taskList)
          .isNotEmpty()
          .allSatisfy(task -> assertThat(task.estimatedRowsCount()).isEqualTo(size));
    }
  }
}
