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
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.TypeUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestEntriesMetadataTable extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testEntriesTable() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table entriesTable = new ManifestEntriesTable(table);

    Schema readSchema = ManifestEntry.getSchema(table.spec().partitionType());
    Schema expectedSchema =
        TypeUtil.join(readSchema, MetricsUtil.readableMetricsSchema(table.schema(), readSchema));

    assertThat(entriesTable.schema().asStruct())
        .as("A tableScan.select() should prune the schema")
        .isEqualTo(expectedSchema.asStruct());
  }

  @TestTemplate
  public void testEntriesTableScan() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Table entriesTable = new ManifestEntriesTable(table);
    TableScan scan = entriesTable.newScan();

    Schema readSchema = ManifestEntry.getSchema(table.spec().partitionType());
    Schema expectedSchema =
        TypeUtil.join(readSchema, MetricsUtil.readableMetricsSchema(table.schema(), readSchema));

    assertThat(scan.schema().asStruct())
        .as("A tableScan.select() should prune the schema")
        .isEqualTo(expectedSchema.asStruct());

    FileScanTask file = Iterables.getOnlyElement(scan.planFiles());
    assertThat(file.file().path())
        .as("Data file should be the table's manifest")
        .isEqualTo(table.currentSnapshot().allManifests(table.io()).get(0).path());

    assertThat(file.file().recordCount()).as("Should contain 2 data file records").isEqualTo(2);
  }

  @TestTemplate
  public void testSplitPlanningWithMetadataSplitSizeProperty() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    // set the split size to a large value so that both manifests are in 1 split
    table
        .updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(128 * 1024 * 1024))
        .commit();

    Table entriesTable = new ManifestEntriesTable(table);

    assertThat(entriesTable.newScan().planTasks()).hasSize(1);

    // set the split size to a small value so that manifests end up in different splits
    table.updateProperties().set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(1)).commit();

    assertThat(entriesTable.newScan().planTasks()).hasSize(2);

    // override the table property with a large value so that both manifests are in 1 split
    TableScan scan =
        entriesTable
            .newScan()
            .option(TableProperties.SPLIT_SIZE, String.valueOf(128 * 1024 * 1024));

    assertThat(scan.planTasks()).hasSize(1);
  }

  @TestTemplate
  public void testSplitPlanningWithDefaultMetadataSplitSize() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    int splitSize =
        (int) TableProperties.METADATA_SPLIT_SIZE_DEFAULT; // default split size is 32 MB

    Table entriesTable = new ManifestEntriesTable(table);
    assertThat(entriesTable.currentSnapshot().allManifests(table.io())).hasSize(1);

    int expectedSplits =
        ((int) entriesTable.currentSnapshot().allManifests(table.io()).get(0).length()
                + splitSize
                - 1)
            / splitSize;

    TableScan scan = entriesTable.newScan();

    assertThat(scan.planTasks()).hasSize(expectedSplits);
  }

  @TestTemplate
  public void testEntriesTableWithDeleteManifests() {
    assumeThat(formatVersion).as("Only V2 Tables Support Deletes").isGreaterThanOrEqualTo(2);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    Table entriesTable = new ManifestEntriesTable(table);
    TableScan scan = entriesTable.newScan();

    Schema readSchema = ManifestEntry.getSchema(table.spec().partitionType());
    Schema expectedSchema =
        TypeUtil.join(readSchema, MetricsUtil.readableMetricsSchema(table.schema(), readSchema));

    assertThat(scan.schema().asStruct())
        .as("A tableScan.select() should prune the schema")
        .isEqualTo(expectedSchema.asStruct());

    List<FileScanTask> files =
        ImmutableList.sortedCopyOf(
            Comparator.comparingInt(
                (FileScanTask t) ->
                    ((BaseEntriesTable.ManifestReadTask) t).manifest().content().id()),
            scan.planFiles());

    assertThat(files.get(0).file().path())
        .as("Data file should be the table's manifest")
        .isEqualTo(table.currentSnapshot().dataManifests(table.io()).get(0).path());
    assertThat(files.get(0).file().recordCount())
        .as("Should contain 2 data file records")
        .isEqualTo(2);
    assertThat(files.get(1).file().path())
        .as("Delete file should be in the table manifest")
        .isEqualTo(table.currentSnapshot().deleteManifests(table.io()).get(0).path());
    assertThat(files.get(1).file().recordCount())
        .as("Should contain 1 delete file record")
        .isEqualTo(1);
  }
}
