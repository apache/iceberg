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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestColumnUpdate extends TestBase {
  private static final int FORMAT_VERSION = 4;
  @TempDir private File tablePath;

  @Test
  public void invalidParameters() {
    BaseTable table =
        TestTables.create(tablePath, "invalid_params_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(() -> table.newColumnUpdate().addColumnUpdate(FILE_A, updateFile).commit())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("No field IDs provided");

    assertThatThrownBy(() -> table.newColumnUpdate().withFieldIds(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: null");

    assertThatThrownBy(() -> table.newColumnUpdate().withFieldIds(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid field IDs: empty");

    assertThatThrownBy(
            () ->
                table.newColumnUpdate().withFieldIds(List.of(1)).addColumnUpdate(null, updateFile))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Base file is null");

    assertThatThrownBy(
            () -> table.newColumnUpdate().withFieldIds(List.of(1)).addColumnUpdate(FILE_A, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column update file is null");

    // Test mismatched spec IDs
    PartitionSpec specWithDifferentId =
        PartitionSpec.builderFor(SCHEMA).withSpecId(99).bucket("data", BUCKETS_NUMBER).build();
    DataFile updateFileDifferentSpec =
        DataFiles.builder(specWithDifferentId)
            .withPath("/path/to/data-update-different-spec.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () ->
                table
                    .newColumnUpdate()
                    .withFieldIds(List.of(1))
                    .addColumnUpdate(FILE_A, updateFileDifferentSpec))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Base file spec ID")
        .hasMessageContaining("doesn't match update file spec ID");

    DataFile updateFileMoreRows =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update-more-rows.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(100) // more rows than FILE_A
            .build();

    assertThatThrownBy(
            () ->
                table
                    .newColumnUpdate()
                    .withFieldIds(List.of(1))
                    .addColumnUpdate(FILE_A, updateFileMoreRows))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Update file can't have more rows than the base file");
  }

  @TestTemplate
  public void unsupportedFormatVersions() {
    assumeThat(formatVersion).isLessThan(4);

    File tableDir = new File(tablePath, "v" + formatVersion);
    BaseTable table =
        TestTables.create(
            tableDir, "old_format_test_v" + formatVersion, SCHEMA, SPEC, formatVersion);
    table.newAppend().appendFile(FILE_A).commit();

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-update.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () ->
                table
                    .newColumnUpdate()
                    .withFieldIds(List.of(1))
                    .addColumnUpdate(FILE_A, updateFile)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column updates are supported from V4");
  }

  @Test
  public void updateOnEmptyTable() {
    BaseTable table =
        TestTables.create(tablePath, "empty_table_test", SCHEMA, SPEC, FORMAT_VERSION);

    assertThatThrownBy(
            () ->
                table
                    .newColumnUpdate()
                    .withFieldIds(List.of(1))
                    .addColumnUpdate(FILE_A, FILE_B)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Column update is not supported on empty tables");
  }

  @Test
  public void fileToUpdateNotFound() {
    BaseTable table =
        TestTables.create(
            tablePath, "referenced_file_not_found_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();

    DataFile updateFile =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    assertThatThrownBy(
            () ->
                table
                    .newColumnUpdate()
                    .withFieldIds(List.of(1))
                    .addColumnUpdate(FILE_B, updateFile)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unable to find base data file: /path/to/data-b.parquet");
  }

  @Test
  public void noUpdateFilesProvided() {
    BaseTable table =
        TestTables.create(tablePath, "no_update_files_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile manifestBeforeUpdate = table.currentSnapshot().allManifests(table.io()).get(0);

    table.newColumnUpdate().withFieldIds(List.of(1)).commit();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile manifestAfterUpdate = table.currentSnapshot().allManifests(table.io()).get(0);
    assertThat(manifestBeforeUpdate).isEqualTo(manifestAfterUpdate);
  }

  @Test
  public void updateSingleColumn() throws IOException {
    BaseTable table =
        TestTables.create(tablePath, "update_column_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newAppend().appendFile(FILE_B).commit();

    List<ManifestFile> manifestsBeforeUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsBeforeUpdate).hasSize(2);

    ManifestFile manifestToKeepIntact = null;
    ManifestEntry<DataFile> fileAEntryBeforeUpdate = null;
    ManifestEntry<DataFile> fileBEntryBeforeUpdate = null;
    for (ManifestFile manifest : manifestsBeforeUpdate) {
      if (manifest.addedFilesCount() == 2) {
        fileAEntryBeforeUpdate = findEntryInManifest(manifest, FILE_A.location(), table.io());
        fileBEntryBeforeUpdate = findEntryInManifest(manifest, FILE_B.location(), table.io());
      } else {
        manifestToKeepIntact = manifest;
      }
    }

    DataFile updateFileA =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    table.newColumnUpdate().withFieldIds(List.of(1)).addColumnUpdate(FILE_A, updateFileA).commit();

    List<ManifestFile> manifestsAfterUpdate = table.currentSnapshot().allManifests(table.io());
    assertThat(manifestsAfterUpdate).hasSize(2);
    // One manifest should be entirely unchanged
    assertThat(manifestsAfterUpdate).contains(manifestToKeepIntact);

    ManifestFile tmpManifest = manifestToKeepIntact;
    ManifestFile updatedManifest =
        manifestsAfterUpdate.stream().filter(m -> !m.equals(tmpManifest)).findFirst().get();
    List<ManifestEntry<DataFile>> entriesAfterUpdate = Lists.newArrayList();
    try (ManifestReader<DataFile> reader = ManifestFiles.read(updatedManifest, table.io())) {
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        entriesAfterUpdate.add(entry.copy());
      }
    }
    assertThat(entriesAfterUpdate).hasSize(2);

    ManifestEntry<DataFile> fileAEntryAfterUpdate =
        findEntryInManifest(updatedManifest, FILE_A.location(), table.io());
    ManifestEntry<DataFile> fileBEntryAfterUpdate =
        findEntryInManifest(updatedManifest, FILE_B.location(), table.io());

    // FILE_B entry should be unchanged
    assertThat(fileBEntryAfterUpdate.status()).isEqualTo(fileBEntryBeforeUpdate.status());
    assertThat(fileBEntryAfterUpdate.snapshotId()).isEqualTo(fileBEntryBeforeUpdate.snapshotId());
    assertThat(fileBEntryAfterUpdate.file().columnUpdateDetails())
        .isEqualTo(fileBEntryBeforeUpdate.file().columnUpdateDetails());

    // FILE_A entry should have column update details set
    assertThat(fileAEntryBeforeUpdate.file().columnUpdateDetails()).isNull();
    assertThat(fileAEntryAfterUpdate.file().columnUpdateDetails()).isNotNull();
    assertThat(fileAEntryAfterUpdate.file().columnUpdateDetails()).hasSize(1);
    assertThat(fileAEntryAfterUpdate.file().columnUpdateDetails().get(0).fieldIds())
        .isEqualTo(List.of(1));
    assertThat(fileAEntryAfterUpdate.file().columnUpdateDetails().get(0).filePath())
        .isEqualTo(updateFileA.location());
  }

  @Test
  public void updateColumnWithFieldIdOverlap() throws IOException {
    BaseTable table =
        TestTables.create(tablePath, "update_column_overlap_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    DataFile updateFileA =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    table
        .newColumnUpdate()
        .withFieldIds(List.of(1, 2))
        .addColumnUpdate(FILE_A, updateFileA)
        .commit();

    DataFile updateFileB =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update2.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    table
        .newColumnUpdate()
        .withFieldIds(List.of(2, 3))
        .addColumnUpdate(FILE_A, updateFileB)
        .commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    ManifestEntry<DataFile> fileAEntry =
        findEntryInManifest(manifests.iterator().next(), FILE_A.location(), table.io());
    assertThat(fileAEntry.file().columnUpdateDetails()).hasSize(2);
    assertThat(fileAEntry.file().columnUpdateDetails().get(0).fieldIds()).isEqualTo(List.of(1));
    assertThat(fileAEntry.file().columnUpdateDetails().get(0).filePath())
        .isEqualTo(updateFileA.location());
    assertThat(fileAEntry.file().columnUpdateDetails().get(1).fieldIds()).isEqualTo(List.of(2, 3));
    assertThat(fileAEntry.file().columnUpdateDetails().get(1).filePath())
        .isEqualTo(updateFileB.location());
  }

  @Test
  public void updateColumnWithSameFieldIds() throws IOException {
    BaseTable table =
        TestTables.create(tablePath, "update_same_columns_test", SCHEMA, SPEC, FORMAT_VERSION);
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    DataFile updateFileA1 =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update1.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    table
        .newColumnUpdate()
        .withFieldIds(List.of(1, 2))
        .addColumnUpdate(FILE_A, updateFileA1)
        .commit();

    DataFile updateFileA2 =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data-a_update2.parquet")
            .withFileSizeInBytes(2)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();

    table
        .newColumnUpdate()
        .withFieldIds(List.of(1, 2))
        .addColumnUpdate(FILE_A, updateFileA2)
        .commit();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(1);

    ManifestEntry<DataFile> fileAEntry =
        findEntryInManifest(manifests.iterator().next(), FILE_A.location(), table.io());
    assertThat(fileAEntry.file().columnUpdateDetails()).hasSize(1);
    assertThat(fileAEntry.file().columnUpdateDetails().get(0).fieldIds()).isEqualTo(List.of(1, 2));
    assertThat(fileAEntry.file().columnUpdateDetails().get(0).filePath())
        .isEqualTo(updateFileA2.location());
  }

  private ManifestEntry<DataFile> findEntryInManifest(
      ManifestFile manifest, String fileLocation, FileIO io) throws IOException {
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, io)) {
      for (ManifestEntry<DataFile> entry : reader.entries()) {
        if (entry.file().location().equals(fileLocation)) {
          return entry.copy();
        }
      }
    }
    throw new IllegalStateException("No manifest found containing file: " + fileLocation);
  }
}
