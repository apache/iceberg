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

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.TestTemplate;

public class TestManifestReader extends TestBase {

  private static final RecursiveComparisonConfiguration FILE_COMPARISON_CONFIG =
      RecursiveComparisonConfiguration.builder()
          .withIgnoredFields(
              "dataSequenceNumber",
              "fileOrdinal",
              "fileSequenceNumber",
              "fromProjectionPos",
              "manifestLocation",
              "partitionData.partitionType.fieldsById")
          .build();

  @TestTemplate
  public void testManifestReaderWithEmptyInheritableMetadata() throws IOException {
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 1000L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      assertThat(entry.status()).isEqualTo(Status.EXISTING);
      assertThat(entry.file().location()).isEqualTo(FILE_A.location());
      assertThat(entry.snapshotId()).isEqualTo(1000L);
    }
  }

  @TestTemplate
  public void testReaderWithFilterWithoutSelect() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, FILE_IO, table.specs())
            .filterRows(Expressions.equal("id", 0))) {
      List<DataFile> files = Streams.stream(reader).collect(Collectors.toList());

      // note that all files are returned because the reader returns data files that may match, and
      // the partition is
      // bucketing by data, which doesn't help filter files
      assertThat(files)
          .usingRecursiveComparison(FILE_COMPARISON_CONFIG)
          .isEqualTo(Lists.newArrayList(FILE_A, FILE_B, FILE_C));
    }
  }

  @TestTemplate
  public void testInvalidUsage() throws IOException {
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    assertThatThrownBy(() -> ManifestFiles.read(manifest, FILE_IO))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read from ManifestFile with null (unassigned) snapshot ID");
  }

  @TestTemplate
  public void testManifestReaderWithPartitionMetadata() throws IOException {
    assumeThat(formatVersion <= 3).as("Parquet Manifests in V4+ do not have Metadata");
    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      assertThat(entry.snapshotId()).isEqualTo(123L);

      List<Types.NestedField> fields =
          ((PartitionData) entry.file().partition()).getPartitionType().fields();
      assertThat(fields).hasSize(1);
      assertThat(fields.get(0).fieldId()).isEqualTo(1000);
      assertThat(fields.get(0).name()).isEqualTo("data_bucket");
      assertThat(fields.get(0).type()).isEqualTo(Types.IntegerType.get());
    }
  }

  @TestTemplate
  public void testManifestReaderWithUpdatedPartitionMetadataForV1Table() throws IOException {
    PartitionSpec spec =
        PartitionSpec.builderFor(table.schema()).bucket("id", 8).bucket("data", 16).build();
    table.ops().commit(table.ops().current(), table.ops().current().updatePartitionSpec(spec));

    ManifestFile manifest = writeManifest(1000L, manifestEntry(Status.EXISTING, 123L, FILE_A));
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      ManifestEntry<DataFile> entry = Iterables.getOnlyElement(reader.entries());
      assertThat(entry.snapshotId()).isEqualTo(123L);

      List<Types.NestedField> fields =
          ((PartitionData) entry.file().partition()).getPartitionType().fields();
      assertThat(fields).hasSize(2);
      assertThat(fields.get(0).fieldId()).isEqualTo(1000);
      assertThat(fields.get(0).name()).isEqualTo("id_bucket");
      assertThat(fields.get(0).type()).isEqualTo(Types.IntegerType.get());

      assertThat(fields.get(1).fieldId()).isEqualTo(1001);
      assertThat(fields.get(1).name()).isEqualTo("data_bucket");
      assertThat(fields.get(1).type()).isEqualTo(Types.IntegerType.get());
    }
  }

  @TestTemplate
  public void testDataFilePositions() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      long expectedPos = 0L;
      for (DataFile file : reader) {
        assertThat(file.pos()).as("Position should match").isEqualTo(expectedPos);
        expectedPos += 1;
      }
    }
  }

  @TestTemplate
  public void testDataFileManifestPaths() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE_A, FILE_B, FILE_C);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      for (DataFile file : reader) {
        assertThat(file.manifestLocation()).isEqualTo(manifest.path());
      }
    }
  }

  @TestTemplate
  public void testDeleteFilePositions() throws IOException {
    assumeThat(formatVersion).as("Delete files only work for format version 2").isEqualTo(2);
    ManifestFile manifest =
        writeDeleteManifest(formatVersion, 1000L, FILE_A_DELETES, FILE_B_DELETES);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, null)) {
      long expectedPos = 0L;
      for (DeleteFile file : reader) {
        assertThat(file.pos()).as("Position should match").isEqualTo(expectedPos);
        expectedPos += 1;
      }
    }
  }

  @TestTemplate
  public void testDeleteFileManifestPaths() throws IOException {
    assumeThat(formatVersion)
        .as("Delete files only work for format version 2 or higher")
        .isGreaterThanOrEqualTo(2);
    ManifestFile manifest =
        writeDeleteManifest(formatVersion, 1000L, FILE_A_DELETES, FILE_B_DELETES);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, table.specs())) {
      for (DeleteFile file : reader) {
        assertThat(file.manifestLocation()).isEqualTo(manifest.path());
      }
    }
  }

  @TestTemplate
  public void testDeleteFilesWithReferences() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    DeleteFile deleteFile1 = newDeleteFileWithRef(FILE_A);
    DeleteFile deleteFile2 = newDeleteFileWithRef(FILE_B);
    ManifestFile manifest = writeDeleteManifest(formatVersion, 1000L, deleteFile1, deleteFile2);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, table.specs())) {
      for (DeleteFile deleteFile : reader) {
        if (deleteFile.location().equals(deleteFile1.location())) {
          assertThat(deleteFile.referencedDataFile()).isEqualTo(FILE_A.location());
        } else {
          assertThat(deleteFile.referencedDataFile()).isEqualTo(FILE_B.location());
        }
      }
    }
  }

  @TestTemplate
  public void testDVs() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    DeleteFile dv1 = newDV(FILE_A);
    DeleteFile dv2 = newDV(FILE_B);
    ManifestFile manifest = writeDeleteManifest(formatVersion, 1000L, dv1, dv2);
    try (ManifestReader<DeleteFile> reader =
        ManifestFiles.readDeleteManifest(manifest, FILE_IO, table.specs())) {
      for (DeleteFile dv : reader) {
        if (dv.location().equals(dv1.location())) {
          assertThat(dv.location()).isEqualTo(dv1.location());
          assertThat(dv.referencedDataFile()).isEqualTo(FILE_A.location());
          assertThat(dv.contentOffset()).isEqualTo(dv1.contentOffset());
          assertThat(dv.contentSizeInBytes()).isEqualTo(dv1.contentSizeInBytes());
        } else {
          assertThat(dv.location()).isEqualTo(dv2.location());
          assertThat(dv.referencedDataFile()).isEqualTo(FILE_B.location());
          assertThat(dv.contentOffset()).isEqualTo(dv2.contentOffset());
          assertThat(dv.contentSizeInBytes()).isEqualTo(dv2.contentSizeInBytes());
        }
      }
    }
  }

  @TestTemplate
  public void testDataFileSplitOffsetsNullWhenInvalid() throws IOException {
    DataFile invalidOffset =
        DataFiles.builder(SPEC)
            .withPath("/path/to/invalid-offsets.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withSplitOffsets(ImmutableList.of(2L, 1000L)) // Offset 1000 is out of bounds
            .build();
    ManifestFile manifest = writeManifest(1000L, invalidOffset);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO, table.specs())) {
      DataFile file = Iterables.getOnlyElement(reader);
      assertThat(file.splitOffsets()).isNull();
    }
  }
}
