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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Round-trip tests for {@link RootManifestWriter} and {@link RootManifestReader}. */
public class TestRootManifest {
  private static final String DATA_MANIFEST_PATH = "s3://bucket/table/data-m1.parquet";
  private static final String DELETE_MANIFEST_PATH = "s3://bucket/table/delete-m1.parquet";
  private static final long LENGTH = 4096L;
  private static final int SPEC_ID = 1;
  private static final long SEQ_NUM = 10L;
  private static final long MIN_SEQ_NUM = 5L;
  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final int ADDED_FILES = 3;
  private static final long ADDED_ROWS = 1500L;
  private static final int EXISTING_FILES = 7;
  private static final long EXISTING_ROWS = 3500L;
  private static final int DELETED_FILES = 1;
  private static final long DELETED_ROWS = 200L;
  private static final long SNAPSHOT_FIRST_ROW_ID = 1000L;
  private static final ByteBuffer KEY_METADATA =
      ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6, 7, 8});

  // Minimal schema + spec used to drive the union partition type. The union for an unpartitioned
  // table is an empty struct; root manifest tests do not exercise partitioned content so this
  // matches the production shape for the only callers that read these files (the SnapshotProducer
  // path threads the table's real schema and specs).
  private static final Schema TABLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(0, PartitionSpec.unpartitioned());

  // A v4 leaf manifest (format_version = 4).
  private static final ManifestFile DATA_MANIFEST =
      new GenericManifestFile(
          DATA_MANIFEST_PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DATA,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          null /* no partition summaries */,
          null /* no key metadata */,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          null /* no firstRowId */);

  // A v4 leaf delete manifest (format_version = 4).
  private static final ManifestFile DELETE_MANIFEST =
      new GenericManifestFile(
          DELETE_MANIFEST_PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DELETES,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          null /* no partition summaries */,
          null /* no key metadata */,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          null /* no firstRowId */);

  // A v4 data manifest with key metadata.
  private static final ManifestFile DATA_MANIFEST_WITH_KEY =
      new GenericManifestFile(
          DATA_MANIFEST_PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DATA,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          null /* no partition summaries */,
          KEY_METADATA,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          null /* no firstRowId */);

  @Test
  public void testWriteForVersionLessThan4Fails() {
    OutputFile file = new InMemoryOutputFile();
    assertThatThrownBy(
            () ->
                RootManifests.write(
                    3,
                    file,
                    PlaintextEncryptionManager.instance(),
                    SNAPSHOT_ID,
                    null,
                    SEQ_NUM,
                    null,
                    TABLE_SCHEMA,
                    SPECS_BY_ID))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("4");
  }

  @Test
  public void testRoundTripDataManifest() throws IOException {
    List<ManifestFile> manifests = writeAndRead(DATA_MANIFEST);

    assertThat(manifests).hasSize(1);
    ManifestFile result = manifests.get(0);

    assertThat(result.path()).isEqualTo(DATA_MANIFEST_PATH);
    assertThat(result.length()).isEqualTo(LENGTH);
    assertThat(result.partitionSpecId()).isEqualTo(SPEC_ID);
    assertThat(result.content()).isEqualTo(ManifestContent.DATA);
    assertThat(result.sequenceNumber()).isEqualTo(SEQ_NUM);
    assertThat(result.minSequenceNumber()).isEqualTo(MIN_SEQ_NUM);
    assertThat(result.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat(result.addedRowsCount()).isEqualTo(ADDED_ROWS);
    assertThat(result.existingFilesCount()).isEqualTo(EXISTING_FILES);
    assertThat(result.existingRowsCount()).isEqualTo(EXISTING_ROWS);
    assertThat(result.deletedFilesCount()).isEqualTo(DELETED_FILES);
    assertThat(result.deletedRowsCount()).isEqualTo(DELETED_ROWS);
    assertThat(result.keyMetadata()).isNull();
  }

  @Test
  public void testRoundTripDeleteManifest() throws IOException {
    List<ManifestFile> manifests = writeAndRead(DELETE_MANIFEST);

    assertThat(manifests).hasSize(1);
    ManifestFile result = manifests.get(0);

    assertThat(result.path()).isEqualTo(DELETE_MANIFEST_PATH);
    assertThat(result.length()).isEqualTo(LENGTH);
    assertThat(result.partitionSpecId()).isEqualTo(SPEC_ID);
    assertThat(result.content()).isEqualTo(ManifestContent.DELETES);
    assertThat(result.sequenceNumber()).isEqualTo(SEQ_NUM);
    assertThat(result.minSequenceNumber()).isEqualTo(MIN_SEQ_NUM);
    assertThat(result.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(result.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat(result.addedRowsCount()).isEqualTo(ADDED_ROWS);
    assertThat(result.existingFilesCount()).isEqualTo(EXISTING_FILES);
    assertThat(result.existingRowsCount()).isEqualTo(EXISTING_ROWS);
    assertThat(result.deletedFilesCount()).isEqualTo(DELETED_FILES);
    assertThat(result.deletedRowsCount()).isEqualTo(DELETED_ROWS);
    assertThat(result.keyMetadata()).isNull();
  }

  @Test
  public void testRoundTripKeyMetadata() throws IOException {
    List<ManifestFile> manifests = writeAndRead(DATA_MANIFEST_WITH_KEY);

    assertThat(manifests).hasSize(1);
    ManifestFile result = manifests.get(0);

    assertThat(result.path()).isEqualTo(DATA_MANIFEST_PATH);
    assertThat(result.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  public void testRoundTripMultipleManifests() throws IOException {
    List<ManifestFile> manifests = writeAndRead(DATA_MANIFEST, DELETE_MANIFEST);

    assertThat(manifests).hasSize(2);

    ManifestFile data = manifests.get(0);
    assertThat(data.content()).isEqualTo(ManifestContent.DATA);
    assertThat(data.path()).isEqualTo(DATA_MANIFEST_PATH);

    ManifestFile deletes = manifests.get(1);
    assertThat(deletes.content()).isEqualTo(ManifestContent.DELETES);
    assertThat(deletes.path()).isEqualTo(DELETE_MANIFEST_PATH);
  }

  @Test
  public void testRoundTripReplacedAndModifiedCounts() throws IOException {
    GenericManifestFile manifest =
        new GenericManifestFile(
            DATA_MANIFEST_PATH,
            LENGTH,
            SPEC_ID,
            ManifestContent.DATA,
            SEQ_NUM,
            MIN_SEQ_NUM,
            SNAPSHOT_ID,
            null /* no partition summaries */,
            null /* no key metadata */,
            ADDED_FILES,
            ADDED_ROWS,
            EXISTING_FILES,
            EXISTING_ROWS,
            DELETED_FILES,
            DELETED_ROWS,
            null /* no firstRowId */);
    manifest.replacedFilesCount = 2;
    manifest.replacedRowsCount = 250L;

    List<ManifestFile> manifests = writeAndRead(manifest);

    assertThat(manifests).hasSize(1);
    ManifestFile result = manifests.get(0);
    assertThat(result.replacedFilesCount()).isEqualTo(2);
    assertThat(result.replacedRowsCount()).isEqualTo(250L);
  }

  @Test
  public void testRoundTripLegacyV3Manifest() throws IOException {
    // Simulate a v3 leaf manifest carried over in a v3-to-v4 upgrade.
    // format_version should be stored as 0 for these entries.
    OutputFile outputFile = new InMemoryOutputFile();
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            SNAPSHOT_FIRST_ROW_ID,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      // DATA_MANIFEST is a GenericManifestFile with the default formatVersion=0, which is
      // the legacy v3 leaf marker.
      writer.add(DATA_MANIFEST);
    }

    List<ManifestFile> manifests = RootManifests.read(outputFile.toInputFile());
    assertThat(manifests).hasSize(1);

    ManifestFile result = manifests.get(0);
    assertThat(result.content()).isEqualTo(ManifestContent.DATA);
    assertThat(result.path()).isEqualTo(DATA_MANIFEST_PATH);
    assertThat(result.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat(result.formatVersion())
        .as("legacy v3 leaf reference must round-trip format_version=0")
        .isEqualTo(0);
  }

  @Test
  public void testFormatVersionRoundTrip() throws IOException {
    // V4 leaf reference writes format_version=4; the reader exposes it on the reconstructed
    // ManifestFile via the new interface default method. The writer reads the value from the input
    // ManifestFile (set by V4Writer.toManifestFile in production); the test sets it directly via
    // the package-private setter.
    GenericManifestFile v4Manifest = (GenericManifestFile) DATA_MANIFEST.copy();
    v4Manifest.setFormatVersion(4);

    OutputFile outputFile = new InMemoryOutputFile();
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            SNAPSHOT_FIRST_ROW_ID,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      writer.add(v4Manifest);
    }

    List<ManifestFile> manifests = RootManifests.read(outputFile.toInputFile());
    assertThat(manifests).hasSize(1);
    assertThat(manifests.get(0).formatVersion())
        .as("v4 leaf reference must round-trip format_version=4")
        .isEqualTo(4);
  }

  @Test
  public void testFirstRowIdAssignmentForDataManifests() throws IOException {
    // Two DATA manifests added without a prior first-row-id. The writer's counter should assign
    // ascending values starting from the snapshot's first-row-id and advance by
    // (existingRowsCount + addedRowsCount) per manifest.
    OutputFile outputFile = new InMemoryOutputFile();
    long snapshotFirstRowId = 1000L;
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            snapshotFirstRowId,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      writer.add(DATA_MANIFEST); // assigned 1000, advances by EXISTING_ROWS + ADDED_ROWS
      writer.add(DATA_MANIFEST); // assigned 1000 + EXISTING_ROWS + ADDED_ROWS
    }

    List<ManifestFile> manifests = RootManifests.read(outputFile.toInputFile());
    assertThat(manifests).hasSize(2);
    assertThat(manifests.get(0).firstRowId()).isEqualTo(snapshotFirstRowId);
    assertThat(manifests.get(1).firstRowId())
        .isEqualTo(snapshotFirstRowId + EXISTING_ROWS + ADDED_ROWS);
  }

  @Test
  public void testFirstRowIdCarriedOverForDataManifest() throws IOException {
    // A DATA manifest with a prior first-row-id assigned should carry that value through (and
    // NOT advance the counter — so the next unassigned manifest still uses the snapshot's
    // first-row-id).
    long priorFirstRowId = 5000L;
    ManifestFile preassigned =
        new GenericManifestFile(
            DATA_MANIFEST_PATH,
            LENGTH,
            SPEC_ID,
            ManifestContent.DATA,
            SEQ_NUM,
            MIN_SEQ_NUM,
            SNAPSHOT_ID,
            null,
            null,
            ADDED_FILES,
            ADDED_ROWS,
            EXISTING_FILES,
            EXISTING_ROWS,
            DELETED_FILES,
            DELETED_ROWS,
            priorFirstRowId);

    OutputFile outputFile = new InMemoryOutputFile();
    long snapshotFirstRowId = 1000L;
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            snapshotFirstRowId,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      writer.add(preassigned); // carries over priorFirstRowId, counter not advanced
      writer.add(DATA_MANIFEST); // assigned snapshotFirstRowId (counter still at 1000)
    }

    List<ManifestFile> manifests = RootManifests.read(outputFile.toInputFile());
    assertThat(manifests.get(0).firstRowId()).isEqualTo(priorFirstRowId);
    assertThat(manifests.get(1).firstRowId()).isEqualTo(snapshotFirstRowId);
  }

  @Test
  public void testFirstRowIdNullForDeleteManifest() throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            1000L,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      writer.add(DELETE_MANIFEST);
    }

    List<ManifestFile> manifests = RootManifests.read(outputFile.toInputFile());
    assertThat(manifests).hasSize(1);
    assertThat(manifests.get(0).firstRowId())
        .as("delete manifests must not carry a first-row-id")
        .isNull();
  }

  private List<ManifestFile> writeAndRead(ManifestFile... manifestFiles) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            SNAPSHOT_FIRST_ROW_ID,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      for (ManifestFile manifestFile : manifestFiles) {
        writer.add(manifestFile);
      }
    }

    return RootManifests.read(outputFile.toInputFile());
  }
}
