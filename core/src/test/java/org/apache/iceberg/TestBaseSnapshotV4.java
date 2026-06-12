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
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

/** Tests for v4+ {@link BaseSnapshot} constructor behavior and cacheManifests dispatch. */
public class TestBaseSnapshotV4 {

  private static final long SNAPSHOT_ID = 987L;
  private static final long SEQ_NUM = 1L;
  private static final String MANIFEST_PATH = "file:/tmp/data-manifest.parquet";
  private static final String MANIFEST_LIST_PATH = "file:/tmp/snap-1.avro";
  private static final String ROOT_MANIFEST_PATH = "file:/tmp/snap-1-root.parquet";

  // Minimal schema + spec map used to drive the v4+ root manifest writer's union partition type
  // computation. Tests use an unpartitioned spec so the partition column is the placeholder.
  private static final Schema TABLE_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Map<Integer, PartitionSpec> SPECS_BY_ID =
      ImmutableMap.of(0, PartitionSpec.unpartitioned());

  @Test
  public void testV4ConstructionWithRootManifest() {
    BaseSnapshot snapshot =
        new BaseSnapshot(
            4,
            SEQ_NUM,
            SNAPSHOT_ID,
            null,
            System.currentTimeMillis(),
            DataOperations.APPEND,
            null,
            null,
            null,
            ROOT_MANIFEST_PATH,
            null,
            null,
            null);

    assertThat(snapshot.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(snapshot.rootManifestLocation()).isEqualTo(ROOT_MANIFEST_PATH);
    assertThat(snapshot.manifestListLocation()).isNull();
  }

  @Test
  public void testV2ConstructionWithManifestList() {
    BaseSnapshot snapshot =
        new BaseSnapshot(
            2,
            SEQ_NUM,
            SNAPSHOT_ID,
            null,
            System.currentTimeMillis(),
            DataOperations.APPEND,
            null,
            null,
            MANIFEST_LIST_PATH,
            null,
            null,
            null,
            null);

    assertThat(snapshot.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(snapshot.manifestListLocation()).isEqualTo(MANIFEST_LIST_PATH);
    assertThat(snapshot.rootManifestLocation()).isNull();
  }

  @Test
  public void testConstructionWithBothLocationsFailsOnNewConstructor() {
    assertThatThrownBy(
            () ->
                new BaseSnapshot(
                    4,
                    SEQ_NUM,
                    SNAPSHOT_ID,
                    null,
                    System.currentTimeMillis(),
                    DataOperations.APPEND,
                    null,
                    null,
                    MANIFEST_LIST_PATH,
                    ROOT_MANIFEST_PATH,
                    null,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly one")
        .hasMessageContaining("manifest-list")
        .hasMessageContaining("root-manifest");
  }

  @Test
  public void testConstructionWithNeitherLocationFailsOnNewConstructor() {
    assertThatThrownBy(
            () ->
                new BaseSnapshot(
                    4,
                    SEQ_NUM,
                    SNAPSHOT_ID,
                    null,
                    System.currentTimeMillis(),
                    DataOperations.APPEND,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("exactly one");
  }

  @Test
  public void testCacheManifestsV4UsesRootManifests() throws IOException {
    // Write a real root manifest to an in-memory file and verify cacheManifests reads it back.
    ManifestFile dataManifest =
        new GenericManifestFile(
            MANIFEST_PATH,
            4096L,
            0,
            ManifestContent.DATA,
            SEQ_NUM,
            SEQ_NUM,
            SNAPSHOT_ID,
            null,
            null,
            2,
            200L,
            0,
            0L,
            0,
            0L,
            null);

    OutputFile outputFile = new InMemoryOutputFile();
    try (RootManifestWriter writer =
        RootManifests.write(
            4,
            outputFile,
            PlaintextEncryptionManager.instance(),
            SNAPSHOT_ID,
            null,
            SEQ_NUM,
            null,
            TABLE_SCHEMA,
            SPECS_BY_ID)) {
      writer.add(dataManifest);
    }

    String rootManifestLocation = outputFile.location();
    byte[] rootManifestBytes;
    try (InputStream stream = outputFile.toInputFile().newStream()) {
      rootManifestBytes = stream.readAllBytes();
    }

    InMemoryFileIO fileIO = new InMemoryFileIO();
    fileIO.addFile(rootManifestLocation, rootManifestBytes);

    BaseSnapshot snapshot =
        new BaseSnapshot(
            4,
            SEQ_NUM,
            SNAPSHOT_ID,
            null,
            System.currentTimeMillis(),
            DataOperations.APPEND,
            null,
            null,
            null,
            rootManifestLocation,
            null,
            null,
            null);

    List<ManifestFile> manifests = snapshot.allManifests(fileIO);
    assertThat(manifests).hasSize(1);
    assertThat(manifests.get(0).path()).isEqualTo(MANIFEST_PATH);
    assertThat(manifests.get(0).content()).isEqualTo(ManifestContent.DATA);

    List<ManifestFile> dataManifests = snapshot.dataManifests(fileIO);
    assertThat(dataManifests).hasSize(1);
    assertThat(dataManifests.get(0).path()).isEqualTo(MANIFEST_PATH);

    List<ManifestFile> deleteManifests = snapshot.deleteManifests(fileIO);
    assertThat(deleteManifests).isEmpty();
  }

  @Test
  public void testV1LegacyConstructorHasNullBothLocations() {
    // The v1 constructor (embedded manifests) sets both locations to null, which is a special case.
    BaseSnapshot snapshot =
        new BaseSnapshot(
            SEQ_NUM,
            SNAPSHOT_ID,
            null,
            System.currentTimeMillis(),
            DataOperations.APPEND,
            null,
            null,
            new String[] {"/tmp/manifest1.avro"});

    assertThat(snapshot.manifestListLocation()).isNull();
    assertThat(snapshot.rootManifestLocation()).isNull();
  }
}
