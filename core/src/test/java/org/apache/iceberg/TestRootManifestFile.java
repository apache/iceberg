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

import java.nio.ByteBuffer;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;
import org.junit.jupiter.api.Test;

class TestRootManifestFile {
  private static final long SNAPSHOT_ID = 42L;
  private static final byte[] CONTENTS = new byte[] {1, 2, 3, 4, 5};
  private static final String LOCATION = "s3://bucket/db/table/metadata/root.avro";

  private final InputFile file = new InMemoryInputFile(LOCATION, CONTENTS);

  @Test
  void exposesLocationLengthAndSnapshotId() {
    RootManifestFile root = new RootManifestFile(file, SNAPSHOT_ID, /* keyMetadata= */ null);

    assertThat(root.path()).isEqualTo(LOCATION);
    assertThat(root.length()).isEqualTo(CONTENTS.length);
    assertThat(root.snapshotId()).isEqualTo(SNAPSHOT_ID);
  }

  @Test
  void readableAsAV4DataManifest() {
    RootManifestFile root = new RootManifestFile(file, SNAPSHOT_ID, null);

    assertThat(root.content()).isEqualTo(ManifestContent.DATA);
    assertThat(root.formatVersion()).isEqualTo(4);
  }

  @Test
  void keyMetadataRoundTrips() {
    assertThat(new RootManifestFile(file, SNAPSHOT_ID, null).keyMetadata()).isNull();

    ByteBuffer keyMetadata = ByteBuffer.wrap(new byte[] {9, 8, 7});
    assertThat(new RootManifestFile(file, SNAPSHOT_ID, keyMetadata).keyMetadata())
        .isEqualTo(keyMetadata);
  }

  @Test
  void copyPreservesValues() {
    ByteBuffer keyMetadata = ByteBuffer.wrap(new byte[] {9, 8, 7});
    ManifestFile copy = new RootManifestFile(file, SNAPSHOT_ID, keyMetadata).copy();

    assertThat(copy.path()).isEqualTo(LOCATION);
    assertThat(copy.length()).isEqualTo(CONTENTS.length);
    assertThat(copy.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat(copy.keyMetadata()).isEqualTo(keyMetadata);
  }

  @Test
  void unsupportedAccessorsThrow() {
    RootManifestFile root = new RootManifestFile(file, SNAPSHOT_ID, null);

    assertThatThrownBy(root::partitionSpecId)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no partition spec");
    assertThatThrownBy(root::sequenceNumber)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no sequence number");
    assertThatThrownBy(root::minSequenceNumber)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no minimum sequence number");
    assertThatThrownBy(root::addedFilesCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no added files count");
    assertThatThrownBy(root::addedRowsCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no added rows count");
    assertThatThrownBy(root::existingFilesCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no existing files count");
    assertThatThrownBy(root::existingRowsCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no existing rows count");
    assertThatThrownBy(root::deletedFilesCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no deleted files count");
    assertThatThrownBy(root::deletedRowsCount)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no deleted rows count");
    assertThatThrownBy(root::partitions)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Root manifest has no partition summaries");
  }
}
