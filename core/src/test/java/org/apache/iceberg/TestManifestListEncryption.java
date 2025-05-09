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
import java.util.UUID;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestManifestListEncryption {
  private static final String PATH = "s3://bucket/table/m1.avro";
  private static final long LENGTH = 1024L;
  private static final int SPEC_ID = 1;
  private static final long SEQ_NUM = 34L;
  private static final long MIN_SEQ_NUM = 10L;
  private static final long SNAPSHOT_ID = 987134631982734L;
  private static final int ADDED_FILES = 2;
  private static final long ADDED_ROWS = 5292L;
  private static final int EXISTING_FILES = 343;
  private static final long EXISTING_ROWS = 857273L;
  private static final int DELETED_FILES = 1;
  private static final long DELETED_ROWS = 22910L;
  private static final long FIRST_ROW_ID = 100L;
  private static final long SNAPSHOT_FIRST_ROW_ID = 130L;

  private static final ByteBuffer FIRST_SUMMARY_LOWER_BOUND =
      Conversions.toByteBuffer(Types.IntegerType.get(), 10);
  private static final ByteBuffer FIRST_SUMMARY_UPPER_BOUND =
      Conversions.toByteBuffer(Types.IntegerType.get(), 100);
  private static final ByteBuffer SECOND_SUMMARY_LOWER_BOUND =
      Conversions.toByteBuffer(Types.IntegerType.get(), 20);
  private static final ByteBuffer SECOND_SUMMARY_UPPER_BOUND =
      Conversions.toByteBuffer(Types.IntegerType.get(), 200);

  private static final List<ManifestFile.PartitionFieldSummary> PARTITION_SUMMARIES =
      Lists.newArrayList(
          new GenericPartitionFieldSummary(
              false, FIRST_SUMMARY_LOWER_BOUND, FIRST_SUMMARY_UPPER_BOUND),
          new GenericPartitionFieldSummary(
              true, false, SECOND_SUMMARY_LOWER_BOUND, SECOND_SUMMARY_UPPER_BOUND));
  private static final ByteBuffer MANIFEST_KEY_METADATA = ByteBuffer.allocate(100);

  private static final ManifestFile TEST_MANIFEST =
      new GenericManifestFile(
          PATH,
          LENGTH,
          SPEC_ID,
          ManifestContent.DATA,
          SEQ_NUM,
          MIN_SEQ_NUM,
          SNAPSHOT_ID,
          PARTITION_SUMMARIES,
          MANIFEST_KEY_METADATA,
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          FIRST_ROW_ID);

  private static final EncryptionManager ENCRYPTION_MANAGER =
      EncryptionTestHelpers.createEncryptionManager();

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeAndReadEncryptedManifestList();

    assertThat(manifest.path()).isEqualTo(PATH);
    assertThat(manifest.length()).isEqualTo(LENGTH);
    assertThat(manifest.partitionSpecId()).isEqualTo(SPEC_ID);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);
    assertThat(manifest.sequenceNumber()).isEqualTo(SEQ_NUM);
    assertThat(manifest.minSequenceNumber()).isEqualTo(MIN_SEQ_NUM);
    assertThat((long) manifest.snapshotId()).isEqualTo(SNAPSHOT_ID);
    assertThat((int) manifest.addedFilesCount()).isEqualTo(ADDED_FILES);
    assertThat((long) manifest.addedRowsCount()).isEqualTo(ADDED_ROWS);
    assertThat((int) manifest.existingFilesCount()).isEqualTo(EXISTING_FILES);
    assertThat((long) manifest.existingRowsCount()).isEqualTo(EXISTING_ROWS);
    assertThat((int) manifest.deletedFilesCount()).isEqualTo(DELETED_FILES);
    assertThat((long) manifest.deletedRowsCount()).isEqualTo(DELETED_ROWS);
    assertThat(manifest.content()).isEqualTo(ManifestContent.DATA);
  }

  private ManifestFile writeAndReadEncryptedManifestList() throws IOException {
    FileIO io = new InMemoryFileIO();
    EncryptingFileIO encryptingFileIO = EncryptingFileIO.combine(io, ENCRYPTION_MANAGER);
    OutputFile outputFile = io.newOutputFile("memory:" + UUID.randomUUID());

    ManifestListWriter writer =
        ManifestLists.write(
            3,
            outputFile,
            encryptingFileIO.encryptionManager(),
            SNAPSHOT_ID,
            SNAPSHOT_ID - 1,
            SEQ_NUM,
            SNAPSHOT_FIRST_ROW_ID);
    writer.add(TEST_MANIFEST);
    writer.close();
    ManifestListFile manifestListFile = writer.toManifestListFile();

    // First try to read without decryption
    assertThatThrownBy(() -> ManifestLists.read(outputFile.toInputFile()))
        .isInstanceOf(RuntimeIOException.class)
        .hasMessageContaining("Failed to open file")
        .hasCauseInstanceOf(InvalidAvroMagicException.class);

    List<ManifestFile> manifests =
        ManifestLists.read(encryptingFileIO.newInputFile(manifestListFile));
    assertThat(manifests.size()).isEqualTo(1);

    return manifests.get(0);
  }
}
