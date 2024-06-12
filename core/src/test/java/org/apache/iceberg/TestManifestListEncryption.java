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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

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

  private static final ByteBuffer firstSummaryLowerBound =
      Conversions.toByteBuffer(Types.IntegerType.get(), 10);
  private static final ByteBuffer firstSummaryUpperBound =
      Conversions.toByteBuffer(Types.IntegerType.get(), 100);
  private static final ByteBuffer secondSummaryLowerBound =
      Conversions.toByteBuffer(Types.IntegerType.get(), 20);
  private static final ByteBuffer secondSummaryUpperBound =
      Conversions.toByteBuffer(Types.IntegerType.get(), 200);

  private static final List<ManifestFile.PartitionFieldSummary> PARTITION_SUMMARIES =
      Lists.newArrayList(
          new GenericPartitionFieldSummary(false, firstSummaryLowerBound, firstSummaryUpperBound),
          new GenericPartitionFieldSummary(
              true, false, secondSummaryLowerBound, secondSummaryUpperBound));
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
          ADDED_FILES,
          ADDED_ROWS,
          EXISTING_FILES,
          EXISTING_ROWS,
          DELETED_FILES,
          DELETED_ROWS,
          PARTITION_SUMMARIES,
          MANIFEST_KEY_METADATA);

  private static final EncryptionManager ENCRYPTION_MANAGER =
      EncryptionTestHelpers.createEncryptionManager();

  @Test
  public void testV2Write() throws IOException {
    ManifestFile manifest = writeAndReadEncryptedManifestList();

    // all v2 fields should be read correctly
    Assert.assertEquals("Path", PATH, manifest.path());
    Assert.assertEquals("Length", LENGTH, manifest.length());
    Assert.assertEquals("Spec id", SPEC_ID, manifest.partitionSpecId());
    Assert.assertEquals("Content", ManifestContent.DATA, manifest.content());
    Assert.assertEquals("Sequence number", SEQ_NUM, manifest.sequenceNumber());
    Assert.assertEquals("Min sequence number", MIN_SEQ_NUM, manifest.minSequenceNumber());
    Assert.assertEquals("Snapshot id", SNAPSHOT_ID, (long) manifest.snapshotId());
    Assert.assertEquals("Added files count", ADDED_FILES, (int) manifest.addedFilesCount());
    Assert.assertEquals("Added rows count", ADDED_ROWS, (long) manifest.addedRowsCount());
    Assert.assertEquals(
        "Existing files count", EXISTING_FILES, (int) manifest.existingFilesCount());
    Assert.assertEquals("Existing rows count", EXISTING_ROWS, (long) manifest.existingRowsCount());
    Assert.assertEquals("Deleted files count", DELETED_FILES, (int) manifest.deletedFilesCount());
    Assert.assertEquals("Deleted rows count", DELETED_ROWS, (long) manifest.deletedRowsCount());
  }

  private ManifestFile writeAndReadEncryptedManifestList() throws IOException {
    OutputFile rawOutput = new InMemoryOutputFile();
    EncryptedOutputFile encryptedOutput = ENCRYPTION_MANAGER.encrypt(rawOutput);
    EncryptionKeyMetadata keyMetadata = encryptedOutput.keyMetadata();

    try (FileAppender<ManifestFile> writer =
        ManifestLists.write(
            2, encryptedOutput.encryptingOutputFile(), SNAPSHOT_ID, SNAPSHOT_ID - 1, SEQ_NUM)) {
      writer.add(TEST_MANIFEST);
    }

    InputFile rawInput = rawOutput.toInputFile();

    // First try to read without decryption
    Assertions.assertThatThrownBy(() -> ManifestLists.read(rawInput))
        .isInstanceOf(RuntimeIOException.class)
        .hasMessageContaining("Failed to open file")
        .hasCauseInstanceOf(InvalidAvroMagicException.class);

    EncryptedInputFile encryptedManifestListInput =
        EncryptedFiles.encryptedInput(rawInput, keyMetadata);
    InputFile manifestListInput = ENCRYPTION_MANAGER.decrypt(encryptedManifestListInput);

    List<ManifestFile> manifests = ManifestLists.read(manifestListInput);
    Assert.assertEquals("Should contain one manifest", 1, manifests.size());
    return manifests.get(0);
  }
}
