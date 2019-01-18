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

package com.netflix.iceberg;

import com.google.common.collect.ImmutableList;
import com.netflix.iceberg.encryption.EncryptionBuilders;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.util.ByteBuffers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestWriteEncryptionMetadata {

  private static final byte[] KEY_METADATA = "key-metadata".getBytes(StandardCharsets.UTF_8);
  private static final String KEY_ALGORITHM = "AES";

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testWriteEncryptionMetadata() throws IOException {
    File manifestFile = new File(temp.newFolder(), "manifest.avro");
    File tempDataFile = temp.newFile();
    java.nio.file.Files.write(tempDataFile.toPath(), "test".getBytes(StandardCharsets.UTF_8));
    DataFile dataFile = DataFiles.builder()
        .withInputFile(Files.localInput(tempDataFile))
        .withEncryption(EncryptionBuilders.encryptionKeyMetadataBuilder()
            .keyMetadata(KEY_METADATA)
            .cipherAlgorithm(TableProperties.DEFAULT_CIPHER_ALGORITHM)
            .keyAlgorithm(KEY_ALGORITHM)
            .build())
        .withFileSizeInBytes(tempDataFile.length())
        .withFormat(FileFormat.AVRO)
        .withRecordCount(1L)
        .build();
    try (ManifestWriter writer = new ManifestWriter(
        PartitionSpec.unpartitioned(), Files.localOutput(manifestFile), 0)) {
      writer.add(dataFile);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }

    List<DataFile> files;
    try (ManifestReader reader = ManifestReader.read(Files.localInput(manifestFile))) {
      files = ImmutableList.copyOf(reader.iterator());
    }
    Assert.assertEquals(
        "Only one file should be in the manifest.",
        1,
        files.size());
    DataFile writtenFile = files.iterator().next();
    Assert.assertNotNull(
        "Encryption metadata should have been written and read.", writtenFile.encryption());
    Assert.assertArrayEquals(
        "Encryption key metadata was incorrect.",
        KEY_METADATA,
        ByteBuffers.toByteArray(writtenFile.encryption().keyMetadata()));
    Assert.assertEquals(
        "Cipher algorithm was incorrect.",
        TableProperties.DEFAULT_CIPHER_ALGORITHM,
        writtenFile.encryption().cipherAlgorithm());
    Assert.assertEquals(
        "Key algorithm was incorrect.",
        KEY_ALGORITHM,
        writtenFile.encryption().keyAlgorithm());
  }
}
