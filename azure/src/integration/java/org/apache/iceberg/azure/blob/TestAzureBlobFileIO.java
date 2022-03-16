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

package org.apache.iceberg.azure.blob;

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.azure.AzureTestUtils;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobFileIO {

  private static BlobServiceClient service;
  private static String containerName;
  private static BlobContainerClient container;
  private static Map<String, String> properties;

  @BeforeClass
  public static void beforeClass() {
    properties = AzureBlobTestUtils.storageAccount1AuthProperties();
    service = AzureBlobTestUtils.storageAccount1BlobServiceClient();
    containerName = TestAzureBlobFileIO.class.getSimpleName().toLowerCase(Locale.ROOT);
    container = service.getBlobContainerClient(containerName);
    AzureBlobTestUtils.deleteAndCreateContainer(container);
  }

  @AfterClass
  public static void afterClass() {
    AzureBlobTestUtils.deleteContainerIfExists(container);
  }

  @Test
  public void testNewInputFile() throws IOException {
    final Random random = AzureTestUtils.random("testNewInputFile");
    final String location = AzureBlobTestUtils.abfsLocation(AzureBlobTestUtils.STORAGE_ACCOUNT_1,
        containerName,
        "/does/this/work/test.dat");
    int dataSize = 1024 * 1024;
    final byte[] expected = AzureTestUtils.randomData(dataSize, random);
    final FileIO io = new AzureBlobFileIO();
    io.initialize(properties);
    checkSimpleReadWriteWorks(location, expected, io);
  }

  @Test
  public void testDelete() {
    final String location =
        AzureBlobTestUtils.abfsLocation(AzureBlobTestUtils.STORAGE_ACCOUNT_1, containerName, "/delete/path/data.dat");
    final AzureURI uri = AzureURI.from(location);
    final BlobClient blobClient = container.getBlobClient(uri.path());
    blobClient.upload(BinaryData.fromBytes(new byte[] {1, 2, 3, 4, 5, 6}));
    assertThat(blobClient.exists()).isTrue();

    final FileIO io = new AzureBlobFileIO();
    io.initialize(properties);
    io.deleteFile(location);
    assertThat(blobClient.exists()).isFalse();
  }

  @Test
  public void testFileIoOnMultipleStorageAccount() throws IOException {
    final Random random = AzureTestUtils.random("testMultipleStorageAccountOps");
    final int dataSize = 1024 * 1024;

    final Map<String, String> combinedProperties = ImmutableMap.<String, String>builder()
        .putAll(AzureBlobTestUtils.storageAccount1AuthProperties())
        .putAll(AzureBlobTestUtils.storageAccount2AuthProperties())
        .putAll(AzureBlobTestUtils.storageAccount3AuthProperties())
        .build();
    final FileIO io = new AzureBlobFileIO();
    io.initialize(combinedProperties);

    final BlobServiceClient[] storageServices =
        {AzureBlobTestUtils.storageAccount1BlobServiceClient(), AzureBlobTestUtils.storageAccount2BlobServiceClient(),
         AzureBlobTestUtils.storageAccount3BlobServiceClient()};

    for (int i = 0; i < storageServices.length; i++) {
      final String testContainerName = String.format("container-for-storage-account-%s", i);
      final BlobContainerClient containerClient = storageServices[i].getBlobContainerClient(testContainerName);
      AzureBlobTestUtils.deleteAndCreateContainer(containerClient);

      try {
        final String location = AzureBlobTestUtils.abfsLocation(storageServices[i].getAccountName(),
            testContainerName,
            String.format("/location/for/storage-account-%s/data" + "-%s.dat", i, i));
        final byte[] expected = AzureTestUtils.randomData(dataSize, random);
        checkSimpleReadWriteWorks(location, expected, io);
      } finally {
        AzureBlobTestUtils.deleteContainerIfExists(containerClient);
      }
    }
  }

  private void checkSimpleReadWriteWorks(String location, byte[] expected, FileIO io) throws IOException {
    final int dataSize = expected.length;

    final InputFile in = io.newInputFile(location);
    assertThat(in.exists()).isFalse();

    final OutputFile out = io.newOutputFile(location);
    try (OutputStream os = out.createOrOverwrite()) {
      IOUtils.write(expected, os);
    }
    assertThat(in.exists()).isTrue();

    final byte[] actual = new byte[dataSize];
    try (InputStream is = in.newStream()) {
      IOUtils.readFully(is, actual);
    }
    assertThat(expected).isEqualTo(actual);

    io.deleteFile(in);
    assertThat(io.newInputFile(location).exists()).isFalse();
  }
}
