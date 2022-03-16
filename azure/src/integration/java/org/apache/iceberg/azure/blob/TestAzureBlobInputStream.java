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
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import org.apache.commons.io.IOUtils;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.AzureTestUtils;
import org.apache.iceberg.io.SeekableInputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAzureBlobInputStream {
  private static AzureProperties azureProperties;
  private static String storageAccount;
  private static BlobServiceClient service;
  private static String containerName;
  private static BlobContainerClient container;

  @BeforeClass
  public static void beforeClass() {
    azureProperties = new AzureProperties(AzureBlobTestUtils.storageAccount1AuthProperties());
    storageAccount = AzureBlobTestUtils.STORAGE_ACCOUNT_1;
    service = AzureBlobTestUtils.storageAccount1BlobServiceClient();
    containerName = TestAzureBlobInputStream.class.getSimpleName().toLowerCase(Locale.ROOT);
    container = service.getBlobContainerClient(containerName);
    AzureBlobTestUtils.deleteAndCreateContainer(container);
  }

  @AfterClass
  public static void afterClass() {
    AzureBlobTestUtils.deleteContainerIfExists(container);
  }

  @Test
  public void testRead() throws Exception {
    final Random random = AzureTestUtils.random("testRead");
    final String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/read.dat");
    final AzureURI uri = AzureURI.from(location);
    final BlobClient blobClient = container.getBlobClient(uri.path());

    final int dataSize = 1024 * 1024 * 10;
    final byte[] data = AzureTestUtils.randomData(dataSize, random);
    writeAzureBlobData(blobClient, data);

    try (SeekableInputStream in = new AzureBlobInputStream(uri, azureProperties, blobClient)) {
      final int readSize = 1024;

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      final int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      final long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  @Test
  public void testSeek() throws Exception {
    final Random random = AzureTestUtils.random("testSeek");
    final String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/seek.dat");
    final AzureURI uri = AzureURI.from(location);
    final BlobClient blobClient = container.getBlobClient(uri.path());
    final byte[] data = AzureTestUtils.randomData(1024 * 1024, random);

    writeAzureBlobData(blobClient, data);

    try (SeekableInputStream in = new AzureBlobInputStream(uri, azureProperties, blobClient)) {
      in.seek(data.length / 2);
      final byte[] actual = new byte[data.length / 2];

      IOUtils.readFully(in, actual, 0, data.length / 2);

      final byte[] expected = Arrays.copyOfRange(data, data.length / 2, data.length);
      assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void testClose() throws Exception {
    final Random random = AzureTestUtils.random("testClose");
    final String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/closed.dat");
    final AzureURI uri = AzureURI.from(location);
    final BlobClient blobClient = container.getBlobClient(uri.path());
    writeAzureBlobData(blobClient, AzureTestUtils.randomData(1, random));
    SeekableInputStream closed = new AzureBlobInputStream(uri, azureProperties, blobClient);
    closed.close();
    assertThatThrownBy(() -> closed.seek(0)).isInstanceOf(IllegalStateException.class);
  }

  private void readAndCheck(SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    assertThat(in.getPos()).isEqualTo(rangeStart);

    final long rangeEnd = rangeStart + size;
    final byte[] actual = new byte[size];

    if (buffered) {
      IOUtils.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    assertThat(in.getPos()).isEqualTo(rangeEnd);
    assertThat(actual).isEqualTo(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd));
  }

  private void writeAzureBlobData(BlobClient blobClient, byte[] data) {
    blobClient.upload(BinaryData.fromBytes(data));
  }
}
