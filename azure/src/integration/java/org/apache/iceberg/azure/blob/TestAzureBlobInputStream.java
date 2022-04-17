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
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

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
    Random random = AzureTestUtils.random("testRead");
    String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/read.dat");
    AzureURI uri = AzureURI.from(location);
    BlobClient blobClient = container.getBlobClient(uri.path());

    int dataSize = 1024 * 1024 * 10;
    byte[] data = AzureTestUtils.randomBytes(dataSize, random);
    writeAzureBlobData(blobClient, data);

    try (SeekableInputStream in = new AzureBlobInputStream(uri, azureProperties, blobClient)) {
      int readSize = 1024;

      readAndCheck(in, in.getPos(), readSize, data, false);
      readAndCheck(in, in.getPos(), readSize, data, true);

      // Seek forward in current stream
      int seekSize = 1024;
      readAndCheck(in, in.getPos() + seekSize, readSize, data, false);
      readAndCheck(in, in.getPos() + seekSize, readSize, data, true);

      // Buffered read
      readAndCheck(in, in.getPos(), readSize, data, true);
      readAndCheck(in, in.getPos(), readSize, data, false);

      // Seek with new stream
      long seekNewStreamPosition = 2 * 1024 * 1024;
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, true);
      readAndCheck(in, in.getPos() + seekNewStreamPosition, readSize, data, false);

      // Backseek and read
      readAndCheck(in, 0, readSize, data, true);
      readAndCheck(in, 0, readSize, data, false);
    }
  }

  @Test
  public void testSeek() throws Exception {
    Random random = AzureTestUtils.random("testSeek");
    String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/seek.dat");
    AzureURI uri = AzureURI.from(location);
    BlobClient blobClient = container.getBlobClient(uri.path());
    byte[] data = AzureTestUtils.randomBytes(1024 * 1024, random);

    writeAzureBlobData(blobClient, data);

    try (SeekableInputStream in = new AzureBlobInputStream(uri, azureProperties, blobClient)) {
      // In the first iteration, we'll be seeking forward since nothing is read yet
      // in the second iteration, we'll be seeking backward since the stream is fully read
      for (int i = 0; i < 2; i++) {
        in.seek(data.length / 2);
        byte[] actual = new byte[data.length / 2];
        IOUtils.readFully(in, actual, 0, data.length / 2);
        byte[] expected = Arrays.copyOfRange(data, data.length / 2, data.length);
        Assertions.assertThat(actual).isEqualTo(expected);
      }
    }
  }

  @Test
  public void testSeekDoesNotOpenInputStream() throws IOException {
    Random random = AzureTestUtils.random("testSeekOccursLazily");
    String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName,
        "/path/to/lazy-seek.dat");
    AzureURI uri = AzureURI.from(location);
    BlobClient blobClient = container.getBlobClient(uri.path());
    int streamLength = 1024;
    byte[] data = AzureTestUtils.randomBytes(streamLength, random);
    writeAzureBlobData(blobClient, data);

    BlobClient mockedBlobClient = Mockito.mock(BlobClient.class);
    // BlobInputStream cannot be mocked since it is final class, thus creating an actual stream to the blob.
    Mockito.when(mockedBlobClient.openInputStream(ArgumentMatchers.any())).thenReturn(blobClient.openInputStream());

    try (SeekableInputStream in = new AzureBlobInputStream(uri, azureProperties, mockedBlobClient)) {
      // seek forward
      for (long i = 0; i < streamLength; i++) {
        in.seek(i);
      }
      // seek backward
      for (long i = streamLength - 1L; i >= 0; i--) {
        in.seek(i);
      }
    }
    // Assert the InputStream is created only once in the constructor, and not every time we seek.
    Mockito.verify(mockedBlobClient, Mockito.times(1)).openInputStream(ArgumentMatchers.any());
  }

  @Test
  public void testClose() throws Exception {
    Random random = AzureTestUtils.random("testClose");
    String location = AzureBlobTestUtils.abfsLocation(storageAccount, containerName, "/path/to/closed.dat");
    AzureURI uri = AzureURI.from(location);
    BlobClient blobClient = container.getBlobClient(uri.path());
    writeAzureBlobData(blobClient, AzureTestUtils.randomBytes(1, random));
    SeekableInputStream closed = new AzureBlobInputStream(uri, azureProperties, blobClient);
    closed.close();
    Assertions.assertThatThrownBy(() -> closed.seek(0)).isInstanceOf(IllegalStateException.class);
  }

  private void readAndCheck(SeekableInputStream in, long rangeStart, int size, byte[] original, boolean buffered)
      throws IOException {
    in.seek(rangeStart);
    Assertions.assertThat(in.getPos()).isEqualTo(rangeStart);

    long rangeEnd = rangeStart + size;
    byte[] actual = new byte[size];

    if (buffered) {
      IOUtils.readFully(in, actual);
    } else {
      int read = 0;
      while (read < size) {
        actual[read++] = (byte) in.read();
      }
    }

    Assertions.assertThat(in.getPos()).isEqualTo(rangeEnd);
    Assertions.assertThat(actual).isEqualTo(Arrays.copyOfRange(original, (int) rangeStart, (int) rangeEnd));
  }

  private void writeAzureBlobData(BlobClient blobClient, byte[] data) {
    blobClient.upload(BinaryData.fromBytes(data));
  }
}
