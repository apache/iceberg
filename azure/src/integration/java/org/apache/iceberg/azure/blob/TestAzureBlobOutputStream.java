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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.azure.AzureTestUtils;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobOutputStream {
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
    containerName = TestAzureBlobOutputStream.class.getSimpleName().toLowerCase(Locale.ROOT);
    container = service.getBlobContainerClient(containerName);
    AzureBlobTestUtils.deleteAndCreateContainer(container);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    AzureBlobTestUtils.deleteContainerIfExists(container);
  }

  @Test
  public void testWrite() {
    final Random random = AzureTestUtils.random("testWrite");
    // Run tests for both byte and array write paths
    Stream.of(true, false).forEach(arrayWrite -> {
      // Test small file write
      writeAndVerify(
          AzureBlobTestUtils.randomAzureURI(storageAccount, containerName),
          AzureTestUtils.randomData(1024, random),
          arrayWrite);

      // Test large file
      writeAndVerify(
          AzureBlobTestUtils.randomAzureURI(storageAccount, containerName),
          AzureTestUtils.randomData(10 * 1024 * 1024, random),
          arrayWrite);
    });
  }

  @Test
  public void testMultipleClose() throws IOException {
    final AzureURI azureURI = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    final BlobClient blobClient = container.getBlobClient(azureURI.path());
    final AzureBlobOutputStream stream = new AzureBlobOutputStream(azureURI, azureProperties, blobClient);
    stream.close();
    stream.close();
  }

  @Test
  public void testBaseObjectWrite() throws IOException {
    final AzureURI azureURI = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    final BlobClient blobClient = container.getBlobClient(azureURI.path());
    try (PositionOutputStream stream = new AzureBlobOutputStream(azureURI, azureProperties, blobClient)) {
      // write 1 byte
      stream.write('1');
      // write 3 bytes
      stream.write("123".getBytes(StandardCharsets.UTF_8));
      // write 7 bytes, totally 11 bytes > local buffer limit (10)
      stream.write("1234567".getBytes(StandardCharsets.UTF_8));
      // write 11 bytes, flush remain 7 bytes and new 11 bytes
      stream.write("12345678901".getBytes(StandardCharsets.UTF_8));
    }

    final String expected = "1" + "123" + "1234567" + "12345678901";
    final String actual = blobClient.downloadContent().toString();
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testRewrite() throws IOException {
    final AzureURI azureURI = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    final BlobClient blobClient = container.getBlobClient(azureURI.path());
    try (PositionOutputStream stream = new AzureBlobOutputStream(azureURI, azureProperties, blobClient)) {
      // write 7 bytes
      stream.write("7654321".getBytes(StandardCharsets.UTF_8));
    }

    try (PositionOutputStream stream = new AzureBlobOutputStream(azureURI, azureProperties, blobClient)) {
      // write 14 bytes
      stream.write("1234567".getBytes(StandardCharsets.UTF_8));
      stream.write("1234567".getBytes(StandardCharsets.UTF_8));
    }

    final String expected = "1234567" + "1234567";
    final String actual = blobClient.downloadContent().toString();
    assertThat(actual).isEqualTo(expected);
  }

  private void writeAndVerify(AzureURI uri, byte[] data, boolean arrayWrite) {
    final BlobClient blobClient = container.getBlobClient(uri.path());
    try (PositionOutputStream stream = new AzureBlobOutputStream(uri, azureProperties, blobClient)) {
      if (arrayWrite) {
        stream.write(data);
        assertThat(stream.getPos()).isEqualTo(data.length);
      } else {
        for (int i = 0; i < data.length; i++) {
          stream.write(data[i]);
          assertThat(stream.getPos()).isEqualTo(i + 1);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    final byte[] actual = blobClient.downloadContent().toBytes();
    assertThat(actual).isEqualTo(data);
  }
}
