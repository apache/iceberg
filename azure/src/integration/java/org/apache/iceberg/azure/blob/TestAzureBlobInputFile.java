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
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobInputFile {
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
    containerName = TestAzureBlobInputFile.class.getSimpleName().toLowerCase(Locale.ROOT);
    container = service.getBlobContainerClient(containerName);
    AzureBlobTestUtils.deleteAndCreateContainer(container);
  }

  @AfterClass
  public static void afterClass() {
    AzureBlobTestUtils.deleteContainerIfExists(container);
  }

  @Test
  public void testAbsentFile() {
    final AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    final BlobClient blobClient = container.getBlobClient(uri.path());
    final InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    assertThat(inputFile.exists()).isFalse();
  }

  @Test
  public void testFileRead() throws IOException {
    final AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    final BlobClient blobClient = container.getBlobClient(uri.path());
    final InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    final String expected = "0123456789";
    blobClient.upload(BinaryData.fromString(expected));

    assertThat(inputFile.exists()).isTrue();
    assertThat(inputFile.getLength()).isEqualTo(10);
    try (InputStream inputStream = inputFile.newStream()) {
      final String actual = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
      assertThat(actual).isEqualTo(expected);
    }
  }
}
