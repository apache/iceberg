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
import com.azure.storage.blob.models.BlobErrorCode;
import com.azure.storage.blob.models.BlobStorageException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
    AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    BlobClient blobClient = container.getBlobClient(uri.path());
    InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    Assertions.assertThat(inputFile.exists()).isFalse();
  }

  @Test
  public void testFileRead() throws IOException {
    AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    BlobClient blobClient = container.getBlobClient(uri.path());
    InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    String expected = "0123456789";
    blobClient.upload(BinaryData.fromString(expected));

    Assertions.assertThat(inputFile.exists()).isTrue();
    Assertions.assertThat(inputFile.getLength()).isEqualTo(10);
    try (InputStream inputStream = inputFile.newStream()) {
      String actual = new String(ByteStreams.toByteArray(inputStream), StandardCharsets.UTF_8);
      Assertions.assertThat(actual).isEqualTo(expected);
    }
  }

  @Test
  public void testThrowsNotFoundExceptionWhenBlobDoesNotExist() {
    AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    BlobClient blobClient = container.getBlobClient(uri.path());
    InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    Assertions.assertThatThrownBy(inputFile::getLength)
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(String.format("File does not exists: %s", uri.location()));
    Assertions.assertThatThrownBy(inputFile::newStream)
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining(String.format("File does not exists: %s", uri.location()));
  }

  @Test
  public void testThrowsNotFoundExceptionWhenContainerDoesNotExist() {
    String nonExistentContainer = "nonExistentContainer";
    AzureURI uri = AzureBlobTestUtils.randomAzureURI(storageAccount, nonExistentContainer);
    BlobContainerClient containerClient = service.getBlobContainerClient(nonExistentContainer);
    BlobClient blobClient = containerClient.getBlobClient(uri.path());
    InputFile inputFile = AzureBlobInputFile.from(uri, blobClient, azureProperties);
    Assertions.setMaxStackTraceElementsDisplayed(400);
    Assertions.assertThatThrownBy(inputFile::getLength)
        .isInstanceOf(BlobStorageException.class)
        .hasMessageContaining(BlobErrorCode.INVALID_RESOURCE_NAME.toString());
    Assertions.assertThatThrownBy(inputFile::newStream)
        .isInstanceOf(BlobStorageException.class)
        .hasMessageContaining(BlobErrorCode.INVALID_RESOURCE_NAME.toString());
  }
}
