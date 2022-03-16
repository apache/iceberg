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
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureBlobOutputFile {

  private AzureURI uri;
  private BlobClient blobClient;
  private OutputFile outputFile;

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
    containerName = TestAzureBlobOutputFile.class.getSimpleName().toLowerCase(Locale.ROOT);
    container = service.getBlobContainerClient(containerName);
    AzureBlobTestUtils.deleteAndCreateContainer(container);
  }

  @AfterClass
  public static void afterClass() {
    AzureBlobTestUtils.deleteContainerIfExists(container);
  }

  @Before
  public void before() {
    uri = AzureBlobTestUtils.randomAzureURI(storageAccount, containerName);
    blobClient = container.getBlobClient(uri.path());
    outputFile = AzureBlobOutputFile.from(uri, blobClient, azureProperties);
  }

  @Test
  public void testFileWrite() throws IOException {
    // File write
    try (PositionOutputStream output = outputFile.create()) {
      output.write("1234567890".getBytes(StandardCharsets.UTF_8));
    }
    final String actual = blobClient.downloadContent().toString();
    assertThat(actual).isEqualTo("1234567890");
  }

  @Test
  public void testFileOverwrite() throws IOException {
    try (PositionOutputStream output = outputFile.create()) {
      output.write("1234567890".getBytes(StandardCharsets.UTF_8));
    }

    try (PositionOutputStream output = outputFile.createOrOverwrite()) {
      output.write("abcdefghij".getBytes(StandardCharsets.UTF_8));
    }
    final String actual = blobClient.downloadContent().toString();
    assertThat(actual).isEqualTo("abcdefghij");
  }

  @Test
  public void testFileAlreadyExists() throws IOException {
    try (PositionOutputStream output = outputFile.create()) {
      output.write("1234567890".getBytes(StandardCharsets.UTF_8));
    }
    AssertHelpers.assertThrows("Create should throw exception",
        AlreadyExistsException.class,
        outputFile.location(),
        outputFile::create);
  }
}
