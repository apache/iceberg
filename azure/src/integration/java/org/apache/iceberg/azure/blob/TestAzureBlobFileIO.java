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
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.azure.AuthType;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestAzureBlobFileIO {

  private static BlobServiceClient serviceClient;
  private static BlobContainerClient containerClient;
  private static Map<String, String> properties;

  @BeforeAll
  public static void beforeClass() {
    serviceClient = AzureBlobTestUtils.blobServiceClient(
        AzureBlobTestConstants.STORAGE_ACCOUNT_1,
        AzureBlobTestConstants.STORAGE_ACCOUNT_1_KEY);
    containerClient = serviceClient.getBlobContainerClient(AzureBlobTestConstants.DATA_CONTAINER);
    properties = ImmutableMap.<String, String>builder()
        .put(
            String.format(AzureProperties.STORAGE_AUTH_TYPE, AzureBlobTestConstants.STORAGE_ACCOUNT_1),
            AuthType.SharedKey.toString())
        .put(
            String.format(AzureProperties.STORAGE_ACCOUNT_KEY, AzureBlobTestConstants.STORAGE_ACCOUNT_1),
            AzureBlobTestConstants.STORAGE_ACCOUNT_1_KEY)
        .build();
  }

  @AfterAll
  public static void afterClass() {
    containerClient.delete();
  }

  @Test
  public void testSimpleReadWrite() throws IOException {
    FileIO io = new AzureBlobFileIO();
    io.initialize(properties);

    final String location = AzureBlobTestUtils.abfsLocation(
        AzureBlobTestConstants.STORAGE_ACCOUNT_1,
        AzureBlobTestConstants.DATA_CONTAINER,
        "/does/this/work/test.txt");

    OutputFile outputFile = io.newOutputFile(location);
    PositionOutputStream positionOutputStream = outputFile.create();
    String expectedData = "Hello World!";
    positionOutputStream.write(BinaryData.fromString(expectedData).toBytes());
    positionOutputStream.close();

    InputFile inputFile = outputFile.toInputFile();
    SeekableInputStream seekableInputStream = inputFile.newStream();
    String actualString = new String(ByteStreams.toByteArray(seekableInputStream));
    seekableInputStream.close();
    Assertions.assertThat(actualString).isEqualTo(expectedData);
  }
}
