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
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FileIO implementation backed by Azure Blob Storage.
 */
// TODO check all the Interfaces and their contracts
// TODO add logging traces
// TODO end all assertion error message with full stop
// TODO enable encryption
public class AzureBlobFileIO implements FileIO {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobFileIO.class);

  private AzureProperties azureProperties;

  /**
   * No-arg constructor to load the FileIO dynamically.
   * <p>
   * All fields are initialized by calling {@link AzureBlobFileIO#initialize(Map)} later.
   */
  public AzureBlobFileIO() {
  }

  @Override
  public InputFile newInputFile(String path) {
    final AzureURI azureURI = AzureURI.from(path);
    return AzureBlobInputFile.from(azureURI, client(azureURI), azureProperties);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    final AzureURI azureURI = AzureURI.from(path);
    return AzureBlobOutputFile.from(azureURI, client(azureURI), azureProperties);
  }

  @Override
  public void deleteFile(String path) {
    client(AzureURI.from(path)).delete();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.azureProperties = new AzureProperties(properties);
  }

  private BlobClient client(AzureURI azureURI) {
    return AzureBlobClientFactory.createBlobClient(azureURI, azureProperties);
  }
}
