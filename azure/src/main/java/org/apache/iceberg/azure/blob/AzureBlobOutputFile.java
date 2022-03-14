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
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class AzureBlobOutputFile extends BaseAzureBlobFile implements OutputFile {

  public AzureBlobOutputFile(AzureURI azureURI, BlobClient blobClient, AzureProperties azureProperties) {
    super(azureURI, blobClient, azureProperties);
  }

  @Override
  public PositionOutputStream create() {
    if (!exists()) {
      return createOrOverwrite();
    } else {
      throw new AlreadyExistsException("Azure Blob already exists: %s", azureURI().location());
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    return new AzureBlobOutputStream(blobClient(), azureProperties());
  }

  @Override
  public InputFile toInputFile() {
    return AzureBlobInputFile.from(azureURI(), blobClient(), azureProperties());
  }

  public static AzureBlobOutputFile from(AzureURI azureURI, BlobClient blobClient, AzureProperties azureProperties) {
    return new AzureBlobOutputFile(azureURI, blobClient, azureProperties);
  }
}
