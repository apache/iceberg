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
package org.apache.iceberg.azure.adlsv2;

import com.azure.storage.file.datalake.DataLakeFileClient;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.metrics.MetricsContext;

class ADLSInputFile extends BaseADLSFile implements InputFile {
  private Long fileSize;

  ADLSInputFile(
      String location,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    this(location, null, fileClient, azureProperties, metrics);
  }

  ADLSInputFile(
      String location,
      Long fileSize,
      DataLakeFileClient fileClient,
      AzureProperties azureProperties,
      MetricsContext metrics) {
    super(location, fileClient, azureProperties, metrics);
    this.fileSize = fileSize != null && fileSize > 0 ? fileSize : null;
  }

  @Override
  public long getLength() {
    if (fileSize == null) {
      this.fileSize = fileClient().getProperties().getFileSize();
    }
    return fileSize;
  }

  @Override
  public SeekableInputStream newStream() {
    return new ADLSInputStream(fileClient(), fileSize, azureProperties(), metrics());
  }
}
