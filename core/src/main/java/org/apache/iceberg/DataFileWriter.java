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

package org.apache.iceberg;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class DataFileWriter<T> implements ContentFileWriter<DataFile, T> {
  private final FileAppender<T> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionKey partitionKey;
  private final PartitionSpec spec;
  private final ByteBuffer keyMetadata;
  private DataFile dataFile = null;

  public DataFileWriter(FileAppender<T> appender, FileFormat format,
                        String location, PartitionKey partitionKey, PartitionSpec spec,
                        EncryptionKeyMetadata keyMetadata) {
    this.appender = appender;
    this.format = format;
    this.location = location;
    this.partitionKey = partitionKey; // set null if unpartitioned.
    this.spec = spec;
    this.keyMetadata = keyMetadata != null ? keyMetadata.buffer() : null;
  }

  @Override
  public void write(T row) {
    appender.add(row);
  }

  @Override
  public Metrics metrics() {
    return appender.metrics();
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public DataFile toContentFile() {
    Preconditions.checkState(dataFile != null, "Cannot create data file from unclosed writer");
    return dataFile;
  }

  @Override
  public void close() throws IOException {
    if (dataFile == null) {
      appender.close();
      this.dataFile = DataFiles.builder(spec)
          .withEncryptionKeyMetadata(keyMetadata)
          .withFormat(format)
          .withPath(location)
          .withFileSizeInBytes(appender.length())
          .withPartition(partitionKey) // set null if unpartitioned
          .withMetrics(appender.metrics())
          .withSplitOffsets(appender.splitOffsets())
          .build();
    }
  }
}
