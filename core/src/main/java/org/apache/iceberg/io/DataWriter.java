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
package org.apache.iceberg.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class DataWriter<T> implements FileWriter<T, DataWriteResult> {
  private final FileAppender<T> appender;
  private final FileFormat format;
  private final String location;
  private final PartitionSpec spec;
  private final StructLike partition;
  private final ByteBuffer keyMetadata;
  private final SortOrder sortOrder;
  private DataFile dataFile = null;

  public DataWriter(
      FileAppender<T> appender,
      FileFormat format,
      String location,
      PartitionSpec spec,
      StructLike partition,
      EncryptionKeyMetadata keyMetadata) {
    this(appender, format, location, spec, partition, keyMetadata, null);
  }

  public DataWriter(
      FileAppender<T> appender,
      FileFormat format,
      String location,
      PartitionSpec spec,
      StructLike partition,
      EncryptionKeyMetadata keyMetadata,
      SortOrder sortOrder) {
    this.appender = appender;
    this.format = format;
    this.location = location;
    this.spec = spec;
    this.partition = partition;
    this.keyMetadata = keyMetadata != null ? keyMetadata.buffer() : null;
    this.sortOrder = sortOrder;
  }

  @Override
  public void write(T row) {
    appender.add(row);
  }

  @Override
  public long length() {
    return appender.length();
  }

  @Override
  public void close() throws IOException {
    if (dataFile == null) {
      appender.close();
      this.dataFile =
          DataFiles.builder(spec)
              .withFormat(format)
              .withPath(location)
              .withPartition(partition)
              .withEncryptionKeyMetadata(keyMetadata)
              .withFileSizeInBytes(appender.length())
              .withMetrics(appender.metrics())
              .withSplitOffsets(appender.splitOffsets())
              .withSortOrder(sortOrder)
              .build();
    }
  }

  public DataFile toDataFile() {
    Preconditions.checkState(dataFile != null, "Cannot create data file from unclosed writer");
    return dataFile;
  }

  @Override
  public DataWriteResult result() {
    return new DataWriteResult(toDataFile());
  }
}
