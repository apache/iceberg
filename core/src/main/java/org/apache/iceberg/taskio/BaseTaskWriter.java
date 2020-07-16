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

package org.apache.iceberg.taskio;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  protected static final int ROWS_DIVISOR = 1000;

  private final List<DataFile> completedFiles = Lists.newArrayList();
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;

  BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                 OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  public List<DataFile> pollCompleteFiles() {
    if (completedFiles.size() > 0) {
      List<DataFile> dataFiles = ImmutableList.copyOf(completedFiles);
      completedFiles.clear();
      return dataFiles;
    } else {
      return Collections.emptyList();
    }
  }

  protected FileIO io() {
    return this.io;
  }

  protected OutputFileFactory outputFileFactory() {
    return this.fileFactory;
  }

  WrappedFileAppender createWrappedFileAppender(PartitionKey partitionKey,
                                                Supplier<EncryptedOutputFile> outputFileSupplier) {
    EncryptedOutputFile outputFile = outputFileSupplier.get();
    FileAppender<T> appender = appenderFactory.newAppender(outputFile.encryptingOutputFile(), format);
    return new WrappedFileAppender(partitionKey, outputFile, appender);
  }

  void closeWrappedFileAppender(WrappedFileAppender appender) throws IOException {
    DataFile dataFile = appender.closeAndBuildDataFile();
    completedFiles.add(dataFile);
  }

  class WrappedFileAppender {
    private final PartitionKey partitionKey;
    private final EncryptedOutputFile encryptedOutputFile;
    private final FileAppender<T> appender;
    private boolean closed = false;
    private long currentRows = 0;

    WrappedFileAppender(PartitionKey partitionKey, EncryptedOutputFile encryptedOutputFile, FileAppender<T> appender) {
      this.partitionKey = partitionKey;
      this.encryptedOutputFile = encryptedOutputFile;
      this.appender = appender;
    }

    void add(T record) {
      this.appender.add(record);
      this.currentRows++;
    }

    boolean shouldRollToNewFile() {
      //TODO: ORC file now not support target file size before closed
      return !format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && appender.length() >= targetFileSize;
    }

    private void close() throws IOException {
      if (!closed) {
        appender.close();
        closed = true;
      }
    }

    private DataFile closeAndBuildDataFile() throws IOException {
      // Close the file appender firstly.
      this.close();

      // metrics are only valid after the appender is closed.
      Metrics metrics = appender.metrics();
      long fileSizeInBytes = appender.length();
      List<Long> splitOffsets = appender.splitOffsets();

      return DataFiles.builder(spec)
          .withEncryptedOutputFile(encryptedOutputFile)
          .withFileSizeInBytes(fileSizeInBytes)
          .withPartition(partitionKey)
          .withMetrics(metrics)
          .withSplitOffsets(splitOffsets)
          .build();
    }
  }
}
