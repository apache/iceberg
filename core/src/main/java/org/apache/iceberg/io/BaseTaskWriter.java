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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;

public abstract class BaseTaskWriter<T> implements TaskWriter<T> {
  private final List<DataFile> completedFiles = Lists.newArrayList();
  private final PartitionSpec spec;
  private final FileFormat format;
  private final FileAppenderFactory<T> appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;

  protected BaseTaskWriter(PartitionSpec spec, FileFormat format, FileAppenderFactory<T> appenderFactory,
                           OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    Tasks.foreach(completedFiles)
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public DataFile[] complete() throws IOException {
    close();

    return completedFiles.toArray(new DataFile[0]);
  }

  protected class RollingFileWriter implements Closeable {
    private static final int ROWS_DIVISOR = 1000;
    private final PartitionKey partitionKey;

    private EncryptedOutputFile currentFile = null;
    private FileAppender<T> currentAppender = null;
    private long currentRows = 0;

    public RollingFileWriter(PartitionKey partitionKey) {
      this.partitionKey = partitionKey;
      openCurrent();
    }

    public void add(T record) throws IOException {
      this.currentAppender.add(record);
      this.currentRows++;

      if (shouldRollToNewFile()) {
        closeCurrent();
        openCurrent();
      }
    }

    private void openCurrent() {
      if (partitionKey == null) {
        // unpartitioned
        currentFile = fileFactory.newOutputFile();
      } else {
        // partitioned
        currentFile = fileFactory.newOutputFile(partitionKey);
      }
      currentAppender = appenderFactory.newAppender(currentFile.encryptingOutputFile(), format);
      currentRows = 0;
    }

    private boolean shouldRollToNewFile() {
      //TODO: ORC file now not support target file size before closed
      return !format.equals(FileFormat.ORC) &&
          currentRows % ROWS_DIVISOR == 0 && currentAppender.length() >= targetFileSize;
    }

    private void closeCurrent() throws IOException {
      if (currentAppender != null) {
        currentAppender.close();
        // metrics are only valid after the appender is closed
        Metrics metrics = currentAppender.metrics();
        long fileSizeInBytes = currentAppender.length();
        List<Long> splitOffsets = currentAppender.splitOffsets();
        this.currentAppender = null;

        if (metrics.recordCount() == 0L) {
          io.deleteFile(currentFile.encryptingOutputFile());
        } else {
          DataFile dataFile = DataFiles.builder(spec)
              .withEncryptionKeyMetadata(currentFile.keyMetadata())
              .withPath(currentFile.encryptingOutputFile().location())
              .withFileSizeInBytes(fileSizeInBytes)
              .withPartition(spec.fields().size() == 0 ? null : partitionKey) // set null if unpartitioned
              .withMetrics(metrics)
              .withSplitOffsets(splitOffsets)
              .build();
          completedFiles.add(dataFile);
        }

        this.currentFile = null;
        this.currentRows = 0;
      }
    }

    @Override
    public void close() throws IOException {
      closeCurrent();
    }
  }
}
