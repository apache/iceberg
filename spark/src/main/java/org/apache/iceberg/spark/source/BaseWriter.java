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

package org.apache.iceberg.spark.source;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.catalyst.InternalRow;

abstract class BaseWriter implements Closeable {
  protected static final int ROWS_DIVISOR = 1000;

  private final List<DataFile> completedFiles = Lists.newArrayList();
  private final PartitionSpec spec;
  private final FileFormat format;
  private final SparkAppenderFactory appenderFactory;
  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSize;
  private PartitionKey currentKey = null;
  private FileAppender<InternalRow> currentAppender = null;
  private EncryptedOutputFile currentFile = null;
  private long currentRows = 0;

  BaseWriter(PartitionSpec spec, FileFormat format, SparkAppenderFactory appenderFactory,
             OutputFileFactory fileFactory, FileIO io, long targetFileSize) {
    this.spec = spec;
    this.format = format;
    this.appenderFactory = appenderFactory;
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSize = targetFileSize;
  }

  public abstract void write(InternalRow row) throws IOException;

  public void writeInternal(InternalRow row)  throws IOException {
    //TODO: ORC file now not support target file size before closed
    if  (!format.equals(FileFormat.ORC) &&
        currentRows % ROWS_DIVISOR == 0 && currentAppender.length() >= targetFileSize) {
      closeCurrent();
      openCurrent();
    }

    currentAppender.add(row);
    currentRows++;
  }

  public TaskResult complete() throws IOException {
    closeCurrent();

    return new TaskResult(completedFiles);
  }

  public void abort() throws IOException {
    closeCurrent();

    // clean up files created by this writer
    Tasks.foreach(completedFiles)
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public void close() throws IOException {
    closeCurrent();
  }

  protected void openCurrent() {
    if (spec.fields().size() == 0) {
      // unpartitioned
      currentFile = fileFactory.newOutputFile();
    } else {
      // partitioned
      currentFile = fileFactory.newOutputFile(currentKey);
    }
    currentAppender = appenderFactory.newAppender(currentFile.encryptingOutputFile(), format);
    currentRows = 0;
  }

  protected void closeCurrent() throws IOException {
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
            .withPartition(spec.fields().size() == 0 ? null : currentKey) // set null if unpartitioned
            .withMetrics(metrics)
            .withSplitOffsets(splitOffsets)
            .build();
        completedFiles.add(dataFile);
      }

      this.currentFile = null;
    }
  }

  protected PartitionKey getCurrentKey() {
    return currentKey;
  }

  protected void setCurrentKey(PartitionKey currentKey) {
    this.currentKey = currentKey;
  }
}
