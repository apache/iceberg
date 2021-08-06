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
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A rolling writer capable of splitting incoming data/deletes into multiple files within one spec/partition.
 */
abstract class RollingFileWriter<T, W extends FileWriter<T, R>, R> implements FileWriter<T, R> {
  private static final int ROWS_DIVISOR = 1000;

  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final FileFormat fileFormat;
  private final long targetFileSizeInBytes;
  private final PartitionSpec spec;
  private final StructLike partition;

  private EncryptedOutputFile currentFile = null;
  private W currentWriter = null;
  private long currentRows = 0;

  private boolean closed = false;

  protected RollingFileWriter(OutputFileFactory fileFactory, FileIO io, FileFormat fileFormat,
                              long targetFileSizeInBytes, PartitionSpec spec, StructLike partition) {
    this.fileFactory = fileFactory;
    this.io = io;
    this.fileFormat = fileFormat;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
    this.spec = spec;
    this.partition = partition;
  }

  protected abstract W newWriter(EncryptedOutputFile file);

  protected abstract long length(W writer);

  protected abstract void addResult(R result);

  protected abstract R aggregatedResult();

  protected PartitionSpec spec() {
    return spec;
  }

  protected StructLike partition() {
    return partition;
  }

  @Override
  public void write(T row) throws IOException {
    currentWriter.write(row);
    currentRows++;

    if (shouldRollToNewFile()) {
      closeCurrent();
      openCurrent();
    }
  }

  public CharSequence currentPath() {
    Preconditions.checkNotNull(currentFile, "The currentFile shouldn't be null");
    return currentFile.encryptingOutputFile().location();
  }

  public long currentRows() {
    return currentRows;
  }

  protected void openCurrent() {
    if (partition == null) {
      this.currentFile = fileFactory.newOutputFile();
    } else {
      this.currentFile = fileFactory.newOutputFile(partition);
    }
    this.currentWriter = newWriter(currentFile);
    this.currentRows = 0;
  }

  private boolean shouldRollToNewFile() {
    // TODO: ORC file now not support target file size before closed
    return !fileFormat.equals(FileFormat.ORC) &&
        currentRows % ROWS_DIVISOR == 0 && length(currentWriter) >= targetFileSizeInBytes;
  }

  private void closeCurrent() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();

      R result = currentWriter.result();

      if (currentRows == 0L) {
        io.deleteFile(currentFile.encryptingOutputFile());
      } else {
        addResult(result);
      }

      this.currentFile = null;
      this.currentWriter = null;
      this.currentRows = 0;
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrent();
      this.closed = true;
    }
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }
}
