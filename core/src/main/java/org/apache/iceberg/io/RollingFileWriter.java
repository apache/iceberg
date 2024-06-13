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
import java.io.UncheckedIOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A rolling writer capable of splitting incoming data or deletes into multiple files within one
 * spec/partition based on the target file size.
 */
abstract class RollingFileWriter<T, W extends FileWriter<T, R>, R> implements FileWriter<T, R> {
  private static final int ROWS_DIVISOR = 1000;
  private static final Logger LOG = LoggerFactory.getLogger(RollingFileWriter.class);

  private final OutputFileFactory fileFactory;
  private final FileIO io;
  private final long targetFileSizeInBytes;
  private final PartitionSpec spec;
  private final StructLike partition;

  private EncryptedOutputFile currentFile = null;
  private long currentFileRows = 0;
  private W currentWriter = null;

  private boolean closed = false;

  protected RollingFileWriter(
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSizeInBytes,
      PartitionSpec spec,
      StructLike partition) {
    this.fileFactory = fileFactory;
    this.io = io;
    this.targetFileSizeInBytes = targetFileSizeInBytes;
    this.spec = spec;
    this.partition = partition;
  }

  protected abstract W newWriter(EncryptedOutputFile file);

  protected abstract void addResult(R result);

  protected abstract R aggregatedResult();

  protected PartitionSpec spec() {
    return spec;
  }

  protected StructLike partition() {
    return partition;
  }

  public CharSequence currentFilePath() {
    return currentFile.encryptingOutputFile().location();
  }

  public long currentFileRows() {
    return currentFileRows;
  }

  @Override
  public long length() {
    throw new UnsupportedOperationException(
        this.getClass().getName() + " does not implement length");
  }

  @Override
  public void write(T row) {
    currentWriter.write(row);
    currentFileRows++;

    if (shouldRollToNewFile()) {
      closeCurrentWriter();
      openCurrentWriter();
    }
  }

  private boolean shouldRollToNewFile() {
    return currentFileRows % ROWS_DIVISOR == 0 && currentWriter.length() >= targetFileSizeInBytes;
  }

  protected void openCurrentWriter() {
    Preconditions.checkState(currentWriter == null, "Current writer has been already initialized");

    this.currentFile = newFile();
    this.currentFileRows = 0;
    this.currentWriter = newWriter(currentFile);
  }

  private EncryptedOutputFile newFile() {
    if (spec.isUnpartitioned() || partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(spec, partition);
    }
  }

  private void closeCurrentWriter() {
    if (currentWriter != null) {
      try {
        currentWriter.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close current writer", e);
      }

      if (currentFileRows == 0L) {
        try {
          io.deleteFile(currentFile.encryptingOutputFile());
        } catch (Throwable t) {
          LOG.warn("Failed to delete empty file. This may result in empty orphan files.", t);
        }
      } else {
        addResult(currentWriter.result());
      }

      this.currentFile = null;
      this.currentFileRows = 0;
      this.currentWriter = null;
    }
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrentWriter();
      this.closed = true;
    }
  }

  @Override
  public final R result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return aggregatedResult();
  }
}
