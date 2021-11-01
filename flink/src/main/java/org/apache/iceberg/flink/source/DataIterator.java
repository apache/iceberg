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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Flink data iterator that reads {@link CombinedScanTask} into a {@link CloseableIterator}
 *
 * @param <T> is the output data type returned by this iterator.
 */
@Internal
public class DataIterator<T> implements CloseableIterator<T> {

  private final FileScanTaskReader<T> fileScanTaskReader;

  private final InputFilesDecryptor inputFilesDecryptor;
  private final CombinedScanTask combinedTask;
  private final Position position;

  private Iterator<FileScanTask> fileTasksIterator;
  private CloseableIterator<T> currentIterator;

  public DataIterator(FileScanTaskReader<T> fileScanTaskReader, CombinedScanTask task,
                      FileIO io, EncryptionManager encryption) {
    this.fileScanTaskReader = fileScanTaskReader;

    this.inputFilesDecryptor = new InputFilesDecryptor(task, io, encryption);
    this.combinedTask = task;
    // fileOffset starts at -1 because we started
    // from an empty iterator that is not from the split files.
    this.position = new Position(-1, 0L);

    this.fileTasksIterator = task.files().iterator();
    this.currentIterator = CloseableIterator.empty();
  }

  public void seek(Position startingPosition) {
    // skip files
    Preconditions.checkArgument(startingPosition.fileOffset() < combinedTask.files().size(),
        "Checkpointed file offset is %d, while CombinedScanTask has %d files",
        startingPosition.fileOffset(), combinedTask.files().size());
    for (long i = 0L; i < startingPosition.fileOffset(); ++i) {
      fileTasksIterator.next();
    }
    updateCurrentIterator();
    // skip records within the file
    for (long i = 0; i < startingPosition.recordOffset(); ++i) {
      if (hasNext()) {
        next();
      } else {
        throw new IllegalStateException("Not enough records to skip: " +
            startingPosition.recordOffset());
      }
    }
    this.position.update(startingPosition.fileOffset(), startingPosition.recordOffset());
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    position.advanceRecord();
    return currentIterator.next();
  }

  public boolean isCurrentIteratorDone() {
    return !currentIterator.hasNext();
  }

  /**
   * Updates the current iterator field to ensure that the current Iterator
   * is not exhausted.
   */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && fileTasksIterator.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(fileTasksIterator.next());
        position.advanceFile();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private CloseableIterator<T> openTaskIterator(FileScanTask scanTask) {
    return fileScanTaskReader.open(scanTask, inputFilesDecryptor);
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
    fileTasksIterator = null;
  }

  public Position position() {
    return position;
  }
}
