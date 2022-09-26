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

  private Iterator<FileScanTask> tasks;
  private CloseableIterator<T> currentIterator;
  private int fileOffset;
  private long recordOffset;

  public DataIterator(
      FileScanTaskReader<T> fileScanTaskReader,
      CombinedScanTask task,
      FileIO io,
      EncryptionManager encryption) {
    this.fileScanTaskReader = fileScanTaskReader;

    this.inputFilesDecryptor = new InputFilesDecryptor(task, io, encryption);
    this.combinedTask = task;

    this.tasks = task.files().iterator();
    this.currentIterator = CloseableIterator.empty();

    // fileOffset starts at -1 because we started
    // from an empty iterator that is not from the split files.
    this.fileOffset = -1;
    // record offset points to the record that next() should return when called
    this.recordOffset = 0L;
  }

  /**
   * (startingFileOffset, startingRecordOffset) points to the next row that reader should resume
   * from. E.g., if the seek position is (file=0, record=1), seek moves the iterator position to the
   * 2nd row in file 0. When next() is called after seek, 2nd row from file 0 should be returned.
   */
  public void seek(int startingFileOffset, long startingRecordOffset) {
    Preconditions.checkState(
        fileOffset == -1, "Seek should be called before any other iterator actions");
    // skip files
    Preconditions.checkState(
        startingFileOffset < combinedTask.files().size(),
        "Invalid starting file offset %s for combined scan task with %s files: %s",
        startingFileOffset,
        combinedTask.files().size(),
        combinedTask);
    for (long i = 0L; i < startingFileOffset; ++i) {
      tasks.next();
    }

    updateCurrentIterator();
    // skip records within the file
    for (long i = 0; i < startingRecordOffset; ++i) {
      if (currentFileHasNext() && hasNext()) {
        next();
      } else {
        throw new IllegalStateException(
            String.format(
                "Invalid starting record offset %d for file %d from CombinedScanTask: %s",
                startingRecordOffset, startingFileOffset, combinedTask));
      }
    }

    fileOffset = startingFileOffset;
    recordOffset = startingRecordOffset;
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    recordOffset += 1;
    return currentIterator.next();
  }

  public boolean currentFileHasNext() {
    return currentIterator.hasNext();
  }

  /** Updates the current iterator field to ensure that the current Iterator is not exhausted. */
  private void updateCurrentIterator() {
    try {
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(tasks.next());
        fileOffset += 1;
        recordOffset = 0L;
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
    tasks = null;
  }

  public int fileOffset() {
    return fileOffset;
  }

  public long recordOffset() {
    return recordOffset;
  }
}
