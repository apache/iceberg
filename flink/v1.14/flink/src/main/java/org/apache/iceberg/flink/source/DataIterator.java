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
import java.util.Optional;

import org.apache.flink.annotation.Internal;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.InputFilesDecryptor;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;

/**
 * Flink data iterator that reads {@link CombinedScanTask} into a {@link CloseableIterator}
 *
 * @param <T> is the output data type returned by this iterator.
 */
@Internal
public class DataIterator<T> implements CloseableIterator<T> {

  private final FileScanTaskReader<T> fileScanTaskReader;

  private final InputFilesDecryptor inputFilesDecryptor;
  private Iterator<FileScanTask> tasks;
  private CloseableIterator<T> currentIterator;
  private CloseableIterator<T> currentDeleteIterator;

  public DataIterator(FileScanTaskReader<T> fileScanTaskReader, CombinedScanTask task,
                      FileIO io, EncryptionManager encryption) {
    this.fileScanTaskReader = fileScanTaskReader;

    this.inputFilesDecryptor = new InputFilesDecryptor(task, io, encryption);
    this.tasks = task.files().iterator();
    this.currentIterator = CloseableIterator.empty();
    this.currentDeleteIterator = CloseableIterator.empty();
  }

  @Override
  public boolean hasNext() {
    updateCurrentIterator();
    return currentIterator.hasNext() || currentDeleteIterator.hasNext();
  }

  @Override
  public T next() {
    updateCurrentIterator();
    if (currentDeleteIterator.hasNext()) {
      return currentDeleteIterator.next();
    } else {
      return currentIterator.next();
    }
  }

  /**
   * Updates the current iterator field to ensure that the current Iterator
   * is not exhausted.
   */
  private void updateCurrentIterator() {
    try {
      while (!currentDeleteIterator.hasNext() && !currentIterator.hasNext() && tasks.hasNext()) {
        FileScanTask next = tasks.next();
        if (!next.deletes().isEmpty()) {
          currentDeleteIterator.close();
          currentDeleteIterator = openDeleteTaskIterator(next);
        }

        currentIterator.close();
        Optional.ofNullable(next.file()).ifPresent(f -> currentIterator = openTaskIterator(next));

      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private CloseableIterator<T> openTaskIterator(FileScanTask scanTask) {
    return fileScanTaskReader.open(scanTask, inputFilesDecryptor);
  }

  private CloseableIterator<T> openDeleteTaskIterator(FileScanTask scanTask) {
    return fileScanTaskReader.openDelete(scanTask, inputFilesDecryptor);
  }

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
    tasks = null;
  }
}
