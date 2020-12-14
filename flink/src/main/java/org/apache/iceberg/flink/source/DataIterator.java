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
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Base class of Flink iterators.
 *
 * @param <T> is the Java class returned by this iterator whose objects contain one or more rows.
 */
public abstract class DataIterator<T> implements CloseableIterator<T> {

  private final CombinedScanTask combinedTask;
  private final Map<String, InputFile> inputFiles;

  private Iterator<FileScanTask> tasks;
  private CloseableIterator<T> currentIterator;
  private Position position;

  DataIterator(CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption) {
    this.combinedTask = combinedTask;
    Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();
    combinedTask.files().stream()
        .flatMap(fileScanTask -> Stream.concat(Stream.of(fileScanTask.file()), fileScanTask.deletes().stream()))
        .forEach(file -> keyMetadata.put(file.path().toString(), file.keyMetadata()));
    Stream<EncryptedInputFile> encrypted = keyMetadata.entrySet().stream()
        .map(entry -> EncryptedFiles.encryptedInput(io.newInputFile(entry.getKey()), entry.getValue()));

    // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
    Iterable<InputFile> decryptedFiles = encryption.decrypt(encrypted::iterator);

    Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(combinedTask.files().size());
    decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
    this.inputFiles = Collections.unmodifiableMap(files);

    this.tasks = combinedTask.files().iterator();
    this.currentIterator = CloseableIterator.empty();
    this.position = new Position();
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");

    return inputFiles.get(task.file().path().toString());
  }

  InputFile getInputFile(String location) {
    return inputFiles.get(location);
  }

  public void seek(CheckpointedPosition checkpointedPosition)  {
    // skip files
    Preconditions.checkArgument(checkpointedPosition.getOffset() < combinedTask.files().size(),
        String.format("Checkpointed file offset is %d, while CombinedScanTask has %d files",
            checkpointedPosition.getOffset(), combinedTask.files().size()));
    for (long i = 0L; i < checkpointedPosition.getOffset(); ++i) {
      tasks.next();
    }
    updateCurrentIterator();
    // skip records within the file
    for (long i = 0; i < checkpointedPosition.getRecordsAfterOffset(); ++i) {
      if (hasNext()) {
        next();
      } else {
        throw new IllegalStateException("Not enough records to skip: " +
            checkpointedPosition.getRecordsAfterOffset());
      }
    }
    position = new Position(checkpointedPosition);
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
      while (!currentIterator.hasNext() && tasks.hasNext()) {
        currentIterator.close();
        currentIterator = openTaskIterator(tasks.next());
        position.advanceFile();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  abstract CloseableIterator<T> openTaskIterator(FileScanTask scanTask) throws IOException;

  @Override
  public void close() throws IOException {
    // close the current iterator
    currentIterator.close();
    tasks = null;
  }

  public Position position() {
    return position;
  }

  public static class Position {

    private long fileOffset;
    private long recordOffset;

    Position() {
      // fileOffset starts at -1 because we started
      // from an empty iterator that is not from the split files.
      this.fileOffset = -1L;
      this.recordOffset = 0L;
    }

    Position(CheckpointedPosition checkpointedPosition) {
      this.fileOffset = checkpointedPosition.getOffset();
      this.recordOffset = checkpointedPosition.getRecordsAfterOffset();
    }

    void advanceFile() {
      this.fileOffset += 1;
      this.recordOffset = 0L;
    }

    void advanceRecord() {
      this.recordOffset += 1L;
    }

    public long fileOffset() {
      return fileOffset;
    }

    public long recordOffset() {
      return recordOffset;
    }
  }
}
