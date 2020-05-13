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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.sources.v2.reader.InputPartitionReader;

/**
 * Base class of readers of type {@link InputPartitionReader} to read data as objects of type @param &lt;T&gt;
 *
 * @param <T> is the Java class returned by this reader whose objects contain one or more rows.
 */
abstract class BaseDataReader<T> implements InputPartitionReader<T> {
  private final Iterator<FileScanTask> tasks;
  private final FileIO fileIo;
  private final Map<String, InputFile> inputFiles;

  private CloseableIterator<T> currentIterator;
  private T current = null;

  BaseDataReader(CombinedScanTask task, FileIO fileIo, EncryptionManager encryptionManager) {
    this.fileIo = fileIo;
    this.tasks = task.files().iterator();
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(Iterables.transform(
        task.files(),
        fileScanTask ->
            EncryptedFiles.encryptedInput(
                this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                fileScanTask.file().keyMetadata())));
    ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
    decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
    this.inputFiles = inputFileBuilder.build();
    this.currentIterator = CloseableIterator.empty();
  }

  @Override
  public boolean next() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        this.current = currentIterator.next();
        return true;
      } else if (tasks.hasNext()) {
        this.currentIterator.close();
        this.currentIterator = open(tasks.next());
      } else {
        return false;
      }
    }
  }

  @Override
  public T get() {
    return current;
  }

  abstract CloseableIterator<T> open(FileScanTask task);

  @Override
  public void close() throws IOException {
    InputFileBlockHolder.unset();

    // close the current iterator
    this.currentIterator.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }

  InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return inputFiles.get(task.file().path().toString());
  }
}
