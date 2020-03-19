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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.spark.rdd.InputFileBlockHolder;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;

@SuppressWarnings("checkstyle:VisibilityModifier")
abstract class BaseTaskDataReader<T> implements Closeable {
  // for some reason, the apply method can't be called from Java without reflection
  static final DynMethods.UnboundMethod APPLY_PROJECTION = DynMethods.builder("apply")
      .impl(UnsafeProjection.class, InternalRow.class)
      .build();

  final Iterator<FileScanTask> tasks;
  final Schema tableSchema;
  final Schema expectedSchema;
  final FileIO fileIo;
  final Map<String, InputFile> inputFiles;
  final boolean caseSensitive;

  Iterator<T> currentIterator;
  Closeable currentCloseable = null;
  T current = null;
  final int batchSize;

  BaseTaskDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive) {
    this(task, tableSchema, expectedSchema, fileIo, encryptionManager, caseSensitive, -1);
  }

  BaseTaskDataReader(
      CombinedScanTask task, Schema tableSchema, Schema expectedSchema, FileIO fileIo,
      EncryptionManager encryptionManager, boolean caseSensitive, int bSize) {
    this.fileIo = fileIo;
    this.tasks = task.files().iterator();
    this.tableSchema = tableSchema;
    this.expectedSchema = expectedSchema;
    Iterable<InputFile> decryptedFiles = encryptionManager.decrypt(Iterables.transform(
        task.files(),
        fileScanTask ->
            EncryptedFiles.encryptedInput(
                this.fileIo.newInputFile(fileScanTask.file().path().toString()),
                fileScanTask.file().keyMetadata())));
    ImmutableMap.Builder<String, InputFile> inputFileBuilder = ImmutableMap.builder();
    decryptedFiles.forEach(decrypted -> inputFileBuilder.put(decrypted.location(), decrypted));
    this.inputFiles = inputFileBuilder.build();
    this.caseSensitive = caseSensitive;
    this.batchSize = bSize;
    // open last because the schemas, fileIo and batchSize must be set
    this.currentIterator = open(tasks.next());
  }

  public boolean next() throws IOException {
    while (true) {
      if (currentIterator.hasNext()) {
        this.current = currentIterator.next();
        return true;
      } else if (tasks.hasNext()) {
        this.currentCloseable.close();
        this.currentIterator = open(tasks.next());
      } else {
        return false;
      }
    }
  }

  abstract Iterator<T> open(FileScanTask task);

  @Override
  public void close() throws IOException {
    InputFileBlockHolder.unset();

    // close the current iterator
    this.currentCloseable.close();

    // exhaust the task iterator
    while (tasks.hasNext()) {
      tasks.next();
    }
  }
}
