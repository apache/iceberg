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
package org.apache.iceberg.encryption;

import java.util.Collection;
import java.util.Map;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/** Resolves the files referenced by scan tasks into readable, decrypted {@link InputFile}s. */
public class InputFilesDecryptor {

  private final Iterable<? extends ContentFile<?>> referencedFiles;
  private final EncryptingFileIO encryptingIO;
  private volatile Map<String, InputFile> lazyInputFiles = null;

  /**
   * @deprecated since 1.12.0, will be removed in 1.13.0; use {@link #fromTasks(Iterable,
   *     EncryptingFileIO)} instead.
   */
  @Deprecated
  public InputFilesDecryptor(
      CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption) {
    this(
        () -> referencedFiles(combinedTask.files()).iterator(),
        EncryptingFileIO.combine(io, encryption));
  }

  public static InputFilesDecryptor fromTasks(
      Iterable<FileScanTask> tasks, EncryptingFileIO encryptingIO) {
    return new InputFilesDecryptor(() -> referencedFiles(tasks).iterator(), encryptingIO);
  }

  private InputFilesDecryptor(
      Iterable<? extends ContentFile<?>> files, EncryptingFileIO encryptingIO) {
    this.referencedFiles = files;
    this.encryptingIO = encryptingIO;
  }

  private Map<String, InputFile> inputFiles() {
    if (lazyInputFiles == null) {
      synchronized (this) {
        if (lazyInputFiles == null) {
          this.lazyInputFiles = encryptingIO.bulkDecrypt(referencedFiles);
        }
      }
    }

    return lazyInputFiles;
  }

  private static Collection<ContentFile<?>> referencedFiles(Iterable<FileScanTask> tasks) {
    Map<String, ContentFile<?>> files = Maps.newHashMap();
    for (FileScanTask task : tasks) {
      files.put(task.file().location(), task.file());
      for (DeleteFile delete : task.deletes()) {
        files.put(delete.location(), delete);
      }
    }

    return files.values();
  }

  /**
   * @deprecated since 1.12.0, will be removed in 1.13.0; use {@link #getInputFile(String)} instead.
   */
  @Deprecated
  public InputFile getInputFile(FileScanTask task) {
    Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
    return getInputFile(task.file().location());
  }

  public InputFile getInputFile(String location) {
    InputFile inputFile = inputFiles().get(location);
    Preconditions.checkArgument(
        inputFile != null, "Cannot find input file for location: %s", location);
    return inputFile;
  }
}
