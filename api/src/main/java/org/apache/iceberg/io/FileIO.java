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

import java.io.Closeable;
import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Pluggable module for reading, writing, and deleting files.
 *
 * <p>Both table metadata files and data files can be written and read by this module.
 * Implementations must be serializable because various clients of Spark tables may initialize this
 * once and pass it off to a separate module that would then interact with the streams.
 */
public interface FileIO extends Serializable, Closeable {

  /** Get a {@link InputFile} instance to read bytes from the file at the given path. */
  InputFile newInputFile(String path);

  /**
   * Get a {@link InputFile} instance to read bytes from the file at the given path, with a known
   * file length.
   */
  default InputFile newInputFile(String path, long length) {
    return newInputFile(path);
  }

  default InputFile newInputFile(DataFile file) {
    Preconditions.checkArgument(
        file.keyMetadata() == null,
        "Cannot decrypt data file: %s (use EncryptingFileIO)",
        file.path());
    return newInputFile(file.path().toString(), file.fileSizeInBytes());
  }

  default InputFile newInputFile(DeleteFile file) {
    Preconditions.checkArgument(
        file.keyMetadata() == null,
        "Cannot decrypt delete file: %s (use EncryptingFileIO)",
        file.path());
    return newInputFile(file.path().toString(), file.fileSizeInBytes());
  }

  default InputFile newInputFile(ManifestFile manifest) {
    Preconditions.checkArgument(
        manifest.keyMetadata() == null,
        "Cannot decrypt manifest: %s (use EncryptingFileIO)",
        manifest.path());
    return newInputFile(manifest.path(), manifest.length());
  }

  /** Get a {@link OutputFile} instance to write bytes to the file at the given path. */
  OutputFile newOutputFile(String path);

  /** Delete the file at the given path. */
  void deleteFile(String path);

  /** Convenience method to {@link #deleteFile(String) delete} an {@link InputFile}. */
  default void deleteFile(InputFile file) {
    deleteFile(file.location());
  }

  /** Convenience method to {@link #deleteFile(String) delete} an {@link OutputFile}. */
  default void deleteFile(OutputFile file) {
    deleteFile(file.location());
  }

  /**
   * Returns the property map used to configure this FileIO
   *
   * @throws UnsupportedOperationException if this FileIO does not expose its configuration
   *     properties
   */
  default Map<String, String> properties() {
    throw new UnsupportedOperationException(
        String.format("%s does not expose configuration properties", this.getClass().toString()));
  }

  /**
   * Initialize File IO from catalog properties.
   *
   * @param properties catalog properties
   */
  default void initialize(Map<String, String> properties) {}

  /**
   * Close File IO to release underlying resources.
   *
   * <p>Calling this method is only required when this FileIO instance is no longer expected to be
   * used, and the resources it holds need to be explicitly released to avoid resource leaks.
   */
  @Override
  default void close() {}
}
