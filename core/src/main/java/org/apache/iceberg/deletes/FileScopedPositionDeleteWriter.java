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
package org.apache.iceberg.deletes;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceUtil;

/**
 * A position delete writer that produces a separate delete file for each referenced data file.
 *
 * <p>This writer does not keep track of seen deletes and assumes all incoming records are ordered
 * by file and position as required by the spec. If there is no external process to order the
 * records, consider using {@link SortingPositionOnlyDeleteWriter} instead.
 */
public class FileScopedPositionDeleteWriter<T>
    implements FileWriter<PositionDelete<T>, DeleteWriteResult> {

  private final Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers;
  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  private FileWriter<PositionDelete<T>, DeleteWriteResult> currentWriter = null;
  private CharSequence currentPath = null;
  private boolean closed = false;

  public FileScopedPositionDeleteWriter(
      Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writers) {
    this.writers = writers;
    this.deleteFiles = Lists.newArrayList();
    this.referencedDataFiles = CharSequenceSet.empty();
  }

  @Override
  public void write(PositionDelete<T> positionDelete) {
    writer(positionDelete.path()).write(positionDelete);
  }

  private FileWriter<PositionDelete<T>, DeleteWriteResult> writer(CharSequence path) {
    if (currentWriter == null) {
      openCurrentWriter(path);
    } else if (CharSequenceUtil.unequalPaths(currentPath, path)) {
      closeCurrentWriter();
      openCurrentWriter(path);
    }

    return currentWriter;
  }

  @Override
  public long length() {
    throw new UnsupportedOperationException(getClass().getName() + " does not implement length");
  }

  @Override
  public DeleteWriteResult result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer");
    return new DeleteWriteResult(deleteFiles, referencedDataFiles);
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      closeCurrentWriter();
      this.closed = true;
    }
  }

  private void openCurrentWriter(CharSequence path) {
    Preconditions.checkState(!closed, "Writer has already been closed");
    this.currentWriter = writers.get();
    this.currentPath = path;
  }

  private void closeCurrentWriter() {
    if (currentWriter != null) {
      try {
        currentWriter.close();
        DeleteWriteResult result = currentWriter.result();
        deleteFiles.addAll(result.deleteFiles());
        referencedDataFiles.addAll(result.referencedDataFiles());
        this.currentWriter = null;
        this.currentPath = null;
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close current writer", e);
      }
    }
  }
}
