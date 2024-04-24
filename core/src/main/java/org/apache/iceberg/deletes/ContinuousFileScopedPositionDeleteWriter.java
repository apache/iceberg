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
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.CharSequenceWrapper;

/**
 * A position delete writer that produces a separate delete file for each referenced data file.
 *
 * <p>This writer does not keep track of seen deletes and assumes all incoming records are ordered
 * by file and position as required by the spec. If there is no external process to order the
 * records, consider using {@link SortingPositionOnlyDeleteWriter} instead.
 */
public class ContinuousFileScopedPositionDeleteWriter<T>
    implements FileWriter<PositionDelete<T>, DeleteWriteResult> {

  private final Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writerSupplier;
  private final List<DeleteFile> deleteFiles;
  private final CharSequenceSet referencedDataFiles;

  private final Map<CharSequenceWrapper, FileWriter<PositionDelete<T>, DeleteWriteResult>> writers;
  private boolean closed = false;

  public ContinuousFileScopedPositionDeleteWriter(
      Supplier<FileWriter<PositionDelete<T>, DeleteWriteResult>> writerSupplier) {
    this.writerSupplier = writerSupplier;
    this.deleteFiles = Lists.newArrayList();
    this.referencedDataFiles = CharSequenceSet.empty();
    this.writers = Maps.newHashMap();
  }

  @Override
  public void write(PositionDelete<T> positionDelete) {
    writer(positionDelete.path()).write(positionDelete);
  }

  private FileWriter<PositionDelete<T>, DeleteWriteResult> writer(CharSequence path) {
    return writers.computeIfAbsent(CharSequenceWrapper.wrap(path), unused -> writerSupplier.get());
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
      for (FileWriter<PositionDelete<T>, DeleteWriteResult> writer : writers.values()) {
        writer.close();
        DeleteWriteResult result = writer.result();
        deleteFiles.addAll(result.deleteFiles());
        referencedDataFiles.addAll(result.referencedDataFiles());
      }

      this.closed = true;
    }
  }
}
