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

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

public abstract class BasePartitionWriter<T> implements PartitionWriter<T> {

  private final FileGroupWriter<T> dataWriter;
  private final FileGroupWriter<T> equalityDeleteWriter;
  private final FileGroupWriter<PositionDelete<T>> positionDeleteWriter;
  private final StructProjection projectRow;
  private final PositionDelete<T> positionDelete = new PositionDelete<>();
  private final StructLikeMap<FilePos> insertedRowMap;

  private static class FilePos {
    private final CharSequence path;
    private final long pos;

    private FilePos(CharSequence path, long pos) {
      this.path = path;
      this.pos = pos;
    }

    private static FilePos create(CharSequence path, long pos) {
      return new FilePos(path, pos);
    }

    private CharSequence path() {
      return path;
    }

    private long pos() {
      return pos;
    }
  }

  public BasePartitionWriter(FileGroupWriter<T> dataWriter) {
    this(dataWriter, null);
  }

  public BasePartitionWriter(FileGroupWriter<T> dataWriter,
                             FileGroupWriter<PositionDelete<T>> positionDeleteWriter) {
    this(dataWriter, positionDeleteWriter, null, null, null);
  }

  public BasePartitionWriter(FileGroupWriter<T> dataWriter,
                             FileGroupWriter<PositionDelete<T>> positionDeleteWriter,
                             FileGroupWriter<T> equalityDeleteWriter,
                             Schema tableSchema,
                             List<Integer> equalityFieldIds) {
    this.dataWriter = dataWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;

    Schema deleteSchema = TypeUtil.select(tableSchema, Sets.newHashSet(equalityFieldIds));
    this.projectRow = StructProjection.create(tableSchema, deleteSchema);
    this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
  }

  protected abstract StructLike asStructLike(T record);

  @Override
  public void append(T row) throws IOException {
    if (allowEqualityDelete()) {
      FilePos filePos = FilePos.create(dataWriter.currentPath(), dataWriter.currentPos());
      insertedRowMap.put(projectRow.wrap(asStructLike(row)), filePos);
    }

    this.dataWriter.write(row);
  }

  @Override
  public void delete(T deleteRow) throws IOException {
    if (!allowEqualityDelete()) {
      throw new UnsupportedOperationException("Couldn't accept equality deletions.");
    }

    FilePos existingFilePos = insertedRowMap.get(projectRow.wrap(asStructLike(deleteRow)));

    if (existingFilePos == null) {
      // Delete the row which has been written before this writer.
      this.equalityDeleteWriter.write(deleteRow);
    } else {
      // Delete the row which was written in current writer.
      this.positionDeleteWriter.write(positionDelete.set(existingFilePos.path(), existingFilePos.pos(), deleteRow));
    }
  }

  @Override
  public void delete(CharSequence path, long pos, T row) throws IOException {
    if (!allowPositionDelete()) {
      throw new UnsupportedOperationException("Couldn't accept positional deletions.");
    }

    this.positionDeleteWriter.write(positionDelete.set(path, pos, row));
  }

  @Override
  public void abort() throws IOException {
    if (dataWriter != null) {
      dataWriter.abort();
    }

    if (equalityDeleteWriter != null) {
      equalityDeleteWriter.abort();
    }

    if (positionDeleteWriter != null) {
      positionDeleteWriter.abort();
    }
  }

  @Override
  public WriterResult complete() throws IOException {
    WriterResult.Builder builder = WriterResult.builder();

    if (dataWriter != null) {
      builder.addAll(dataWriter.complete().contentFiles());
    }

    if (equalityDeleteWriter != null) {
      builder.addAll(equalityDeleteWriter.complete().contentFiles());
    }

    if (positionDeleteWriter != null) {
      builder.addAll(positionDeleteWriter.complete().contentFiles());
    }

    return builder.build();
  }

  private boolean allowEqualityDelete() {
    return equalityDeleteWriter != null && positionDeleteWriter != null;
  }

  private boolean allowPositionDelete() {
    return positionDeleteWriter != null;
  }
}
