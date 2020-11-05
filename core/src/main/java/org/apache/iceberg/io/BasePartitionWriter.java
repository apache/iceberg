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
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructProjection;

public abstract class BasePartitionWriter<T> implements PartitionWriter<T> {

  private final FileGroupWriter<T> dataWriter;
  private final FileGroupWriter<T> equalityDeleteWriter;
  private final FileGroupWriter<PositionDelete<T>> positionDeleteWriter;
  private final StructProjection projectRow;
  private final Schema deleteSchema;
  private final List<Integer> equalityFieldIds;
  private final PositionDelete<T> positionDelete = new PositionDelete<>();
  private final StructLikeSet insertedDataSet;

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
                             Schema writeSchema,
                             List<Integer> equalityFieldIds) {
    this.dataWriter = dataWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.deleteSchema = TypeUtil.select(writeSchema, Sets.newHashSet(equalityFieldIds));
    this.equalityFieldIds = equalityFieldIds;
    this.projectRow = StructProjection.create(writeSchema, deleteSchema);
    this.insertedDataSet = StructLikeSet.create(deleteSchema.asStruct());
  }

  protected abstract StructLike asStructLike(T record);

  @Override
  public void append(T row) throws IOException {
    this.dataWriter.write(row);

    if (allowEqualityDelete()) {
      // TODO Put the <row, <file, offset>> into the insert data MAP, rather than SET.
      insertedDataSet.add(projectRow.wrap(asStructLike(row)));
    }
  }

  @Override
  public void delete(T deleteRow) throws IOException {
    if (!allowEqualityDelete()) {
      throw new UnsupportedOperationException("Couldn't accept equality deletions.");
    }

    if (!insertedDataSet.contains(projectRow.wrap(asStructLike(deleteRow)))) {
      this.equalityDeleteWriter.write(deleteRow);
    } else {
      // TODO Get the correct path and pos from insert data MAP rather than SET.
      this.positionDeleteWriter.write(positionDelete.set(dataWriter.currentPath(), dataWriter.currentPos(), deleteRow));
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
