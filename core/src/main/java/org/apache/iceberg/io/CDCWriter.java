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
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

public class CDCWriter<T> extends BaseDeltaWriter<T> {

  private final FanoutDataWriter<T> dataWriter;
  private final PartitionAwareFileWriter<T, DeleteWriteResult> equalityDeleteWriter;
  private final PartitionAwareFileWriter<PositionDelete<T>, DeleteWriteResult> positionDeleteWriter;
  private final StructProjection keyProjection;
  private final Map<StructLike, PartitionAwarePathOffset> insertedRows;
  private final PositionDelete<T> positionDelete;
  private final Function<T, StructLike> toStructLike;

  public CDCWriter(FanoutDataWriter<T> dataWriter,
                   PartitionAwareFileWriter<T, DeleteWriteResult> equalityDeleteWriter,
                   PartitionAwareFileWriter<PositionDelete<T>, DeleteWriteResult> positionDeleteWriter,
                   Schema schema, Schema deleteSchema, Function<T, StructLike> toStructLike) {
    this.dataWriter = dataWriter;
    this.equalityDeleteWriter = equalityDeleteWriter;
    this.positionDeleteWriter = positionDeleteWriter;
    this.positionDelete = new PositionDelete<>();
    this.keyProjection = StructProjection.create(schema, deleteSchema);
    this.insertedRows = StructLikeMap.create(deleteSchema.asStruct());
    this.toStructLike = toStructLike;
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) throws IOException {
    CharSequence currentPath = dataWriter.currentPath(spec, partition);
    long currentPosition = dataWriter.currentPosition(spec, partition);
    PartitionAwarePathOffset offset = new PartitionAwarePathOffset(spec, partition, currentPath, currentPosition);

    StructLike copiedKey = StructCopy.copy(keyProjection.wrap(toStructLike.apply(row)));

    PartitionAwarePathOffset previous = insertedRows.put(copiedKey, offset);
    if (previous != null) {
      // TODO: attach the previous row if has a position delete row schema
      positionDelete.set(previous.path(), previous.rowOffset(), null);
      positionDeleteWriter.write(positionDelete, spec, partition);
    }

    dataWriter.write(row, spec, partition);
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) throws IOException {
    StructLike key = keyProjection.wrap(toStructLike.apply(row));
    PartitionAwarePathOffset previous = insertedRows.remove(key);
    if (previous != null) {
      // TODO: attach the previous row if has a position delete row schema
      positionDelete.set(previous.path(), previous.rowOffset(), null);
      positionDeleteWriter.write(positionDelete, previous.spec, previous.partition);
    }

    equalityDeleteWriter.write(row, spec, partition);
  }

  @Override
  public void delete(CharSequence path, long pos, T row, PartitionSpec spec, StructLike partition) throws IOException {
    throw new IllegalArgumentException(this.getClass().getName() + " does not implement explicit position delete");
  }

  @Override
  protected void closeWriters() throws IOException {
    if (dataWriter != null) {
      closeDataWriter(dataWriter);
    }

    if (equalityDeleteWriter != null) {
      closeDeleteWriter(equalityDeleteWriter);
    }

    if (positionDeleteWriter != null) {
      closeDeleteWriter(positionDeleteWriter);
    }
  }

  private static class PartitionAwarePathOffset {
    private final PartitionSpec spec;
    private final StructLike partition;
    private final CharSequence path;
    private final long rowOffset;

    private PartitionAwarePathOffset(PartitionSpec spec, StructLike partition, CharSequence path, long rowOffset) {
      this.spec = spec;
      this.partition = partition;
      this.path = path;
      this.rowOffset = rowOffset;
    }

    public PartitionSpec spec() {
      return spec;
    }

    public StructLike partition() {
      return partition;
    }

    public CharSequence path() {
      return path;
    }

    public long rowOffset() {
      return rowOffset;
    }
  }
}
