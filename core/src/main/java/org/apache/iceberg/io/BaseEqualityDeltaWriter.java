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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Function;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;

public class BaseEqualityDeltaWriter<T> implements EqualityDeltaWriter<T> {

  private final ThreadLocal<PositionDelete<T>> posDelete = ThreadLocal.withInitial(PositionDelete::create);

  private final PartitioningWriter<T, DataWriteResult> dataWriter;
  private final PartitioningWriter<T, DeleteWriteResult> equalityWriter;
  private final PartitioningWriter<PositionDelete<T>, DeleteWriteResult> positionWriter;

  private final Map<StructLike, PathOffset> insertedRowMap;

  private final Function<T, StructLike> asStructLike;
  private final Function<T, StructLike> keyRefFunc;
  private final Function<T, StructLike> keyCopyFunc;

  private boolean closed = false;

  public BaseEqualityDeltaWriter(
      PartitioningWriter<T, DataWriteResult> dataWriter,
      PartitioningWriter<T, DeleteWriteResult> equalityWriter,
      PartitioningWriter<PositionDelete<T>, DeleteWriteResult> positionWriter,
      Schema schema,
      Schema deleteSchema,
      Function<T, StructLike> asStructLike) {
    this.dataWriter = dataWriter;
    this.equalityWriter = equalityWriter;
    this.positionWriter = positionWriter;

    this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
    this.asStructLike = asStructLike;

    StructProjection projection = StructProjection.create(schema, deleteSchema);
    this.keyRefFunc = row -> projection.wrap(asStructLike.apply(row));
    this.keyCopyFunc = row -> StructCopy.copy(keyRefFunc.apply(row));
  }

  @Override
  public void insert(T row, PartitionSpec spec, StructLike partition) {
    PathOffset pathOffset = dataWriter.write(row, spec, partition);

    // Create a copied key from this row.
    StructLike copiedKey = keyCopyFunc.apply(row);

    // Adding a pos-delete to replace the old path-offset.
    PathOffset previous = insertedRowMap.put(copiedKey, pathOffset);
    if (previous != null) {
      positionWriter.write(posDelete.get().set(previous.path(), previous.rowOffset(), null), spec, partition);
    }
  }

  /**
   * Retire the old key & position from insertedRowMap cache to position delete file.
   */
  private boolean retireOldKey(StructLike key, PartitionSpec spec, StructLike partition) {
    PathOffset previous = insertedRowMap.remove(key);
    if (previous != null) {
      positionWriter.write(posDelete.get().set(previous.path(), previous.rowOffset(), null), spec, partition);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void delete(T row, PartitionSpec spec, StructLike partition) {
    if (!retireOldKey(keyRefFunc.apply(row), spec, partition)) {
      equalityWriter.write(row, spec, partition);
    }
  }

  @Override
  public void deleteKey(T key, PartitionSpec spec, StructLike partition) {
    if (!retireOldKey(asStructLike.apply(key), spec, partition)) {
      equalityWriter.write(key, spec, partition);
    }
  }

  @Override
  public WriteResult result() {
    Preconditions.checkState(closed, "Cannot get result from unclosed writer.");

    DataWriteResult dataWriteResult = dataWriter.result();
    DeleteWriteResult positionWriteResult = positionWriter.result();
    DeleteWriteResult equalityWriteResult = equalityWriter.result();

    return WriteResult.builder()
        .addDataFiles(dataWriteResult.dataFiles())
        .addDeleteFiles(positionWriteResult.deleteFiles())
        .addDeleteFiles(equalityWriteResult.deleteFiles())
        .addReferencedDataFiles(positionWriteResult.referencedDataFiles())
        .build();
  }

  @Override
  public void close() throws IOException {
    if (!closed) {
      dataWriter.close();
      positionWriter.close();
      equalityWriter.close();

      this.closed = true;
    }
  }
}
