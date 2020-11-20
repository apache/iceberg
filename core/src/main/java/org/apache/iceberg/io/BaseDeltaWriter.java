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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;

public abstract class BaseDeltaWriter<T> implements DeltaWriter<T> {
  private final RollingContentFileWriter<DataFile, T> dataWriter;
  private final RollingEqDeleteWriter<T> eqDeleteWriter;
  private final RollingPosDeleteWriter<T> posDeleteWriter;

  private final PositionDelete<T> positionDelete = new PositionDelete<>();
  private final StructLikeMap<RowOffset> insertedRowMap;

  protected BaseDeltaWriter(RollingContentFileWriter<DataFile, T> dataWriter,
                            RollingPosDeleteWriter<T> posDeleteWriter,
                            RollingEqDeleteWriter<T> eqDeleteWriter,
                            Schema tableSchema,
                            List<Integer> equalityFieldIds) {

    Preconditions.checkNotNull(dataWriter, "Data writer should always not be null.");

    if (posDeleteWriter == null) {
      // Only accept INSERT records.
      Preconditions.checkArgument(eqDeleteWriter == null,
          "Could not accept equality deletes when position delete writer is null.");
    }

    if (posDeleteWriter != null && eqDeleteWriter == null) {
      // Only accept INSERT records and POS-DELETE records.
      Preconditions.checkArgument(tableSchema == null, "Table schema is only required for equality delete writer.");
      Preconditions.checkArgument(equalityFieldIds == null,
          "Equality field id list is only required for equality delete writer.");
    }

    if (eqDeleteWriter != null) {
      // Accept INSERT records, POS-DELETE records and EQUALITY-DELETE records.
      Preconditions.checkNotNull(posDeleteWriter,
          "Position delete writer shouldn't be null when writing equality deletions.");
      Preconditions.checkNotNull(tableSchema, "Iceberg table schema shouldn't be null");
      Preconditions.checkNotNull(equalityFieldIds, "Equality field ids shouldn't be null");

      Schema deleteSchema = TypeUtil.select(tableSchema, Sets.newHashSet(equalityFieldIds));
      this.insertedRowMap = StructLikeMap.create(deleteSchema.asStruct());
    } else {
      this.insertedRowMap = null;
    }

    this.dataWriter = dataWriter;
    this.eqDeleteWriter = eqDeleteWriter;
    this.posDeleteWriter = posDeleteWriter;
  }

  protected abstract StructLike asKey(T row);

  protected abstract StructLike asCopiedKey(T row);

  @Override
  public void writeRow(T row) {
    if (allowEqDelete()) {
      RowOffset rowOffset = RowOffset.create(dataWriter.currentPath(), dataWriter.currentRows());

      // Copy the key to avoid messing up the insertedRowMap.
      StructLike key = asCopiedKey(row);
      RowOffset previous = insertedRowMap.putIfAbsent(key, rowOffset);
      ValidationException.check(previous == null, "Detected duplicate insert for %s", key);
    }

    dataWriter.write(row);
  }

  @Override
  public void writeEqualityDelete(T equalityDelete) {
    Preconditions.checkState(allowEqDelete(), "Could not accept equality deletion.");

    StructLike key = asKey(equalityDelete);
    RowOffset existing = insertedRowMap.get(key);

    if (existing == null) {
      // Delete the row which have been written by other completed delta writer.
      eqDeleteWriter.write(equalityDelete);
    } else {
      // Delete the rows which was written in current delta writer. If the position delete row schema is null, then the
      // writer won't write the records even if we provide the rows here.
      posDeleteWriter.write(positionDelete.set(existing.path, existing.rowId, equalityDelete));
      // Remove the records from insertedRowMap because we've already deleted it by writing position delete file.
      insertedRowMap.remove(key);
    }
  }

  @Override
  public void writePosDelete(CharSequence path, long offset, T row) {
    Preconditions.checkState(allowPosDelete(), "Could not accept position deletion.");

    posDeleteWriter.write(positionDelete.set(path, offset, row));
  }

  @Override
  public void abort() {
    if (dataWriter != null) {
      dataWriter.abort();
    }

    if (eqDeleteWriter != null) {
      eqDeleteWriter.abort();
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      posDeleteWriter.abort();
    }
  }

  @Override
  public WriterResult complete() {
    WriterResult.Builder builder = WriterResult.builder();

    if (dataWriter != null) {
      builder.add(dataWriter.complete());
    }

    if (eqDeleteWriter != null) {
      builder.add(eqDeleteWriter.complete());
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      builder.add(posDeleteWriter.complete());
    }

    return builder.build();
  }

  @Override
  public void close() throws IOException {
    if (dataWriter != null) {
      dataWriter.close();
    }

    if (eqDeleteWriter != null) {
      eqDeleteWriter.close();
      insertedRowMap.clear();
    }

    if (posDeleteWriter != null) {
      posDeleteWriter.close();
    }
  }

  private boolean allowEqDelete() {
    return eqDeleteWriter != null && posDeleteWriter != null;
  }

  private boolean allowPosDelete() {
    return posDeleteWriter != null;
  }

  private static class RowOffset {
    private final CharSequence path;
    private final long rowId;

    private RowOffset(CharSequence path, long rowId) {
      this.path = path;
      this.rowId = rowId;
    }

    private static RowOffset create(CharSequence path, long pos) {
      return new RowOffset(path, pos);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("path", path)
          .add("row_id", rowId)
          .toString();
    }
  }
}
