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

package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.io.DataWriteResult;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PartitioningWriter;
import org.apache.iceberg.io.PartitioningWriterFactory;
import org.apache.iceberg.io.PathOffset;
import org.apache.iceberg.io.StructCopy;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ObjectArrays;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;

public class FlinkTaskWriter implements TaskWriter<RowData> {
  public static Function<RowData, StructLike> partitionerFor(
      PartitionSpec spec,
      Schema schema,
      RowType flinkSchema) {
    return new Function<RowData, StructLike>() {
      private final RowDataWrapper wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
      private final PartitionKey partitionKey = new PartitionKey(spec, schema);

      @Override
      public StructLike apply(RowData row) {
        partitionKey.partition(wrapper.wrap(row));
        return partitionKey;
      }
    };
  }

  public static Function<RowData, StructLike> equalityKeyerFor(
      Set<Integer> equalityFieldIds,
      Schema schema,
      RowType flinkSchema) {
    return new Function<RowData, StructLike>() {
      private final RowDataWrapper wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
      private final StructProjection structProjection = StructProjection.create(schema, equalityFieldIds);

      @Override
      public StructLike apply(RowData row) {
        structProjection.wrap(wrapper.wrap(row));
        return structProjection;
      }
    };
  }

  private final PartitioningWriter<RowData, DataWriteResult> insertWriter;
  private final PartitioningWriter<RowData, DeleteWriteResult> equalityDeleteWriter;
  private final PartitioningWriter<PositionDelete<RowData>, DeleteWriteResult> positionDeleteWriter;

  private final PartitionSpec spec;
  private final FileIO io;
  private final boolean upsert;
  private final Function<RowData, StructLike> partitioner;
  private final Function<RowData, StructLike> equalityKeyer;

  private StructLike partition;
  private StructLike equalityKey;
  private final PositionDelete<RowData> positionDelete;
  private final Map<StructLike, PathOffset> insertedRowMap; // map row Key to PathOffset

  protected FlinkTaskWriter(
      PartitioningWriterFactory<RowData> partitioningWriterFactory,
      Function<RowData, StructLike> partitioner,
      Schema schema, PartitionSpec spec, Set<Integer> equalityFieldIds,
      FileIO io, RowType flinkSchema, boolean upsert) {
    this.insertWriter = partitioningWriterFactory.newDataWriter();
    this.equalityDeleteWriter = partitioningWriterFactory.newEqualityDeleteWriter();
    this.positionDeleteWriter = partitioningWriterFactory.newPositionDeleteWriter();

    this.spec = spec;
    this.io = io;
    this.upsert = upsert;

    this.insertedRowMap = StructLikeMap.create(TypeUtil.select(schema.asStruct(), equalityFieldIds));
    this.positionDelete = PositionDelete.create();
    this.partitioner = partitioner;
    this.equalityKeyer = equalityKeyerFor(equalityFieldIds, schema, flinkSchema);
  }

  @Override
  @SuppressWarnings("DuplicateBranchesInSwitch")
  public void write(RowData row) throws IOException {
    partition = partitioner.apply(row);
    equalityKey = equalityKeyer.apply(row);

    switch (row.getRowKind()) {
      case INSERT:
        if (upsert) {
          delete(row);
        }
        insert(row);
        break;

      case UPDATE_BEFORE:
        if (upsert) {
          break;  // UPDATE_BEFORE is not necessary for UPDATE, ignore to prevent deleting twice
        }
        delete(row);
        break;

      case UPDATE_AFTER:
        if (upsert) {
          delete(row);
        }
        insert(row);
        break;

      case DELETE:
        delete(row);
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + row.getRowKind());
    }
  }

  protected boolean internalPosDelete(StructLike key) {
    PathOffset previous = insertedRowMap.remove(key);
    if (previous != null) {
      positionDeleteWriter.write(previous.setTo(positionDelete), spec, partition);
      return true;
    }
    return false;
  }

  protected void insert(RowData row) throws IOException {
    StructLike copiedKey = StructCopy.copy(equalityKey);

    internalPosDelete(copiedKey);
    PathOffset pathOffset = insertWriter.write(row, spec, partition);
    insertedRowMap.put(copiedKey, pathOffset);
  }

  protected void delete(RowData row) throws IOException {
    if (!internalPosDelete(equalityKey)) {
      equalityDeleteWriter.write(row, spec, partition);
    }
  }

  @Override
  public void abort() throws IOException {
    close();

    // clean up files created by this writer
    WriteResult result = writeResult();
    Tasks.foreach(ObjectArrays.concat(result.dataFiles(), result.deleteFiles(), ContentFile.class))
        .throwFailureWhenFinished()
        .noRetry()
        .run(file -> io.deleteFile(file.path().toString()));
  }

  @Override
  public WriteResult complete() throws IOException {
    close();
    return writeResult();
  }

  @Override
  public void close() throws IOException {
    insertWriter.close();
    equalityDeleteWriter.close();
    positionDeleteWriter.close();
  }

  private WriteResult writeResult() {
    DataWriteResult insertResult = insertWriter.result();
    DeleteWriteResult equalityDeleteResult = equalityDeleteWriter.result();
    DeleteWriteResult positionDeleteResult = positionDeleteWriter.result();

    return WriteResult.builder()
        .addDataFiles(insertResult.dataFiles())
        .addDeleteFiles(equalityDeleteResult.deleteFiles())
        .addDeleteFiles(positionDeleteResult.deleteFiles())
        .addReferencedDataFiles(positionDeleteResult.referencedDataFiles())
        .build();
  }
}
