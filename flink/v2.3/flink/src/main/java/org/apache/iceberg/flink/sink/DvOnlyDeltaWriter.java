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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.data.RowDataProjection;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.flink.maintenance.operator.StructLikeSerializer;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.iceberg.util.StructLikeUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.iceberg.util.Tasks;

/**
 * Writes the changes of one checkpoint for the DV-only write path, which commits data files and
 * deletion vectors but never an equality delete file.
 *
 * <p>Inserts become data files. A delete of a row this writer wrote in the same checkpoint is
 * reported as the position of that row; any other delete is reported as its equality key, to be
 * resolved against the primary key index downstream. The rows still live when the writer completes
 * are reported with their keys and positions, so that the index learns about them.
 *
 * <p>The reports are read through {@link #positionDeletes()}, {@link #deleteKeys()} and {@link
 * #liveRows()} once {@link #complete()} returned.
 */
class DvOnlyDeltaWriter extends BaseTaskWriter<RowData> implements BaseDeltaTaskWriter.RowChanges {

  private final Schema deleteSchema;
  private final RowDataWrapper wrapper;
  private final RowDataWrapper keyWrapper;
  private final RowDataProjection keyProjection;
  private final StructProjection structProjection;
  private final boolean upsert;
  private final PartitionKey partitionKey;
  private final StructLikeSerializer serializer = new StructLikeSerializer();

  private final Map<StructLike, RollingFileWriter> dataWriters = Maps.newHashMap();

  private final Map<StructLike, RowPosition> insertedRows;

  private final Set<SerializedEqualityValues> deleteKeys = Sets.newLinkedHashSet();

  // Deletion vectors are written downstream, where the vector a data file currently carries is
  // known, so rows this writer removes again are handed on as positions instead.
  private final List<DVPosition> positionDeletes = Lists.newArrayList();

  private List<PkIndexEntry> liveRows = Lists.newArrayList();

  DvOnlyDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<RowData> fileWriterFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      RowType flinkSchema,
      Set<Integer> equalityFieldIds,
      boolean upsert) {
    super(spec, format, fileWriterFactory, fileFactory, io, targetFileSize, false);
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
    this.wrapper = new RowDataWrapper(flinkSchema, schema.asStruct());
    this.keyWrapper =
        new RowDataWrapper(FlinkSchemaUtil.convert(deleteSchema), deleteSchema.asStruct());
    this.keyProjection =
        RowDataProjection.create(flinkSchema, schema.asStruct(), deleteSchema.asStruct());
    this.structProjection = StructProjection.create(schema, deleteSchema);
    this.upsert = upsert;
    this.partitionKey = spec.isUnpartitioned() ? null : new PartitionKey(spec, schema);
    this.insertedRows = StructLikeMap.create(deleteSchema.asStruct());
  }

  @Override
  public void write(RowData row) throws IOException {
    BaseDeltaTaskWriter.applyChange(row, upsert, keyProjection, this);
  }

  Set<SerializedEqualityValues> deleteKeys() {
    return deleteKeys;
  }

  List<PkIndexEntry> liveRows() {
    return liveRows;
  }

  List<DVPosition> positionDeletes() {
    return positionDeletes;
  }

  @Override
  public void insert(RowData row) throws IOException {
    StructLike partition = partitionFor(row);
    RollingFileWriter writer = writerFor(partition);
    // Captured before the write so that it describes the row about to be appended. Rolling to a
    // new file only happens afterwards, which leaves already captured positions valid.
    RowPosition position =
        new RowPosition(writer.currentPath().toString(), writer.currentRows(), partition);

    StructLike key = StructLikeUtil.copy(structProjection.wrap(wrapper.wrap(row)));
    RowPosition previous = insertedRows.put(key, position);
    if (previous != null) {
      writePositionDelete(previous);
    }

    writer.write(row);
  }

  @Override
  public void delete(RowData row) {
    resolveOrCollect(structProjection.wrap(wrapper.wrap(row)));
  }

  @Override
  public void deleteKey(RowData key) {
    resolveOrCollect(keyWrapper.wrap(key));
  }

  private void resolveOrCollect(StructLike key) {
    RowPosition previous = insertedRows.remove(key);
    if (previous != null) {
      writePositionDelete(previous);
    } else {
      deleteKeys.add(serializer.serializeKey(key, deleteSchema.asStruct()));
    }
  }

  private void writePositionDelete(RowPosition position) {
    positionDeletes.add(position.toDVPosition(serializer, spec()));
  }

  private StructLike partitionFor(RowData row) {
    if (partitionKey == null) {
      return null;
    }

    partitionKey.partition(wrapper.wrap(row));
    return partitionKey.copy();
  }

  private RollingFileWriter writerFor(StructLike partition) {
    return dataWriters.computeIfAbsent(partition, RollingFileWriter::new);
  }

  @Override
  public void close() throws IOException {
    try {
      Tasks.foreach(dataWriters.values())
          .throwFailureWhenFinished()
          .noRetry()
          .run(RollingFileWriter::close, IOException.class);
      dataWriters.clear();
    } finally {
      super.close();
    }
  }

  @Override
  public WriteResult complete() throws IOException {
    WriteResult result = super.complete();
    this.liveRows = Lists.newArrayListWithExpectedSize(insertedRows.size());
    for (Map.Entry<StructLike, RowPosition> entry : insertedRows.entrySet()) {
      RowPosition position = entry.getValue();
      liveRows.add(
          new PkIndexEntry(
              serializer.serializeKey(entry.getKey(), deleteSchema.asStruct()),
              position.toDVPosition(serializer, spec())));
    }

    insertedRows.clear();
    return result;
  }

  private static final class RowPosition {
    private final String path;
    private final long offset;
    private final StructLike partition;

    private RowPosition(String path, long offset, StructLike partition) {
      this.path = path;
      this.offset = offset;
      this.partition = partition;
    }

    private DVPosition toDVPosition(StructLikeSerializer structLikeSerializer, PartitionSpec spec) {
      byte[] encoded =
          partition == null
              ? StructLikeSerializer.EMPTY_PARTITION
              : structLikeSerializer.encodePartition(partition, spec.partitionType());
      return new DVPosition(path, offset, spec.specId(), encoded, PkIndexEntry.UNKNOWN_SEQUENCE);
    }
  }
}
