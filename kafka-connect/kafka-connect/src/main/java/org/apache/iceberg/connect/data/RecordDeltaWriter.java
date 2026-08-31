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
package org.apache.iceberg.connect.data;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.StructProjection;

/**
 * A delta task writer that supports insert / update / delete on Iceberg V2 tables via equality
 * deletes. Backs the sink's upsert mode.
 */
class RecordDeltaWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final PartitionKey partitionKey;
  private final InternalRecordWrapper partitionWrapper;
  private final Map<PartitionKey, RecordEqualityDeltaWriter> writers = Maps.newHashMap();
  private final boolean unpartitioned;
  private RecordEqualityDeltaWriter singleWriter;

  RecordDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> equalityFieldIds) {
    super(spec, format, writerFactory, fileFactory, io, targetFileSize);
    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, equalityFieldIds);
    this.partitionKey = new PartitionKey(spec, schema);
    this.partitionWrapper = new InternalRecordWrapper(schema.asStruct());
    this.unpartitioned = spec.isUnpartitioned();
  }

  /** Insert a new row: equality-delete the key first (idempotent on replay) then append. */
  void insertRow(Record row) throws IOException {
    RecordEqualityDeltaWriter writer = writerFor(row);
    writer.deleteKey(row);
    writer.write(row);
  }

  /** Update an existing row: equality-delete the key then append the new version. */
  void updateRow(Record row) throws IOException {
    RecordEqualityDeltaWriter writer = writerFor(row);
    writer.deleteKey(row);
    writer.write(row);
  }

  /** Delete a row by its equality key. */
  void deleteRow(Record row) throws IOException {
    RecordEqualityDeltaWriter writer = writerFor(row);
    writer.deleteKey(row);
  }

  /** TaskWriter contract; defaults to insert semantics. */
  @Override
  public void write(Record row) throws IOException {
    insertRow(row);
  }

  private RecordEqualityDeltaWriter writerFor(Record row) {
    if (unpartitioned) {
      if (singleWriter == null) {
        singleWriter = new RecordEqualityDeltaWriter(null);
      }
      return singleWriter;
    }

    partitionKey.partition(partitionWrapper.wrap(row));
    RecordEqualityDeltaWriter writer = writers.get(partitionKey);
    if (writer == null) {
      PartitionKey copiedKey = partitionKey.copy();
      writer = new RecordEqualityDeltaWriter(copiedKey);
      writers.put(copiedKey, writer);
    }
    return writer;
  }

  @Override
  public void close() throws IOException {
    try {
      super.close();
      if (singleWriter != null) {
        singleWriter.close();
        singleWriter = null;
      }
      for (RecordEqualityDeltaWriter w : writers.values()) {
        w.close();
      }
      writers.clear();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close record delta writer", e);
    }
  }

  private class RecordEqualityDeltaWriter extends BaseEqualityDeltaWriter {
    private final InternalRecordWrapper rowWrapper;
    private final InternalRecordWrapper keyWrapper;
    private final StructProjection keyProjection;

    RecordEqualityDeltaWriter(StructLike partition) {
      super(partition, schema, deleteSchema);
      this.rowWrapper = new InternalRecordWrapper(schema.asStruct());
      this.keyWrapper = new InternalRecordWrapper(schema.asStruct());
      this.keyProjection = StructProjection.create(schema, deleteSchema);
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return rowWrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record data) {
      return keyProjection.wrap(keyWrapper.wrap(data));
    }
  }
}
