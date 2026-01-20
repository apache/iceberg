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
import java.util.Set;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;

abstract class BaseDeltaWriter extends BaseTaskWriter<Record> {

  private final Schema schema;
  private final Schema deleteSchema;
  private final InternalRecordWrapper wrapper;
  private final InternalRecordWrapper keyWrapper;
  private final RecordProjection keyProjection;
  private final boolean upsert;
  private final String[] cdcField;

  BaseDeltaWriter(
      PartitionSpec spec,
      FileFormat format,
      FileWriterFactory<Record> writerFactory,
      OutputFileFactory fileFactory,
      FileIO io,
      long targetFileSize,
      Schema schema,
      Set<Integer> identifierFieldIds,
      boolean upsert,
      boolean useDv,
      String cdcField) {
    super(spec, format, writerFactory, fileFactory, io, targetFileSize, useDv);

    this.schema = schema;
    this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(identifierFieldIds));
    this.wrapper = new InternalRecordWrapper(schema.asStruct());
    this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
    this.keyProjection = RecordProjection.create(schema, deleteSchema);
    this.upsert = upsert;

    if (cdcField == null || cdcField.isEmpty()) {
      throw new IllegalArgumentException("CDC field must be provided for delta writer");
    }
    this.cdcField = cdcField.split("\\.");
  }

  abstract RowDataDeltaWriter route(Record row);

  @Override
  public void write(Record row) throws IOException {

    Operation op = Operation.fromString(getCdcOpFromRow(row));
    RowDataDeltaWriter writer = route(row);

    switch (op) {
      case INSERT:
        writer.write(row);
        break;
      case UPDATE:
        if (upsert) {
          writer.deleteKey(keyProjection.wrap(row));
        } else {
          writer.delete(row);
        }
        writer.write(row);
        break;
      case DELETE:
        if (upsert) {
          writer.deleteKey(keyProjection.wrap(row));
        } else {
          writer.delete(row);
        }
        break;

      default:
        throw new UnsupportedOperationException("Unknown row kind: " + op);
    }
  }

  private String getCdcOpFromRow(Record row) {
    for (String field : cdcField) {
      Object value = row.getField(field);
      if (value == null) {
        throw new IllegalArgumentException("CDC field " + String.join(".", cdcField) + " is null");
      }
      if (value instanceof String) {
        return (String) value;
      } else {
        row = (Record) value;
      }
    }
    throw new IllegalArgumentException(
        "CDC field " + String.join(".", cdcField) + " is not a string");
  }

  public InternalRecordWrapper getWrapper() {
    return wrapper;
  }

  protected class RowDataDeltaWriter extends BaseEqualityDeltaWriter {

    RowDataDeltaWriter(PartitionKey partition) {
      super(partition, schema, deleteSchema, DeleteGranularity.FILE, dvFileWriter());
    }

    @Override
    protected StructLike asStructLike(Record data) {
      return wrapper.wrap(data);
    }

    @Override
    protected StructLike asStructLikeKey(Record data) {
      return keyWrapper.wrap(data);
    }
  }
}
