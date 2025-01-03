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
package org.apache.iceberg.data.parquet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueReaders.StructReader;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class InternalReader extends BaseParquetReaders<StructLike> {

  private static final InternalReader INSTANCE = new InternalReader();

  private InternalReader() {}

  public static ParquetValueReader<StructLike> buildReader(
      Schema expectedSchema, MessageType fileSchema) {
    return INSTANCE.createReader(expectedSchema, fileSchema);
  }

  public static ParquetValueReader<StructLike> buildReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return INSTANCE.createReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  protected ParquetValueReader<StructLike> createStructReader(
      List<Type> types, List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    return new StructLikeReader(types, fieldReaders, structType);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> fixedReader(ColumnDescriptor desc) {
    return new ParquetValueReaders.BytesReader(desc);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> int96Reader(ColumnDescriptor desc) {
    return new ParquetValueReaders.TimestampInt96Reader(desc);
  }

  @Override
  protected Optional<ParquetValueReader<?>> dateReader(ColumnDescriptor desc) {
    return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
  }

  @Override
  protected Optional<ParquetValueReader<?>> timeReader(
      ColumnDescriptor desc, LogicalTypeAnnotation.TimeUnit unit) {
    if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
      return Optional.of(new ParquetValueReaders.TimestampMillisReader(desc));
    }

    return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
  }

  @Override
  protected Optional<ParquetValueReader<?>> timestampReader(
      ColumnDescriptor desc, LogicalTypeAnnotation.TimeUnit unit, boolean isAdjustedToUTC) {
    if (unit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
      return Optional.of(new ParquetValueReaders.TimestampMillisReader(desc));
    }

    return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
  }

  @Override
  protected Optional<ParquetValueReader<?>> uuidReader(ColumnDescriptor desc) {
    return Optional.of(new ParquetValueReaders.UUIDReader(desc));
  }

  private static class StructLikeReader extends StructReader<StructLike, StructLike> {
    private final GenericRecord template;

    StructLikeReader(List<Type> types, List<ParquetValueReader<?>> readers, StructType struct) {
      super(types, readers);
      this.template = struct != null ? GenericRecord.create(struct) : null;
    }

    @Override
    protected StructLike newStructData(StructLike reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        return template.copy();
      }
    }

    @Override
    protected Object getField(StructLike intermediate, int pos) {
      return intermediate.get(pos, Object.class);
    }

    @Override
    protected StructLike buildStruct(StructLike struct) {
      return struct;
    }

    @Override
    protected void set(StructLike struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }
}
