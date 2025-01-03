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
import org.apache.parquet.schema.PrimitiveType;
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
    return new ParquetStructReader(types, fieldReaders, structType);
  }

  @Override
  protected LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ParquetValueReader<?>>
      logicalTypeReaderVisitor(
          ColumnDescriptor desc,
          org.apache.iceberg.types.Type.PrimitiveType expected,
          PrimitiveType primitive) {
    return new LogicalTypeAnnotationParquetValueReaderVisitor(desc, expected, primitive);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> fixedReader(ColumnDescriptor desc) {
    return new ParquetValueReaders.BytesReader(desc);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> int96Reader(ColumnDescriptor desc) {
    // normal handling as int96
    return new ParquetValueReaders.UnboxedReader<>(desc);
  }

  private static class ParquetStructReader extends StructReader<StructLike, StructLike> {
    private final GenericRecord template;

    ParquetStructReader(List<Type> types, List<ParquetValueReader<?>> readers, StructType struct) {
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

  private static class LogicalTypeAnnotationParquetValueReaderVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ParquetValueReader<?>> {

    private final ColumnDescriptor desc;
    private final org.apache.iceberg.types.Type.PrimitiveType expected;
    private final PrimitiveType primitive;

    LogicalTypeAnnotationParquetValueReaderVisitor(
        ColumnDescriptor desc,
        org.apache.iceberg.types.Type.PrimitiveType expected,
        PrimitiveType primitive) {
      this.desc = desc;
      this.expected = expected;
      this.primitive = primitive;
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
      return Optional.of(new ParquetValueReaders.UUIDReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return Optional.of(
              new ParquetValueReaders.BinaryAsDecimalReader(desc, decimalLogicalType.getScale()));
        case INT64:
          return Optional.of(
              new ParquetValueReaders.LongAsDecimalReader(desc, decimalLogicalType.getScale()));
        case INT32:
          return Optional.of(
              new ParquetValueReaders.IntegerAsDecimalReader(desc, decimalLogicalType.getScale()));
        default:
          throw new UnsupportedOperationException(
              "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
      }
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      if (intLogicalType.getBitWidth() == 64) {
        return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
      }
      return (expected.typeId() == org.apache.iceberg.types.Type.TypeID.LONG)
          ? Optional.of(new ParquetValueReaders.IntAsLongReader(desc))
          : Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return Optional.of(new ParquetValueReaders.BytesReader(desc));
    }
  }
}
