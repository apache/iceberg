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
import java.util.Optional;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;

/**
 * A Writer that consumes Iceberg's internal in-memory object model.
 *
 * <p>Iceberg's internal in-memory object model produces the types defined in {@link
 * Type.TypeID#javaClass()}.
 */
public class InternalWriter extends BaseParquetWriter<StructLike> {
  private static final InternalWriter INSTANCE = new InternalWriter();

  private InternalWriter() {}

  public static ParquetValueWriter<StructLike> buildWriter(MessageType type) {
    return INSTANCE.createWriter(type);
  }

  @Override
  protected StructWriter<StructLike> createStructWriter(List<ParquetValueWriter<?>> writers) {
    return new ParquetStructWriter(writers);
  }

  @Override
  protected LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<
          ParquetValueWriters.PrimitiveWriter<?>>
      logicalTypeWriterVisitor(ColumnDescriptor desc) {
    return new LogicalTypeWriterVisitor(desc);
  }

  @Override
  protected ParquetValueWriters.PrimitiveWriter<?> fixedWriter(ColumnDescriptor desc) {
    // accepts ByteBuffer and internally writes as binary.
    return ParquetValueWriters.byteBuffers(desc);
  }

  private static class ParquetStructWriter extends StructWriter<StructLike> {
    private ParquetStructWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(StructLike struct, int index) {
      return struct.get(index, Object.class);
    }
  }

  private static class LogicalTypeWriterVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<
          ParquetValueWriters.PrimitiveWriter<?>> {
    private final ColumnDescriptor desc;

    private LogicalTypeWriterVisitor(ColumnDescriptor desc) {
      this.desc = desc;
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalType) {
      switch (desc.getPrimitiveType().getPrimitiveTypeName()) {
        case INT32:
          return Optional.of(
              ParquetValueWriters.decimalAsInteger(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
        case INT64:
          return Optional.of(
              ParquetValueWriters.decimalAsLong(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return Optional.of(
              ParquetValueWriters.decimalAsFixed(
                  desc, decimalType.getPrecision(), decimalType.getScale()));
      }
      return Optional.empty();
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intType) {
      Preconditions.checkArgument(
          intType.isSigned() || intType.getBitWidth() < 64,
          "Cannot read uint64: not a supported Java type");
      if (intType.getBitWidth() < 64) {
        return Optional.of(ParquetValueWriters.ints(desc));
      } else {
        return Optional.of(ParquetValueWriters.longs(desc));
      }
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return Optional.of(ParquetValueWriters.strings(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonType) {
      return Optional.of(ParquetValueWriters.byteBuffers(desc));
    }

    @Override
    public Optional<ParquetValueWriters.PrimitiveWriter<?>> visit(
        LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
      return Optional.of(ParquetValueWriters.uuids(desc));
    }
  }
}
