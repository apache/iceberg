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
package org.apache.iceberg.parquet;

import java.util.List;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.iceberg.parquet.ParquetValueWriters.PrimitiveWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class ParquetAvroWriter {
  private ParquetAvroWriter() {}

  @SuppressWarnings("unchecked")
  public static <T> ParquetValueWriter<T> buildWriter(MessageType type) {
    return (ParquetValueWriter<T>) ParquetTypeVisitor.visit(type, new WriteBuilder(type));
  }

  private static class WriteBuilder extends ParquetTypeVisitor<ParquetValueWriter<?>> {
    private final MessageType type;

    WriteBuilder(MessageType type) {
      this.type = type;
    }

    @Override
    public ParquetValueWriter<?> message(
        MessageType message, List<ParquetValueWriter<?>> fieldWriters) {
      return struct(message.asGroupType(), fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(
        GroupType struct, List<ParquetValueWriter<?>> fieldWriters) {
      List<Type> fields = struct.getFields();
      List<ParquetValueWriter<?>> writers = Lists.newArrayListWithExpectedSize(fieldWriters.size());
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = struct.getType(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()));
        writers.add(ParquetValueWriters.option(fieldType, fieldD, fieldWriters.get(i)));
      }

      return new RecordWriter(writers);
    }

    @Override
    public ParquetValueWriter<?> list(GroupType array, ParquetValueWriter<?> elementWriter) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return ParquetValueWriters.collections(
          repeatedD, repeatedR, ParquetValueWriters.option(elementType, elementD, elementWriter));
    }

    @Override
    public ParquetValueWriter<?> map(
        GroupType map, ParquetValueWriter<?> keyWriter, ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return ParquetValueWriters.maps(
          repeatedD,
          repeatedR,
          ParquetValueWriters.option(keyType, keyD, keyWriter),
          ParquetValueWriters.option(valueType, valueD, valueWriter));
    }

    @Override
    public ParquetValueWriter<?> primitive(PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return ParquetValueWriters.strings(desc);
          case DATE:
          case INT_8:
          case INT_16:
          case INT_32:
            return ParquetValueWriters.ints(desc);
          case INT_64:
          case TIME_MICROS:
          case TIMESTAMP_MICROS:
            return ParquetValueWriters.longs(desc);
          case DECIMAL:
            DecimalLogicalTypeAnnotation decimal =
                (DecimalLogicalTypeAnnotation) primitive.getLogicalTypeAnnotation();
            switch (primitive.getPrimitiveTypeName()) {
              case INT32:
                return ParquetValueWriters.decimalAsInteger(
                    desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return ParquetValueWriters.decimalAsLong(
                    desc, decimal.getPrecision(), decimal.getScale());
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return ParquetValueWriters.decimalAsFixed(
                    desc, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return ParquetValueWriters.byteBuffers(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedWriter(desc);
        case BINARY:
          return ParquetValueWriters.byteBuffers(desc);
        case BOOLEAN:
          return ParquetValueWriters.booleans(desc);
        case INT32:
          return ParquetValueWriters.ints(desc);
        case INT64:
          return ParquetValueWriters.longs(desc);
        case FLOAT:
          return ParquetValueWriters.floats(desc);
        case DOUBLE:
          return ParquetValueWriters.doubles(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  private static class FixedWriter extends PrimitiveWriter<Fixed> {
    private FixedWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Fixed buffer) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(buffer.bytes()));
    }
  }

  private static class RecordWriter extends StructWriter<IndexedRecord> {
    private RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(IndexedRecord struct, int index) {
      return struct.get(index);
    }
  }
}
