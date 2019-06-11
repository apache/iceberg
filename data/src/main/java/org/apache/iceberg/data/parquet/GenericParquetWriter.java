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

import com.google.common.collect.Lists;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetTypeVisitor;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.PrimitiveWriter;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class GenericParquetWriter {
  private GenericParquetWriter() {
  }

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
    public ParquetValueWriter<?> message(MessageType message,
                                         List<ParquetValueWriter<?>> fieldWriters) {
      return struct(message.asGroupType(), fieldWriters);
    }

    @Override
    public ParquetValueWriter<?> struct(GroupType struct,
                                        List<ParquetValueWriter<?>> fieldWriters) {
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

      org.apache.parquet.schema.Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()));

      return ParquetValueWriters.collections(repeatedD, repeatedR,
          ParquetValueWriters.option(elementType, elementD, elementWriter));
    }

    @Override
    public ParquetValueWriter<?> map(GroupType map,
                                     ParquetValueWriter<?> keyWriter,
                                     ParquetValueWriter<?> valueWriter) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath);
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath);

      org.apache.parquet.schema.Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()));
      org.apache.parquet.schema.Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()));

      return ParquetValueWriters.maps(repeatedD, repeatedR,
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
          case INT_8:
          case INT_16:
          case INT_32:
          case INT_64:
            return ParquetValueWriters.unboxed(desc);
          case DATE:
            return new DateWriter(desc);
          case TIME_MICROS:
            return new TimeWriter(desc);
          case TIMESTAMP_MICROS:
            return new TimestamptzWriter(desc);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case INT32:
                return ParquetValueWriters.decimalAsInteger(desc, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return ParquetValueWriters.decimalAsLong(desc, decimal.getPrecision(), decimal.getScale());
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return ParquetValueWriters.decimalAsFixed(desc, decimal.getPrecision(), decimal.getScale());
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
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
          return ParquetValueWriters.unboxed(desc);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }

    private String[] currentPath() {
      String[] path = new String[fieldNames.size()];
      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }

    private String[] path(String name) {
      String[] path = new String[fieldNames.size() + 1];
      path[fieldNames.size()] = name;

      if (!fieldNames.isEmpty()) {
        Iterator<String> iter = fieldNames.descendingIterator();
        for (int i = 0; iter.hasNext(); i += 1) {
          path[i] = iter.next();
        }
      }

      return path;
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateWriter extends PrimitiveWriter<LocalDate> {
    private DateWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDate value) {
      column.writeInteger(repetitionLevel, (int) ChronoUnit.DAYS.between(EPOCH_DAY, value));
    }
  }

  private static class TimeWriter extends PrimitiveWriter<LocalTime> {
    private TimeWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalTime value) {
      column.writeLong(repetitionLevel, value.toNanoOfDay() / 1000);
    }
  }

  private static class TimestampWriter extends PrimitiveWriter<LocalDateTime> {
    private TimestampWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDateTime value) {
      column.writeLong(repetitionLevel,
          ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class TimestamptzWriter extends PrimitiveWriter<OffsetDateTime> {
    private TimestamptzWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, OffsetDateTime value) {
      column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value));
    }
  }

  private static class FixedWriter extends PrimitiveWriter<byte[]> {
    private FixedWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, byte[] value) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(value));
    }
  }

  private static class RecordWriter extends StructWriter<Record> {
    private RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int index) {
      return struct.get(index);
    }
  }
}
