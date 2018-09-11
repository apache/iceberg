/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.data.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.data.GenericRecord;
import com.netflix.iceberg.data.Record;
import com.netflix.iceberg.parquet.ParquetValueReader;
import com.netflix.iceberg.parquet.ParquetValueReaders;
import com.netflix.iceberg.parquet.ParquetValueReaders.BinaryAsDecimalReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.BytesReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.IntAsLongReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.IntegerAsDecimalReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.ListReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.LongAsDecimalReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.MapReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.PrimitiveReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.StringReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.StructReader;
import com.netflix.iceberg.parquet.ParquetValueReaders.UnboxedReader;
import com.netflix.iceberg.parquet.TypeWithSchemaVisitor;
import com.netflix.iceberg.types.Type.TypeID;
import com.netflix.iceberg.types.Types;
import com.netflix.iceberg.types.Types.StructType;
import com.netflix.iceberg.types.Types.TimestampType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.netflix.iceberg.parquet.ParquetSchemaUtil.hasIds;
import static com.netflix.iceberg.parquet.ParquetValueReaders.option;

public class GenericParquetReaders {
  private GenericParquetReaders() {
  }

  @SuppressWarnings("unchecked")
  public static ParquetValueReader<GenericRecord> buildReader(Schema expectedSchema,
                                                              MessageType fileSchema) {
    if (hasIds(fileSchema)) {
      return (ParquetValueReader<GenericRecord>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new ReadBuilder(fileSchema));
    } else {
      return (ParquetValueReader<GenericRecord>)
          TypeWithSchemaVisitor.visit(expectedSchema.asStruct(), fileSchema,
              new FallbackReadBuilder(fileSchema));
    }
  }

  private static class FallbackReadBuilder extends ReadBuilder {
    FallbackReadBuilder(MessageType type) {
      super(type);
    }

    @Override
    public ParquetValueReader<?> message(StructType expected, MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      // the top level matches by ID, but the remaining IDs are missing
      return super.struct(expected, message, fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(StructType expected, GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      // the expected struct is ignored because nested fields are never found when the
      List<ParquetValueReader<?>> newFields = Lists.newArrayListWithExpectedSize(
          fieldReaders.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(fieldReaders.size());
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()))-1;
        newFields.add(option(fieldType, fieldD, fieldReaders.get(i)));
        types.add(fieldType);
      }

      return new RecordReader(types, newFields, expected);
    }
  }

  private static class ReadBuilder extends TypeWithSchemaVisitor<ParquetValueReader<?>> {
    final MessageType type;

    ReadBuilder(MessageType type) {
      this.type = type;
    }

    @Override
    public ParquetValueReader<?> message(StructType expected, MessageType message,
                                         List<ParquetValueReader<?>> fieldReaders) {
      return struct(expected, message.asGroupType(), fieldReaders);
    }

    @Override
    public ParquetValueReader<?> struct(StructType expected, GroupType struct,
                                        List<ParquetValueReader<?>> fieldReaders) {
      // match the expected struct's order
      Map<Integer, ParquetValueReader<?>> readersById = Maps.newHashMap();
      Map<Integer, Type> typesById = Maps.newHashMap();
      List<Type> fields = struct.getFields();
      for (int i = 0; i < fields.size(); i += 1) {
        Type fieldType = fields.get(i);
        int fieldD = type.getMaxDefinitionLevel(path(fieldType.getName()))-1;
        int id = fieldType.getId().intValue();
        readersById.put(id, option(fieldType, fieldD, fieldReaders.get(i)));
        typesById.put(id, fieldType);
      }

      List<Types.NestedField> expectedFields = expected != null ?
          expected.fields() : ImmutableList.of();
      List<ParquetValueReader<?>> reorderedFields = Lists.newArrayListWithExpectedSize(
          expectedFields.size());
      List<Type> types = Lists.newArrayListWithExpectedSize(expectedFields.size());
      for (Types.NestedField field : expectedFields) {
        int id = field.fieldId();
        ParquetValueReader<?> reader = readersById.get(id);
        if (reader != null) {
          reorderedFields.add(reader);
          types.add(typesById.get(id));
        } else {
          reorderedFields.add(ParquetValueReaders.nulls());
          types.add(null);
        }
      }

      return new RecordReader(types, reorderedFields, expected);
    }

    @Override
    public ParquetValueReader<?> list(Types.ListType expectedList, GroupType array,
                                      ParquetValueReader<?> elementReader) {
      GroupType repeated = array.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath)-1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath)-1;

      Type elementType = repeated.getType(0);
      int elementD = type.getMaxDefinitionLevel(path(elementType.getName()))-1;

      return new ListReader<>(repeatedD, repeatedR, option(elementType, elementD, elementReader));
    }

    @Override
    public ParquetValueReader<?> map(Types.MapType expectedMap, GroupType map,
                                     ParquetValueReader<?> keyReader,
                                     ParquetValueReader<?> valueReader) {
      GroupType repeatedKeyValue = map.getFields().get(0).asGroupType();
      String[] repeatedPath = currentPath();

      int repeatedD = type.getMaxDefinitionLevel(repeatedPath)-1;
      int repeatedR = type.getMaxRepetitionLevel(repeatedPath)-1;

      Type keyType = repeatedKeyValue.getType(0);
      int keyD = type.getMaxDefinitionLevel(path(keyType.getName()))-1;
      Type valueType = repeatedKeyValue.getType(1);
      int valueD = type.getMaxDefinitionLevel(path(valueType.getName()))-1;

      return new MapReader<>(repeatedD, repeatedR,
          option(keyType, keyD, keyReader), option(valueType, valueD, valueReader));
    }

    @Override
    public ParquetValueReader<?> primitive(com.netflix.iceberg.types.Type.PrimitiveType expected,
                                           PrimitiveType primitive) {
      ColumnDescriptor desc = type.getColumnDescription(currentPath());

      if (primitive.getOriginalType() != null) {
        switch (primitive.getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
            return new StringReader(desc);
          case INT_8:
          case INT_16:
          case INT_32:
            if (expected.typeId() == TypeID.LONG) {
              return new IntAsLongReader(desc);
            } else {
              return new UnboxedReader<>(desc);
            }
          case INT_64:
            return new UnboxedReader<>(desc);
          case DATE:
            return new DateReader(desc);
          case TIMESTAMP_MICROS:
            TimestampType tsMicrosType = (TimestampType) expected;
            if (tsMicrosType.shouldAdjustToUTC()) {
              return new TimestamptzReader(desc);
            } else {
              return new TimestampReader(desc);
            }
          case TIMESTAMP_MILLIS:
            TimestampType tsMillisType = (TimestampType) expected;
            if (tsMillisType.shouldAdjustToUTC()) {
              return new TimestamptzMillisReader(desc);
            } else {
              return new TimestampMillisReader(desc);
            }
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new BinaryAsDecimalReader(desc, decimal.getScale());
              case INT64:
                return new LongAsDecimalReader(desc, decimal.getScale());
              case INT32:
                return new IntegerAsDecimalReader(desc, decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                    "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          case BSON:
            return new BytesReader(desc);
          default:
            throw new UnsupportedOperationException(
                "Unsupported logical type: " + primitive.getOriginalType());
        }
      }

      switch (primitive.getPrimitiveTypeName()) {
        case FIXED_LEN_BYTE_ARRAY:
          return new FixedReader(desc);
        case BINARY:
          return new BytesReader(desc);
        case INT32:
          if (expected != null && expected.typeId() == TypeID.LONG) {
            return new IntAsLongReader(desc);
          } else {
            return new UnboxedReader<>(desc);
          }
        case FLOAT:
          if (expected != null && expected.typeId() == TypeID.DOUBLE) {
            return new ParquetValueReaders.FloatAsDoubleReader(desc);
          } else {
            return new UnboxedReader<>(desc);
          }
        case BOOLEAN:
        case INT64:
        case DOUBLE:
          return new UnboxedReader<>(desc);
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

    protected String[] path(String name) {
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

  private static class DateReader extends PrimitiveReader<LocalDate> {
    private DateReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDate read(LocalDate reuse) {
      return EPOCH_DAY.plusDays(column.nextInteger());
    }
  }

  private static class TimestampReader extends PrimitiveReader<LocalDateTime> {
    private TimestampReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDateTime read(LocalDateTime reuse) {
      return EPOCH.plus(column.nextLong(), ChronoUnit.MICROS).toLocalDateTime();
    }
  }

  private static class TimestampMillisReader extends PrimitiveReader<LocalDateTime> {
    private TimestampMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDateTime read(LocalDateTime reuse) {
      return EPOCH.plus(column.nextLong() * 1000, ChronoUnit.MICROS).toLocalDateTime();
    }
  }

  private static class TimestamptzReader extends PrimitiveReader<OffsetDateTime> {
    private TimestamptzReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong(), ChronoUnit.MICROS);
    }
  }

  private static class TimestamptzMillisReader extends PrimitiveReader<OffsetDateTime> {
    private TimestamptzMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong() * 1000, ChronoUnit.MICROS);
    }
  }

  private static class FixedReader extends PrimitiveReader<byte[]> {
    private FixedReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public byte[] read(byte[] reuse) {
      if (reuse != null) {
        column.nextBinary().toByteBuffer().duplicate().get(reuse);
        return reuse;
      } else {
        return column.nextBinary().getBytes();
      }
    }
  }

  static class RecordReader extends StructReader<Record, Record> {
    private final StructType struct;

    RecordReader(List<Type> types,
                 List<ParquetValueReader<?>> readers,
                 StructType struct) {
      super(types, readers);
      this.struct = struct;
    }

    @Override
    protected Record newStructData(Record reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        return GenericRecord.create(struct);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object getField(Record intermediate, int pos) {
      return intermediate.get(pos);
    }

    @Override
    protected Record buildStruct(Record struct) {
      return struct;
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }
}
