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

package org.apache.iceberg.data.orc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.StructColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;

/**
 * ORC reader for Generic Record.
 */
public class GenericOrcReader implements OrcValueReader<Record> {

  private final Schema schema;
  private final List<TypeDescription> columns;
  private final Converter[] converters;

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private GenericOrcReader(Schema expectedSchema, TypeDescription readSchema) {
    this.schema = expectedSchema;
    this.columns = readSchema.getChildren();
    this.converters = buildConverters();
  }

  private Converter[] buildConverters() {
    Preconditions.checkState(schema.columns().size() == columns.size(),
        "Expected schema must have same number of columns as projection.");
    Converter[] newConverters = new Converter[columns.size()];
    List<Types.NestedField> icebergCols = schema.columns();
    for (int c = 0; c < newConverters.length; ++c) {
      newConverters[c] = buildConverter(icebergCols.get(c), columns.get(c));
    }
    return newConverters;
  }

  public static OrcValueReader<Record> buildReader(Schema expectedSchema, TypeDescription fileSchema) {
    return new GenericOrcReader(expectedSchema, fileSchema);
  }

  @Override
  public Record read(VectorizedRowBatch batch, int row) {
    Record rowRecord = GenericRecord.create(schema);
    for (int c = 0; c < batch.cols.length; ++c) {
      rowRecord.set(c, converters[c].convert(batch.cols[c], row));
    }
    return rowRecord;
  }

  interface Converter<T> {
    T convert(ColumnVector vector, int row);
  }

  private static class BooleanConverter implements Converter<Boolean> {
    @Override
    public Boolean convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return ((LongColumnVector) vector).vector[rowIndex] != 0;
      }
    }
  }

  private static class ByteConverter implements Converter<Byte> {
    @Override
    public Byte convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return (byte) ((LongColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class ShortConverter implements Converter<Short> {
    @Override
    public Short convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return (short) ((LongColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class IntConverter implements Converter<Integer> {
    @Override
    public Integer convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return (int) ((LongColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class TimeConverter implements Converter<LocalTime> {
    @Override
    public LocalTime convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return LocalTime.ofNanoOfDay(((LongColumnVector) vector).vector[rowIndex] * 1_000);
      }
    }
  }

  private static class DateConverter implements Converter<LocalDate> {
    @Override
    public LocalDate convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return EPOCH_DAY.plusDays((int) ((LongColumnVector) vector).vector[rowIndex]);
      }
    }
  }

  private static class LongConverter implements Converter<Long> {
    @Override
    public Long convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return ((LongColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class FloatConverter implements Converter<Float> {
    @Override
    public Float convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return (float) ((DoubleColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class DoubleConverter implements Converter<Double> {
    @Override
    public Double convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return ((DoubleColumnVector) vector).vector[rowIndex];
      }
    }
  }

  private static class TimestampTzConverter implements Converter<OffsetDateTime> {
    private OffsetDateTime convert(TimestampColumnVector vector, int row) {
      return Instant.ofEpochSecond(Math.floorDiv(vector.time[row], 1_000), vector.nanos[row]).atOffset(ZoneOffset.UTC);
    }

    @Override
    public OffsetDateTime convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return convert((TimestampColumnVector) vector, rowIndex);
      }
    }
  }

  private static class TimestampConverter implements Converter<LocalDateTime> {

    private LocalDateTime convert(TimestampColumnVector vector, int row) {
      return Instant.ofEpochSecond(Math.floorDiv(vector.time[row], 1_000), vector.nanos[row]).atOffset(ZoneOffset.UTC)
          .toLocalDateTime();
    }

    @Override
    public LocalDateTime convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return convert((TimestampColumnVector) vector, rowIndex);
      }
    }
  }

  private static class FixedConverter implements Converter<byte[]> {
    @Override
    public byte[] convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        BytesColumnVector bytesVector = (BytesColumnVector) vector;
        return Arrays.copyOfRange(bytesVector.vector[rowIndex], bytesVector.start[rowIndex],
            bytesVector.start[rowIndex] + bytesVector.length[rowIndex]);
      }
    }
  }

  private static class BinaryConverter implements Converter<ByteBuffer> {
    @Override
    public ByteBuffer convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        BytesColumnVector bytesVector = (BytesColumnVector) vector;
        return ByteBuffer.wrap(bytesVector.vector[rowIndex], bytesVector.start[rowIndex], bytesVector.length[rowIndex]);
      }
    }
  }

  private static class UUIDConverter implements Converter<UUID> {
    @Override
    public UUID convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        BytesColumnVector bytesVector = (BytesColumnVector) vector;
        ByteBuffer buf = ByteBuffer.wrap(bytesVector.vector[rowIndex], bytesVector.start[rowIndex],
            bytesVector.length[rowIndex]);
        long mostSigBits = buf.getLong();
        long leastSigBits = buf.getLong();
        return new UUID(mostSigBits, leastSigBits);
      }
    }
  }

  private static class StringConverter implements Converter<String> {
    @Override
    public String convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        BytesColumnVector bytesVector = (BytesColumnVector) vector;
        return new String(bytesVector.vector[rowIndex], bytesVector.start[rowIndex], bytesVector.length[rowIndex],
            StandardCharsets.UTF_8);
      }
    }
  }

  private static class DecimalConverter implements Converter<BigDecimal> {
    @Override
    public BigDecimal convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        DecimalColumnVector cv = (DecimalColumnVector) vector;
        return cv.vector[rowIndex].getHiveDecimal().bigDecimalValue().setScale(cv.scale);
      }
    }
  }

  private static class ListConverter implements Converter<List<?>> {
    private final Converter childConverter;

    ListConverter(Types.NestedField icebergField, TypeDescription schema) {
      Preconditions.checkArgument(icebergField.type().isListType());
      TypeDescription child = schema.getChildren().get(0);

      childConverter = buildConverter(icebergField
          .type()
          .asListType()
          .fields()
          .get(0), child);
    }

    List<?> readList(ListColumnVector vector, int row) {
      int offset = (int) vector.offsets[row];
      int length = (int) vector.lengths[row];

      List<Object> list = Lists.newArrayListWithExpectedSize(length);
      for (int c = 0; c < length; ++c) {
        list.add(childConverter.convert(vector.child, offset + c));
      }
      return list;
    }

    @Override
    public List<?> convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return readList((ListColumnVector) vector, rowIndex);
      }
    }
  }

  private static class MapConverter implements Converter {
    private final Converter keyConvert;
    private final Converter valueConvert;

    MapConverter(Types.NestedField icebergField, TypeDescription schema) {
      Preconditions.checkArgument(icebergField.type().isMapType());
      TypeDescription keyType = schema.getChildren().get(0);
      TypeDescription valueType = schema.getChildren().get(1);
      List<Types.NestedField> mapFields = icebergField.type().asMapType().fields();

      keyConvert = buildConverter(mapFields.get(0), keyType);
      valueConvert = buildConverter(mapFields.get(1), valueType);
    }

    Map<?, ?> readMap(MapColumnVector vector, int row) {
      final int offset = (int) vector.offsets[row];
      final int length = (int) vector.lengths[row];

      Map<Object, Object> map = Maps.newHashMapWithExpectedSize(length);
      for (int c = 0; c < length; ++c) {
        Object key = keyConvert.convert(vector.keys, offset + c);
        Object value = valueConvert.convert(vector.values, offset + c);
        map.put(key, value);
      }

      return map;
    }

    @Override
    public Map convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return readMap((MapColumnVector) vector, rowIndex);
      }
    }
  }

  private static class StructConverter implements Converter<Record> {
    private final Converter[] children;
    private final Schema icebergStructSchema;

    StructConverter(final Types.NestedField icebergField, final TypeDescription schema) {
      Preconditions.checkArgument(icebergField.type().isStructType());
      icebergStructSchema = new Schema(icebergField.type().asStructType().fields());
      List<Types.NestedField> icebergChildren = icebergField.type().asStructType().fields();
      children = new Converter[schema.getChildren().size()];

      Preconditions.checkState(icebergChildren.size() == children.length,
          "Expected schema must have same number of columns as projection.");
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(icebergChildren.get(c), schema.getChildren().get(c));
      }
    }

    Record writeStruct(StructColumnVector vector, int row) {
      Record data = GenericRecord.create(icebergStructSchema);
      for (int c = 0; c < children.length; ++c) {
        data.set(c, children[c].convert(vector.fields[c], row));
      }
      return data;
    }

    @Override
    public Record convert(ColumnVector vector, int row) {
      int rowIndex = vector.isRepeating ? 0 : row;
      if (!vector.noNulls && vector.isNull[rowIndex]) {
        return null;
      } else {
        return writeStruct((StructColumnVector) vector, rowIndex);
      }
    }
  }

  private static Converter buildConverter(final Types.NestedField icebergField, final TypeDescription schema) {
    switch (schema.getCategory()) {
      case BOOLEAN:
        return new BooleanConverter();
      case BYTE:
        return new ByteConverter();
      case SHORT:
        return new ShortConverter();
      case DATE:
        return new DateConverter();
      case INT:
        return new IntConverter();
      case LONG:
        String longAttributeValue = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_LONG_TYPE_ATTRIBUTE);
        ORCSchemaUtil.LongType longType = longAttributeValue == null ? ORCSchemaUtil.LongType.LONG :
            ORCSchemaUtil.LongType.valueOf(longAttributeValue);
        switch (longType) {
          case TIME:
            return new TimeConverter();
          case LONG:
            return new LongConverter();
          default:
            throw new IllegalStateException("Unhandled Long type found in ORC type attribute: " + longType);
        }
      case FLOAT:
        return new FloatConverter();
      case DOUBLE:
        return new DoubleConverter();
      case TIMESTAMP:
        return new TimestampConverter();
      case TIMESTAMP_INSTANT:
        return new TimestampTzConverter();
      case DECIMAL:
        return new DecimalConverter();
      case BINARY:
        String binaryAttributeValue = schema.getAttributeValue(ORCSchemaUtil.ICEBERG_BINARY_TYPE_ATTRIBUTE);
        ORCSchemaUtil.BinaryType binaryType = binaryAttributeValue == null ? ORCSchemaUtil.BinaryType.BINARY :
            ORCSchemaUtil.BinaryType.valueOf(binaryAttributeValue);
        switch (binaryType) {
          case UUID:
            return new UUIDConverter();
          case FIXED:
            return new FixedConverter();
          case BINARY:
            return new BinaryConverter();
          default:
            throw new IllegalStateException("Unhandled Binary type found in ORC type attribute: " + binaryType);
        }
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringConverter();
      case STRUCT:
        return new StructConverter(icebergField, schema);
      case LIST:
        return new ListConverter(icebergField, schema);
      case MAP:
        return new MapConverter(icebergField, schema);
      default:
        throw new IllegalArgumentException("Unhandled type " + schema);
    }
  }
}
