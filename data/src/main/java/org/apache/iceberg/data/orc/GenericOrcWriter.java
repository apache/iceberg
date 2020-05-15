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

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.orc.ORCSchemaUtil;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
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

public class GenericOrcWriter implements OrcValueWriter<Record> {
  private final Converter[] converters;
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private GenericOrcWriter(TypeDescription schema) {
    this.converters = buildConverters(schema);
  }

  public static OrcValueWriter<Record> buildWriter(TypeDescription fileSchema) {
    return new GenericOrcWriter(fileSchema);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void write(Record value, VectorizedRowBatch output) throws IOException {
    int row = output.size++;
    for (int c = 0; c < converters.length; ++c) {
      converters[c].addValue(row, value.get(c, converters[c].getJavaClass()), output.cols[c]);
    }
  }

  /**
   * The interface for the conversion from Spark's SpecializedGetters to
   * ORC's ColumnVectors.
   */
  interface Converter<T> {

    Class<T> getJavaClass();

    /**
     * Take a value from the Spark data value and add it to the ORC output.
     * @param rowId the row in the ColumnVector
     * @param data either an InternalRow or ArrayData
     * @param output the ColumnVector to put the value into
     */
    void addValue(int rowId, T data, ColumnVector output);
  }

  static class BooleanConverter implements Converter<Boolean> {
    @Override
    public Class<Boolean> getJavaClass() {
      return Boolean.class;
    }

    @Override
    public void addValue(int rowId, Boolean data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data ? 1 : 0;
      }
    }
  }

  static class ByteConverter implements Converter<Byte> {
    @Override
    public Class<Byte> getJavaClass() {
      return Byte.class;
    }

    @Override
    public void addValue(int rowId, Byte data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class ShortConverter implements Converter<Short> {
    @Override
    public Class<Short> getJavaClass() {
      return Short.class;
    }

    @Override
    public void addValue(int rowId, Short data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class IntConverter implements Converter<Integer> {
    @Override
    public Class<Integer> getJavaClass() {
      return Integer.class;
    }

    @Override
    public void addValue(int rowId, Integer data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class TimeConverter implements Converter<LocalTime> {
    @Override
    public Class<LocalTime> getJavaClass() {
      return LocalTime.class;
    }

    @Override
    public void addValue(int rowId, LocalTime data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data.toNanoOfDay() / 1_000;
      }
    }
  }

  static class LongConverter implements Converter<Long> {
    @Override
    public Class<Long> getJavaClass() {
      return Long.class;
    }

    @Override
    public void addValue(int rowId, Long data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class FloatConverter implements Converter<Float> {
    @Override
    public Class<Float> getJavaClass() {
      return Float.class;
    }

    @Override
    public void addValue(int rowId, Float data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DoubleColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class DoubleConverter implements Converter<Double> {
    @Override
    public Class<Double> getJavaClass() {
      return Double.class;
    }

    @Override
    public void addValue(int rowId, Double data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DoubleColumnVector) output).vector[rowId] = data;
      }
    }
  }

  static class StringConverter implements Converter<String> {
    @Override
    public Class<String> getJavaClass() {
      return String.class;
    }

    @Override
    public void addValue(int rowId, String data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        byte[] value = data.getBytes(StandardCharsets.UTF_8);
        ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
      }
    }
  }

  static class BytesConverter implements Converter<ByteBuffer> {
    @Override
    public Class<ByteBuffer> getJavaClass() {
      return ByteBuffer.class;
    }

    @Override
    public void addValue(int rowId, ByteBuffer data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((BytesColumnVector) output).setRef(rowId, data.array(), 0, data.array().length);
      }
    }
  }

  static class UUIDConverter implements Converter<UUID> {
    @Override
    public Class<UUID> getJavaClass() {
      return UUID.class;
    }

    @Override
    public void addValue(int rowId, UUID data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(data.getMostSignificantBits());
        buffer.putLong(data.getLeastSignificantBits());
        ((BytesColumnVector) output).setRef(rowId, buffer.array(), 0, buffer.array().length);
      }
    }
  }

  static class FixedConverter implements Converter<byte[]> {
    @Override
    public Class<byte[]> getJavaClass() {
      return byte[].class;
    }

    @Override
    public void addValue(int rowId, byte[] data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((BytesColumnVector) output).setRef(rowId, data, 0, data.length);
      }
    }
  }

  static class DateConverter implements Converter<LocalDate> {
    @Override
    public Class<LocalDate> getJavaClass() {
      return LocalDate.class;
    }

    @Override
    public void addValue(int rowId, LocalDate data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((LongColumnVector) output).vector[rowId] = ChronoUnit.DAYS.between(EPOCH_DAY, data);
      }
    }
  }

  static class TimestampTzConverter implements Converter<OffsetDateTime> {
    @Override
    public Class<OffsetDateTime> getJavaClass() {
      return OffsetDateTime.class;
    }

    @Override
    public void addValue(int rowId, OffsetDateTime data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        TimestampColumnVector cv = (TimestampColumnVector) output;
        cv.time[rowId] = data.toInstant().toEpochMilli(); // millis
        cv.nanos[rowId] = (data.getNano() / 1_000) * 1_000; // truncate nanos to only keep microsecond precision
      }
    }
  }

  static class TimestampConverter implements Converter<LocalDateTime> {

    @Override
    public Class<LocalDateTime> getJavaClass() {
      return LocalDateTime.class;
    }

    @Override
    public void addValue(int rowId, LocalDateTime data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        TimestampColumnVector cv = (TimestampColumnVector) output;
        cv.setIsUTC(true);
        cv.time[rowId] = data.toInstant(ZoneOffset.UTC).toEpochMilli(); // millis
        cv.nanos[rowId] = (data.getNano() / 1_000) * 1_000; // truncate nanos to only keep microsecond precision
      }
    }
  }

  static class Decimal18Converter implements Converter<BigDecimal> {
    private final int scale;

    Decimal18Converter(TypeDescription schema) {
      this.scale = schema.getScale();
    }

    @Override
    public Class<BigDecimal> getJavaClass() {
      return BigDecimal.class;
    }

    @Override
    public void addValue(int rowId, BigDecimal data, ColumnVector output) {
      // TODO: validate precision and scale from schema
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DecimalColumnVector) output).vector[rowId]
            .setFromLongAndScale(data.unscaledValue().longValueExact(), scale);
      }
    }
  }

  static class Decimal38Converter implements Converter<BigDecimal> {
    Decimal38Converter(TypeDescription schema) {
    }

    @Override
    public Class<BigDecimal> getJavaClass() {
      return BigDecimal.class;
    }

    @Override
    public void addValue(int rowId, BigDecimal data, ColumnVector output) {
      // TODO: validate precision and scale from schema
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        ((DecimalColumnVector) output).vector[rowId].set(HiveDecimal.create(data, false));
      }
    }
  }

  static class StructConverter implements Converter<Record> {
    private final Converter[] children;

    StructConverter(TypeDescription schema) {
      this.children = new Converter[schema.getChildren().size()];
      for (int c = 0; c < children.length; ++c) {
        children[c] = buildConverter(schema.getChildren().get(c));
      }
    }

    @Override
    public Class<Record> getJavaClass() {
      return Record.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, Record data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        StructColumnVector cv = (StructColumnVector) output;
        for (int c = 0; c < children.length; ++c) {
          children[c].addValue(rowId, data.get(c, children[c].getJavaClass()), cv.fields[c]);
        }
      }
    }
  }

  static class ListConverter implements Converter<List> {
    private final Converter children;

    ListConverter(TypeDescription schema) {
      this.children = buildConverter(schema.getChildren().get(0));
    }

    @Override
    public Class<List> getJavaClass() {
      return List.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, List data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        List<Object> value = (List<Object>) data;
        ListColumnVector cv = (ListColumnVector) output;
        // record the length and start of the list elements
        cv.lengths[rowId] = value.size();
        cv.offsets[rowId] = cv.childCount;
        cv.childCount += cv.lengths[rowId];
        // make sure the child is big enough
        cv.child.ensureSize(cv.childCount, true);
        // Add each element
        for (int e = 0; e < cv.lengths[rowId]; ++e) {
          children.addValue((int) (e + cv.offsets[rowId]), value.get(e), cv.child);
        }
      }
    }
  }

  static class MapConverter implements Converter<Map> {
    private final Converter keyConverter;
    private final Converter valueConverter;

    MapConverter(TypeDescription schema) {
      this.keyConverter = buildConverter(schema.getChildren().get(0));
      this.valueConverter = buildConverter(schema.getChildren().get(1));
    }

    @Override
    public Class<Map> getJavaClass() {
      return Map.class;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addValue(int rowId, Map data, ColumnVector output) {
      if (data == null) {
        output.noNulls = false;
        output.isNull[rowId] = true;
      } else {
        output.isNull[rowId] = false;
        Map<Object, Object> map = (Map<Object, Object>) data;
        List<Object> keys = Lists.newArrayListWithExpectedSize(map.size());
        List<Object> values = Lists.newArrayListWithExpectedSize(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          keys.add(entry.getKey());
          values.add(entry.getValue());
        }
        MapColumnVector cv = (MapColumnVector) output;
        // record the length and start of the list elements
        cv.lengths[rowId] = map.size();
        cv.offsets[rowId] = cv.childCount;
        cv.childCount += cv.lengths[rowId];
        // make sure the child is big enough
        cv.keys.ensureSize(cv.childCount, true);
        cv.values.ensureSize(cv.childCount, true);
        // Add each element
        for (int e = 0; e < cv.lengths[rowId]; ++e) {
          int pos = (int) (e + cv.offsets[rowId]);
          keyConverter.addValue(pos, keys.get(e), cv.keys);
          valueConverter.addValue(pos, values.get(e), cv.values);
        }
      }
    }
  }

  private static Converter buildConverter(TypeDescription schema) {
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
            return new BytesConverter();
          default:
            throw new IllegalStateException("Unhandled Binary type found in ORC type attribute: " + binaryType);
        }
      case STRING:
      case CHAR:
      case VARCHAR:
        return new StringConverter();
      case DECIMAL:
        return schema.getPrecision() <= 18 ? new Decimal18Converter(schema) : new Decimal38Converter(schema);
      case TIMESTAMP:
        return new TimestampConverter();
      case TIMESTAMP_INSTANT:
        return new TimestampTzConverter();
      case STRUCT:
        return new StructConverter(schema);
      case LIST:
        return new ListConverter(schema);
      case MAP:
        return new MapConverter(schema);
    }
    throw new IllegalArgumentException("Unhandled type " + schema);
  }

  private static Converter[] buildConverters(TypeDescription schema) {
    if (schema.getCategory() != TypeDescription.Category.STRUCT) {
      throw new IllegalArgumentException("Top level must be a struct " + schema);
    }

    List<TypeDescription> children = schema.getChildren();
    Converter[] result = new Converter[children.size()];
    for (int c = 0; c < children.size(); ++c) {
      result[c] = buildConverter(children.get(c));
    }
    return result;
  }
}
