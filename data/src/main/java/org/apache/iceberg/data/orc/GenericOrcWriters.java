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

public class GenericOrcWriters {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private GenericOrcWriters() {
  }

  /**
   * The interface for the conversion from Spark's SpecializedGetters to
   * ORC's ColumnVectors.
   */
  interface Converter<T> {

    Class<T> getJavaClass();

    /**
     * Take a value from the Spark data value and add it to the ORC output.
     *
     * @param rowId  the row in the ColumnVector
     * @param data   either an InternalRow or ArrayData
     * @param output the ColumnVector to put the value into
     */
    void addValue(int rowId, T data, ColumnVector output);
  }

  public static Converter<Boolean> booleans() {
    return BooleanConverter.INSTANCE;
  }

  public static Converter<Byte> bytes() {
    return ByteConverter.INSTANCE;
  }

  public static Converter<Short> shorts() {
    return ShortConverter.INSTANCE;
  }

  public static Converter<Integer> ints() {
    return IntConverter.INSTANCE;
  }

  public static Converter<LocalTime> times() {
    return TimeConverter.INSTANCE;
  }

  public static Converter<Long> longs() {
    return LongConverter.INSTANCE;
  }

  public static Converter<Float> floats() {
    return FloatConverter.INSTANCE;
  }

  public static Converter<Double> doubles() {
    return DoubleConverter.INSTANCE;
  }

  public static Converter<String> strings() {
    return StringConverter.INSTANCE;
  }

  public static Converter<ByteBuffer> binary() {
    return BytesConverter.INSTANCE;
  }

  public static Converter<UUID> uuids() {
    return UUIDConverter.INSTANCE;
  }

  public static Converter<byte[]> fixed() {
    return FixedConverter.INSTANCE;
  }

  public static Converter<LocalDate> dates() {
    return DateConverter.INSTANCE;
  }

  public static Converter<OffsetDateTime> timestampTz() {
    return TimestampTzConverter.INSTANCE;
  }

  public static Converter<LocalDateTime> timestamp() {
    return TimestampConverter.INSTANCE;
  }

  public static Converter<BigDecimal> decimal18(TypeDescription schema) {
    return new Decimal18Converter(schema);
  }

  public static Converter<BigDecimal> decimal38(TypeDescription schema) {
    return Decimal38Converter.INSTANCE;
  }

  private static class BooleanConverter implements Converter<Boolean> {
    private static final Converter<Boolean> INSTANCE = new BooleanConverter();

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

  private static class ByteConverter implements Converter<Byte> {
    private static final Converter<Byte> INSTANCE = new ByteConverter();

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

  private static class ShortConverter implements Converter<Short> {
    private static final Converter<Short> INSTANCE = new ShortConverter();

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

  private static class IntConverter implements Converter<Integer> {
    private static final Converter<Integer> INSTANCE = new IntConverter();

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

  private static class TimeConverter implements Converter<LocalTime> {
    private static final Converter<LocalTime> INSTANCE = new TimeConverter();

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

  private static class LongConverter implements Converter<Long> {
    private static final Converter<Long> INSTANCE = new LongConverter();

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

  private static class FloatConverter implements Converter<Float> {
    private static final Converter<Float> INSTANCE = new FloatConverter();

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

  private static class DoubleConverter implements Converter<Double> {
    private static final Converter<Double> INSTANCE = new DoubleConverter();

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

  private static class StringConverter implements Converter<String> {
    private static final Converter<String> INSTANCE = new StringConverter();

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

  private static class BytesConverter implements Converter<ByteBuffer> {
    private static final Converter<ByteBuffer> INSTANCE = new BytesConverter();

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

  private static class UUIDConverter implements Converter<UUID> {
    private static final Converter<UUID> INSTANCE = new UUIDConverter();

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

  private static class FixedConverter implements Converter<byte[]> {
    private static final Converter<byte[]> INSTANCE = new FixedConverter();

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

  private static class DateConverter implements Converter<LocalDate> {
    private static final Converter<LocalDate> INSTANCE = new DateConverter();

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

  private static class TimestampTzConverter implements Converter<OffsetDateTime> {
    private static final Converter<OffsetDateTime> INSTANCE = new TimestampTzConverter();

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

  private static class TimestampConverter implements Converter<LocalDateTime> {
    private static final Converter<LocalDateTime> INSTANCE = new TimestampConverter();

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

  private static class Decimal18Converter implements Converter<BigDecimal> {
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

  private static class Decimal38Converter implements Converter<BigDecimal> {
    private static final Converter<BigDecimal> INSTANCE = new Decimal38Converter();

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

  public static class RecordConverter implements Converter<Record> {
    private final List<Converter> converters;

    RecordConverter(List<Converter> converters) {
      this.converters = converters;
    }

    public List<Converter> converters() {
      return converters;
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
        for (int c = 0; c < converters.size(); ++c) {
          converters.get(c).addValue(rowId, data.get(c, converters.get(c).getJavaClass()), cv.fields[c]);
        }
      }
    }
  }

  public static class ListConverter implements Converter<List> {
    private final Converter children;

    ListConverter(Converter children) {
      this.children = children;
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

  public static class MapConverter implements Converter<Map> {
    private final Converter keyConverter;
    private final Converter valueConverter;

    MapConverter(Converter keyConverter, Converter valueConverter) {
      this.keyConverter = keyConverter;
      this.valueConverter = valueConverter;
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
}
