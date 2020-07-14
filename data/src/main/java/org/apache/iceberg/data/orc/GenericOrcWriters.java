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
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;

public class GenericOrcWriters {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private GenericOrcWriters() {
  }

  public static OrcValueWriter<Boolean> booleans() {
    return BooleanOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Byte> bytes() {
    return ByteOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Short> shorts() {
    return ShortOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Integer> ints() {
    return IntOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalTime> times() {
    return TimeOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Long> longs() {
    return LongOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Float> floats() {
    return FloatOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<Double> doubles() {
    return DoubleOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<String> strings() {
    return StringOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<ByteBuffer> binary() {
    return BytesOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<UUID> uuids() {
    return UUIDOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<byte[]> fixed() {
    return FixedOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalDate> dates() {
    return DateOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<OffsetDateTime> timestampTz() {
    return TimestampTzOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalDateTime> timestamp() {
    return TimestampOrcValueWriter.INSTANCE;
  }

  public static OrcValueWriter<BigDecimal> decimal(int scala, int precision) {
    if (precision <= 18) {
      return new Decimal18OrcValueWriter(scala);
    } else {
      return Decimal38OrcValueWriter.INSTANCE;
    }
  }

  public static OrcValueWriter<List> list(OrcValueWriter element) {
    return new ListOrcValueWriter(element);
  }

  public static OrcValueWriter<Map> map(OrcValueWriter key, OrcValueWriter value) {
    return new MapOrcValueWriter(key, value);
  }

  private static class BooleanOrcValueWriter implements OrcValueWriter<Boolean> {
    private static final OrcValueWriter<Boolean> INSTANCE = new BooleanOrcValueWriter();

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

  private static class ByteOrcValueWriter implements OrcValueWriter<Byte> {
    private static final OrcValueWriter<Byte> INSTANCE = new ByteOrcValueWriter();

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

  private static class ShortOrcValueWriter implements OrcValueWriter<Short> {
    private static final OrcValueWriter<Short> INSTANCE = new ShortOrcValueWriter();

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

  private static class IntOrcValueWriter implements OrcValueWriter<Integer> {
    private static final OrcValueWriter<Integer> INSTANCE = new IntOrcValueWriter();

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

  private static class TimeOrcValueWriter implements OrcValueWriter<LocalTime> {
    private static final OrcValueWriter<LocalTime> INSTANCE = new TimeOrcValueWriter();

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

  private static class LongOrcValueWriter implements OrcValueWriter<Long> {
    private static final OrcValueWriter<Long> INSTANCE = new LongOrcValueWriter();

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

  private static class FloatOrcValueWriter implements OrcValueWriter<Float> {
    private static final OrcValueWriter<Float> INSTANCE = new FloatOrcValueWriter();

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

  private static class DoubleOrcValueWriter implements OrcValueWriter<Double> {
    private static final OrcValueWriter<Double> INSTANCE = new DoubleOrcValueWriter();

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

  private static class StringOrcValueWriter implements OrcValueWriter<String> {
    private static final OrcValueWriter<String> INSTANCE = new StringOrcValueWriter();

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

  private static class BytesOrcValueWriter implements OrcValueWriter<ByteBuffer> {
    private static final OrcValueWriter<ByteBuffer> INSTANCE = new BytesOrcValueWriter();

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

  private static class UUIDOrcValueWriter implements OrcValueWriter<UUID> {
    private static final OrcValueWriter<UUID> INSTANCE = new UUIDOrcValueWriter();

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

  private static class FixedOrcValueWriter implements OrcValueWriter<byte[]> {
    private static final OrcValueWriter<byte[]> INSTANCE = new FixedOrcValueWriter();

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

  private static class DateOrcValueWriter implements OrcValueWriter<LocalDate> {
    private static final OrcValueWriter<LocalDate> INSTANCE = new DateOrcValueWriter();

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

  private static class TimestampTzOrcValueWriter implements OrcValueWriter<OffsetDateTime> {
    private static final OrcValueWriter<OffsetDateTime> INSTANCE = new TimestampTzOrcValueWriter();

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

  private static class TimestampOrcValueWriter implements OrcValueWriter<LocalDateTime> {
    private static final OrcValueWriter<LocalDateTime> INSTANCE = new TimestampOrcValueWriter();

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

  private static class Decimal18OrcValueWriter implements OrcValueWriter<BigDecimal> {
    private final int scale;

    Decimal18OrcValueWriter(int scale) {
      this.scale = scale;
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

  private static class Decimal38OrcValueWriter implements OrcValueWriter<BigDecimal> {
    private static final OrcValueWriter<BigDecimal> INSTANCE = new Decimal38OrcValueWriter();

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

  private static class ListOrcValueWriter implements OrcValueWriter<List> {
    private final OrcValueWriter element;

    ListOrcValueWriter(OrcValueWriter element) {
      this.element = element;
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
          element.addValue((int) (e + cv.offsets[rowId]), value.get(e), cv.child);
        }
      }
    }
  }

  private static class MapOrcValueWriter implements OrcValueWriter<Map> {
    private final OrcValueWriter keyWriter;
    private final OrcValueWriter valueWriter;

    MapOrcValueWriter(OrcValueWriter keyWriter, OrcValueWriter valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
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
          keyWriter.addValue(pos, keys.get(e), cv.keys);
          valueWriter.addValue(pos, values.get(e), cv.values);
        }
      }
    }
  }
}
