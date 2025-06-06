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
package org.apache.iceberg.data.vortex;

import dev.vortex.api.Array;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.vortex.VortexValueReader;

public class GenericVortexReaders {
  private GenericVortexReaders() {}

  public static VortexValueReader<Boolean> bools() {
    return BooleanReader.INSTANCE;
  }

  public static VortexValueReader<Integer> ints() {
    return IntegerReader.INSTANCE;
  }

  public static VortexValueReader<BigDecimal> decimals() {
    return DecimalReader.INSTANCE;
  }

  public static VortexValueReader<Long> longs() {
    return LongReader.INSTANCE;
  }

  public static VortexValueReader<Float> floats() {
    return FloatReader.INSTANCE;
  }

  public static VortexValueReader<Double> doubles() {
    return DoubleReader.INSTANCE;
  }

  public static VortexValueReader<String> strings() {
    return StringReader.INSTANCE;
  }

  public static VortexValueReader<byte[]> bytes() {
    return BytesReader.INSTANCE;
  }

  public static VortexValueReader<LocalDate> date(boolean isMillis) {
    return new DateReader(isMillis);
  }

  public static VortexValueReader<LocalDateTime> timestamp(boolean nanosecond) {
    return new TimestampReader(nanosecond);
  }

  public static VortexValueReader<OffsetDateTime> timestampTz(String timeZone, boolean nanosecond) {
    return new TimestampTzReader(timeZone, nanosecond);
  }

  // Read a struct of record values instead.
  public static VortexValueReader<Record> struct(
      Types.StructType schema, List<VortexValueReader<?>> readers) {
    return new StructReader(schema, readers);
  }

  public static <T> VortexValueReader<List<T>> list(VortexValueReader<T> elementReader) {
    return new ListReader(elementReader);
  }

  private static class StructReader implements VortexValueReader<Record> {
    private final Types.StructType schema;
    private final List<VortexValueReader<?>> readers;

    private StructReader(Types.StructType schema, List<VortexValueReader<?>> readers) {
      this.schema = schema;
      this.readers = readers;
    }

    @Override
    public Record readNonNull(Array array, int row) {
      GenericRecord record = GenericRecord.create(schema);
      for (int i = 0; i < readers.size(); i++) {
        VortexValueReader<?> reader = readers.get(i);
        Array field = array.getField(i);
        Object value = reader.read(field, row);
        record.set(i, value);
      }
      return record;
    }
  }

  @SuppressWarnings("UnusedVariable")
  private static class ListReader<T> implements VortexValueReader<List<T>> {
    private final VortexValueReader<T> elementReader;

    private ListReader(VortexValueReader<T> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public List<T> readNonNull(Array array, int row) {
      // TODO(aduffy): implement LIST reads in vortex-jni.
      throw new UnsupportedOperationException("Reading lists from Vortex not supported yet");
    }
  }

  private static class BooleanReader implements VortexValueReader<Boolean> {
    static final BooleanReader INSTANCE = new BooleanReader();

    private BooleanReader() {}

    @Override
    public Boolean readNonNull(Array array, int row) {
      return array.getBool(row);
    }
  }

  private static class IntegerReader implements VortexValueReader<Integer> {
    static final IntegerReader INSTANCE = new IntegerReader();

    private IntegerReader() {}

    @Override
    public Integer readNonNull(Array array, int row) {
      return array.getInt(row);
    }
  }

  private static class LongReader implements VortexValueReader<Long> {
    static final LongReader INSTANCE = new LongReader();

    private LongReader() {}

    @Override
    public Long readNonNull(Array array, int row) {
      return array.getLong(row);
    }
  }

  private static class DecimalReader implements VortexValueReader<BigDecimal> {
    static final DecimalReader INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public BigDecimal readNonNull(Array array, int row) {
      return array.getBigDecimal(row);
    }
  }

  private static class FloatReader implements VortexValueReader<Float> {
    static final FloatReader INSTANCE = new FloatReader();

    private FloatReader() {}

    @Override
    public Float readNonNull(Array array, int row) {
      return array.getFloat(row);
    }
  }

  private static class DoubleReader implements VortexValueReader<Double> {
    static final DoubleReader INSTANCE = new DoubleReader();

    private DoubleReader() {}

    @Override
    public Double readNonNull(Array array, int row) {
      return array.getDouble(row);
    }
  }

  private static class StringReader implements VortexValueReader<String> {
    static final StringReader INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public String readNonNull(Array array, int row) {
      return array.getUTF8(row);
    }
  }

  private static class BytesReader implements VortexValueReader<byte[]> {
    static final BytesReader INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public byte[] readNonNull(Array array, int row) {
      return array.getBinary(row);
    }
  }

  private static class DateReader implements VortexValueReader<LocalDate> {
    private final boolean isMillis;

    DateReader(boolean isMillis) {
      this.isMillis = isMillis;
    }

    @Override
    public LocalDate readNonNull(Array array, int row) {
      int days;
      if (isMillis) {
        days = (int) Math.floorDiv(array.getLong(row), 86_400_000L);
      } else {
        days = array.getInt(row);
      }

      return DateTimeUtil.dateFromDays(days);
    }
  }

  private static class TimestampReader implements VortexValueReader<LocalDateTime> {
    private final boolean nanosecond;

    private TimestampReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    public LocalDateTime readNonNull(Array array, int row) {
      long measure = array.getLong(row);
      if (nanosecond) {
        return DateTimeUtil.timestampFromNanos(measure);
      } else {
        return DateTimeUtil.timestampFromMicros(measure);
      }
    }
  }

  private static class TimestampTzReader implements VortexValueReader<OffsetDateTime> {
    private final ZoneId timeZone;
    private final boolean nanosecond;

    private TimestampTzReader(String timeZone, boolean nanosecond) {
      this.timeZone = ZoneId.of(timeZone);
      this.nanosecond = nanosecond;
    }

    @Override
    public OffsetDateTime readNonNull(Array array, int row) {
      long measure = array.getLong(row);
      long nanoAdjustment;
      if (nanosecond) {
        nanoAdjustment = measure;
      } else {
        nanoAdjustment = Math.multiplyExact(1_000, measure);
      }
      return OffsetDateTime.ofInstant(Instant.EPOCH.plusNanos(nanoAdjustment), timeZone);
    }
  }
}
