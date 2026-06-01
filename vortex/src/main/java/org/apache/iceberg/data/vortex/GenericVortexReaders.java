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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BaseIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
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

  public static VortexValueReader<ByteBuffer> bytes() {
    return BytesReader.INSTANCE;
  }

  public static VortexValueReader<UUID> uuids() {
    return UuidReader.INSTANCE;
  }

  public static VortexValueReader<LocalDate> date(boolean isMillis) {
    return new DateReader(isMillis);
  }

  public static VortexValueReader<LocalTime> time(boolean nanosecond) {
    return new TimeReader(nanosecond);
  }

  public static VortexValueReader<LocalDateTime> timestamp(boolean nanosecond) {
    return new TimestampReader(nanosecond);
  }

  public static VortexValueReader<OffsetDateTime> timestampTz(String timeZone, boolean nanosecond) {
    return new TimestampTzReader(timeZone, nanosecond);
  }

  public static VortexValueReader<Record> struct(
      Types.StructType schema, List<VortexValueReader<?>> readers) {
    return new StructReader(schema, readers);
  }

  public static <T> VortexValueReader<List<T>> list(VortexValueReader<T> elementReader) {
    return new ListReader<>(elementReader);
  }

  private static class StructReader implements VortexValueReader<Record> {
    private final Types.StructType schema;
    private final List<VortexValueReader<?>> readers;

    private StructReader(Types.StructType schema, List<VortexValueReader<?>> readers) {
      this.schema = schema;
      this.readers = readers;
    }

    @Override
    public Record readNonNull(FieldVector vector, int row) {
      StructVector struct = (StructVector) vector;
      GenericRecord record = GenericRecord.create(schema);
      for (int i = 0; i < readers.size(); i++) {
        VortexValueReader<?> reader = readers.get(i);
        FieldVector child = (FieldVector) struct.getChildByOrdinal(i);
        Object value = reader.read(child, row);
        record.set(i, value);
      }
      return record;
    }
  }

  private static class ListReader<T> implements VortexValueReader<List<T>> {
    private final VortexValueReader<T> elementReader;

    private ListReader(VortexValueReader<T> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public List<T> readNonNull(FieldVector vector, int row) {
      ListVector listVector = (ListVector) vector;
      int start = listVector.getElementStartIndex(row);
      int end = listVector.getElementEndIndex(row);
      FieldVector elementVector = listVector.getDataVector();
      return IntStream.range(start, end)
          .mapToObj(i -> elementReader.read(elementVector, i))
          .toList();
    }
  }

  private static class BooleanReader implements VortexValueReader<Boolean> {
    static final BooleanReader INSTANCE = new BooleanReader();

    private BooleanReader() {}

    @Override
    public Boolean readNonNull(FieldVector vector, int row) {
      return ((BitVector) vector).get(row) != 0;
    }
  }

  private static class IntegerReader implements VortexValueReader<Integer> {
    static final IntegerReader INSTANCE = new IntegerReader();

    private IntegerReader() {}

    @Override
    public Integer readNonNull(FieldVector vector, int row) {
      return (int) ((BaseIntVector) vector).getValueAsLong(row);
    }
  }

  private static class LongReader implements VortexValueReader<Long> {
    static final LongReader INSTANCE = new LongReader();

    private LongReader() {}

    @Override
    public Long readNonNull(FieldVector vector, int row) {
      return ((BaseIntVector) vector).getValueAsLong(row);
    }
  }

  private static class DecimalReader implements VortexValueReader<BigDecimal> {
    static final DecimalReader INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public BigDecimal readNonNull(FieldVector vector, int row) {
      return ((DecimalVector) vector).getObjectNotNull(row);
    }
  }

  private static class FloatReader implements VortexValueReader<Float> {
    static final FloatReader INSTANCE = new FloatReader();

    private FloatReader() {}

    @Override
    public Float readNonNull(FieldVector vector, int row) {
      return ((Float4Vector) vector).get(row);
    }
  }

  private static class DoubleReader implements VortexValueReader<Double> {
    static final DoubleReader INSTANCE = new DoubleReader();

    private DoubleReader() {}

    @Override
    public Double readNonNull(FieldVector vector, int row) {
      return ((Float8Vector) vector).get(row);
    }
  }

  private static class StringReader implements VortexValueReader<String> {
    static final StringReader INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public String readNonNull(FieldVector vector, int row) {
      return new String(((VarCharVector) vector).get(row), StandardCharsets.UTF_8);
    }
  }

  private static class BytesReader implements VortexValueReader<ByteBuffer> {
    static final BytesReader INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public ByteBuffer readNonNull(FieldVector vector, int row) {
      return ByteBuffer.wrap(((VarBinaryVector) vector).get(row));
    }
  }

  private static class UuidReader implements VortexValueReader<UUID> {
    static final UuidReader INSTANCE = new UuidReader();

    private UuidReader() {}

    @Override
    public UUID readNonNull(FieldVector vector, int row) {
      return UUIDUtil.convert(uuidStorage(vector).get(row));
    }
  }

  /**
   * Returns the underlying {@link FixedSizeBinaryVector} for a UUID column. Vortex may emit either
   * a registered extension vector (wrapping FixedSizeBinary) or the raw fixed-binary storage,
   * depending on whether {@code arrow.uuid} is registered in the consumer's {@link
   * org.apache.arrow.vector.types.pojo.ExtensionTypeRegistry}.
   */
  static FixedSizeBinaryVector uuidStorage(FieldVector vector) {
    if (vector instanceof ExtensionTypeVector<?> ext) {
      return (FixedSizeBinaryVector) ext.getUnderlyingVector();
    }
    return (FixedSizeBinaryVector) vector;
  }

  private static class DateReader implements VortexValueReader<LocalDate> {
    private final boolean isMillis;

    DateReader(boolean isMillis) {
      this.isMillis = isMillis;
    }

    @Override
    public LocalDate readNonNull(FieldVector vector, int row) {
      int days;
      if (isMillis) {
        days = (int) Math.floorDiv(((DateMilliVector) vector).get(row), 86_400_000L);
      } else {
        days = ((DateDayVector) vector).get(row);
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
    public LocalDateTime readNonNull(FieldVector vector, int row) {
      long measure = ((TimeStampVector) vector).get(row);
      if (nanosecond) {
        return DateTimeUtil.timestampFromNanos(measure);
      } else {
        return DateTimeUtil.timestampFromMicros(measure);
      }
    }
  }

  private static class TimeReader implements VortexValueReader<LocalTime> {
    private final boolean nanosecond;

    private TimeReader(boolean nanosecond) {
      this.nanosecond = nanosecond;
    }

    @Override
    public LocalTime readNonNull(FieldVector vector, int row) {
      if (nanosecond) {
        return LocalTime.ofNanoOfDay(((TimeNanoVector) vector).get(row));
      } else {
        return DateTimeUtil.timeFromMicros(((TimeMicroVector) vector).get(row));
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
    public OffsetDateTime readNonNull(FieldVector vector, int row) {
      long measure = ((TimeStampVector) vector).get(row);
      long nanoAdjustment;
      if (nanosecond) {
        nanoAdjustment = measure;
      } else {
        nanoAdjustment = Math.multiplyExact(1_000L, measure);
      }
      return OffsetDateTime.ofInstant(Instant.EPOCH.plusNanos(nanoAdjustment), timeZone);
    }
  }
}
