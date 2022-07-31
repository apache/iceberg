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
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.iceberg.DoubleFieldMetrics;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FloatFieldMetrics;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.orc.OrcRowWriter;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ByteBuffers;
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

public class GenericOrcWriters {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private GenericOrcWriters() {}

  public static OrcValueWriter<Boolean> booleans() {
    return BooleanWriter.INSTANCE;
  }

  public static OrcValueWriter<Byte> bytes() {
    return ByteWriter.INSTANCE;
  }

  public static OrcValueWriter<Short> shorts() {
    return ShortWriter.INSTANCE;
  }

  public static OrcValueWriter<Integer> ints() {
    return IntWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalTime> times() {
    return TimeWriter.INSTANCE;
  }

  public static OrcValueWriter<Long> longs() {
    return LongWriter.INSTANCE;
  }

  public static OrcValueWriter<Float> floats(int id) {
    return new FloatWriter(id);
  }

  public static OrcValueWriter<Double> doubles(int id) {
    return new DoubleWriter(id);
  }

  public static OrcValueWriter<String> strings() {
    return StringWriter.INSTANCE;
  }

  public static OrcValueWriter<ByteBuffer> byteBuffers() {
    return ByteBufferWriter.INSTANCE;
  }

  public static OrcValueWriter<UUID> uuids() {
    return UUIDWriter.INSTANCE;
  }

  public static OrcValueWriter<byte[]> byteArrays() {
    return ByteArrayWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalDate> dates() {
    return DateWriter.INSTANCE;
  }

  public static OrcValueWriter<OffsetDateTime> timestampTz() {
    return TimestampTzWriter.INSTANCE;
  }

  public static OrcValueWriter<LocalDateTime> timestamp() {
    return TimestampWriter.INSTANCE;
  }

  public static OrcValueWriter<BigDecimal> decimal(int precision, int scale) {
    if (precision <= 18) {
      return new Decimal18Writer(precision, scale);
    } else if (precision <= 38) {
      return new Decimal38Writer(precision, scale);
    } else {
      throw new IllegalArgumentException("Invalid precision: " + precision);
    }
  }

  public static <T> OrcValueWriter<List<T>> list(OrcValueWriter<T> element) {
    return new ListWriter<>(element);
  }

  public static <K, V> OrcValueWriter<Map<K, V>> map(
      OrcValueWriter<K> key, OrcValueWriter<V> value) {
    return new MapWriter<>(key, value);
  }

  public static <T> OrcRowWriter<PositionDelete<T>> positionDelete(
      OrcRowWriter<T> writer, Function<CharSequence, ?> pathTransformFunc) {
    return new PositionDeleteStructWriter<>(writer, pathTransformFunc);
  }

  private static class BooleanWriter implements OrcValueWriter<Boolean> {
    private static final OrcValueWriter<Boolean> INSTANCE = new BooleanWriter();

    @Override
    public void nonNullWrite(int rowId, Boolean data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data ? 1 : 0;
    }
  }

  private static class ByteWriter implements OrcValueWriter<Byte> {
    private static final OrcValueWriter<Byte> INSTANCE = new ByteWriter();

    @Override
    public void nonNullWrite(int rowId, Byte data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class ShortWriter implements OrcValueWriter<Short> {
    private static final OrcValueWriter<Short> INSTANCE = new ShortWriter();

    @Override
    public void nonNullWrite(int rowId, Short data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class IntWriter implements OrcValueWriter<Integer> {
    private static final OrcValueWriter<Integer> INSTANCE = new IntWriter();

    @Override
    public void nonNullWrite(int rowId, Integer data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class TimeWriter implements OrcValueWriter<LocalTime> {
    private static final OrcValueWriter<LocalTime> INSTANCE = new TimeWriter();

    @Override
    public void nonNullWrite(int rowId, LocalTime data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.toNanoOfDay() / 1_000;
    }
  }

  private static class LongWriter implements OrcValueWriter<Long> {
    private static final OrcValueWriter<Long> INSTANCE = new LongWriter();

    @Override
    public void nonNullWrite(int rowId, Long data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class FloatWriter implements OrcValueWriter<Float> {
    private final FloatFieldMetrics.Builder floatFieldMetricsBuilder;
    private long nullValueCount = 0;

    private FloatWriter(int id) {
      this.floatFieldMetricsBuilder = new FloatFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, Float data, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = data;
      floatFieldMetricsBuilder.addValue(data);
    }

    @Override
    public void nullWrite() {
      nullValueCount++;
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      FieldMetrics<Float> metricsWithoutNullCount = floatFieldMetricsBuilder.build();
      return Stream.of(
          new FieldMetrics<>(
              metricsWithoutNullCount.id(),
              metricsWithoutNullCount.valueCount() + nullValueCount,
              nullValueCount,
              metricsWithoutNullCount.nanValueCount(),
              metricsWithoutNullCount.lowerBound(),
              metricsWithoutNullCount.upperBound()));
    }
  }

  private static class DoubleWriter implements OrcValueWriter<Double> {
    private final DoubleFieldMetrics.Builder doubleFieldMetricsBuilder;
    private long nullValueCount = 0;

    private DoubleWriter(Integer id) {
      this.doubleFieldMetricsBuilder = new DoubleFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, Double data, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = data;
      doubleFieldMetricsBuilder.addValue(data);
    }

    @Override
    public void nullWrite() {
      nullValueCount++;
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      FieldMetrics<Double> metricsWithoutNullCount = doubleFieldMetricsBuilder.build();
      return Stream.of(
          new FieldMetrics<>(
              metricsWithoutNullCount.id(),
              metricsWithoutNullCount.valueCount() + nullValueCount,
              nullValueCount,
              metricsWithoutNullCount.nanValueCount(),
              metricsWithoutNullCount.lowerBound(),
              metricsWithoutNullCount.upperBound()));
    }
  }

  private static class StringWriter implements OrcValueWriter<String> {
    private static final OrcValueWriter<String> INSTANCE = new StringWriter();

    @Override
    public void nonNullWrite(int rowId, String data, ColumnVector output) {
      byte[] value = data.getBytes(StandardCharsets.UTF_8);
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class ByteBufferWriter implements OrcValueWriter<ByteBuffer> {
    private static final OrcValueWriter<ByteBuffer> INSTANCE = new ByteBufferWriter();

    @Override
    public void nonNullWrite(int rowId, ByteBuffer data, ColumnVector output) {
      if (data.hasArray()) {
        ((BytesColumnVector) output)
            .setRef(rowId, data.array(), data.arrayOffset() + data.position(), data.remaining());
      } else {
        byte[] rawData = ByteBuffers.toByteArray(data);
        ((BytesColumnVector) output).setRef(rowId, rawData, 0, rawData.length);
      }
    }
  }

  private static class UUIDWriter implements OrcValueWriter<UUID> {
    private static final OrcValueWriter<UUID> INSTANCE = new UUIDWriter();

    @Override
    @SuppressWarnings("ByteBufferBackingArray")
    public void nonNullWrite(int rowId, UUID data, ColumnVector output) {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(data.getMostSignificantBits());
      buffer.putLong(data.getLeastSignificantBits());
      ((BytesColumnVector) output).setRef(rowId, buffer.array(), 0, buffer.array().length);
    }
  }

  private static class ByteArrayWriter implements OrcValueWriter<byte[]> {
    private static final OrcValueWriter<byte[]> INSTANCE = new ByteArrayWriter();

    @Override
    public void nonNullWrite(int rowId, byte[] data, ColumnVector output) {
      ((BytesColumnVector) output).setRef(rowId, data, 0, data.length);
    }
  }

  private static class DateWriter implements OrcValueWriter<LocalDate> {
    private static final OrcValueWriter<LocalDate> INSTANCE = new DateWriter();

    @Override
    public void nonNullWrite(int rowId, LocalDate data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = ChronoUnit.DAYS.between(EPOCH_DAY, data);
    }
  }

  private static class TimestampTzWriter implements OrcValueWriter<OffsetDateTime> {
    private static final OrcValueWriter<OffsetDateTime> INSTANCE = new TimestampTzWriter();

    @Override
    @SuppressWarnings("JavaLocalDateTimeGetNano")
    public void nonNullWrite(int rowId, OffsetDateTime data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      // millis
      cv.time[rowId] = data.toInstant().toEpochMilli();
      // truncate nanos to only keep microsecond precision
      cv.nanos[rowId] = data.getNano() / 1_000 * 1_000;
    }
  }

  private static class TimestampWriter implements OrcValueWriter<LocalDateTime> {
    private static final OrcValueWriter<LocalDateTime> INSTANCE = new TimestampWriter();

    @Override
    @SuppressWarnings("JavaLocalDateTimeGetNano")
    public void nonNullWrite(int rowId, LocalDateTime data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      cv.setIsUTC(true);
      cv.time[rowId] = data.toInstant(ZoneOffset.UTC).toEpochMilli(); // millis
      cv.nanos[rowId] =
          (data.getNano() / 1_000) * 1_000; // truncate nanos to only keep microsecond precision
    }
  }

  private static class Decimal18Writer implements OrcValueWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    Decimal18Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
      Preconditions.checkArgument(
          data.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s",
          precision,
          scale,
          data);
      Preconditions.checkArgument(
          data.precision() <= precision,
          "Cannot write value as decimal(%s,%s), invalid precision: %s",
          precision,
          scale,
          data);

      ((DecimalColumnVector) output)
          .vector[rowId].setFromLongAndScale(data.unscaledValue().longValueExact(), scale);
    }
  }

  private static class Decimal38Writer implements OrcValueWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    Decimal38Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, BigDecimal data, ColumnVector output) {
      Preconditions.checkArgument(
          data.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s",
          precision,
          scale,
          data);
      Preconditions.checkArgument(
          data.precision() <= precision,
          "Cannot write value as decimal(%s,%s), invalid precision: %s",
          precision,
          scale,
          data);

      ((DecimalColumnVector) output).vector[rowId].set(HiveDecimal.create(data, false));
    }
  }

  private static class ListWriter<T> implements OrcValueWriter<List<T>> {
    private final OrcValueWriter<T> element;

    ListWriter(OrcValueWriter<T> element) {
      this.element = element;
    }

    @Override
    public void nonNullWrite(int rowId, List<T> value, ColumnVector output) {
      ListColumnVector cv = (ListColumnVector) output;
      // record the length and start of the list elements
      cv.lengths[rowId] = value.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
      // make sure the child is big enough
      growColumnVector(cv.child, cv.childCount);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        element.write((int) (e + cv.offsets[rowId]), value.get(e), cv.child);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return element.metrics();
    }
  }

  private static class MapWriter<K, V> implements OrcValueWriter<Map<K, V>> {
    private final OrcValueWriter<K> keyWriter;
    private final OrcValueWriter<V> valueWriter;

    MapWriter(OrcValueWriter<K> keyWriter, OrcValueWriter<V> valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    public void nonNullWrite(int rowId, Map<K, V> map, ColumnVector output) {
      List<K> keys = Lists.newArrayListWithExpectedSize(map.size());
      List<V> values = Lists.newArrayListWithExpectedSize(map.size());
      for (Map.Entry<K, V> entry : map.entrySet()) {
        keys.add(entry.getKey());
        values.add(entry.getValue());
      }
      MapColumnVector cv = (MapColumnVector) output;
      // record the length and start of the list elements
      cv.lengths[rowId] = map.size();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
      // make sure the child is big enough
      growColumnVector(cv.keys, cv.childCount);
      growColumnVector(cv.values, cv.childCount);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        int pos = (int) (e + cv.offsets[rowId]);
        keyWriter.write(pos, keys.get(e), cv.keys);
        valueWriter.write(pos, values.get(e), cv.values);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
    }
  }

  public abstract static class StructWriter<S> implements OrcValueWriter<S> {
    private final List<OrcValueWriter<?>> writers;

    protected StructWriter(List<OrcValueWriter<?>> writers) {
      this.writers = writers;
    }

    public List<OrcValueWriter<?>> writers() {
      return writers;
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writers.stream().flatMap(OrcValueWriter::metrics);
    }

    @Override
    public void nonNullWrite(int rowId, S value, ColumnVector output) {
      StructColumnVector cv = (StructColumnVector) output;
      write(rowId, value, c -> cv.fields[c]);
    }

    // Special case of writing the root struct
    public void writeRow(S value, VectorizedRowBatch output) {
      int rowId = output.size;
      output.size += 1;
      write(rowId, value, c -> output.cols[c]);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void write(int rowId, S value, Function<Integer, ColumnVector> colVectorAtFunc) {
      for (int c = 0; c < writers.size(); ++c) {
        OrcValueWriter writer = writers.get(c);
        writer.write(rowId, get(value, c), colVectorAtFunc.apply(c));
      }
    }

    protected abstract Object get(S struct, int index);
  }

  private static class PositionDeleteStructWriter<T> extends StructWriter<PositionDelete<T>>
      implements OrcRowWriter<PositionDelete<T>> {
    private final Function<CharSequence, ?> pathTransformFunc;

    PositionDeleteStructWriter(
        OrcRowWriter<T> replacedWriter, Function<CharSequence, ?> pathTransformFunc) {
      super(replacedWriter.writers());
      this.pathTransformFunc = pathTransformFunc;
    }

    @Override
    protected Object get(PositionDelete<T> delete, int index) {
      switch (index) {
        case 0:
          return pathTransformFunc.apply(delete.path());
        case 1:
          return delete.pos();
        case 2:
          return delete.row();
      }
      throw new IllegalArgumentException("Cannot get value for invalid index: " + index);
    }

    @Override
    public void write(PositionDelete<T> row, VectorizedRowBatch output) throws IOException {
      Preconditions.checkArgument(row != null, "value must not be null");
      Preconditions.checkArgument(
          writers().size() == 2 || row.row() != null,
          "The row in PositionDelete must not be null because it was set row schema in position delete.");
      writeRow(row, output);
    }
  }

  private static void growColumnVector(ColumnVector cv, int requestedSize) {
    if (cv.isNull.length < requestedSize) {
      // Use growth factor of 3 to avoid frequent array allocations
      cv.ensureSize(requestedSize * 3, true);
    }
  }
}
