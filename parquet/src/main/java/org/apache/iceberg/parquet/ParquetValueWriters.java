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

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.DoubleFieldMetrics;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FloatFieldMetrics;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.Type;

public class ParquetValueWriters {
  private ParquetValueWriters() {}

  public static <T> ParquetValueWriter<T> option(
      Type type, int definitionLevel, ParquetValueWriter<T> writer) {
    if (type.isRepetition(Type.Repetition.OPTIONAL)) {
      return new OptionWriter<>(definitionLevel, writer);
    }

    return writer;
  }

  public static UnboxedWriter<Boolean> booleans(ColumnDescriptor desc) {
    return new UnboxedWriter<>(desc);
  }

  public static UnboxedWriter<Byte> tinyints(ColumnDescriptor desc) {
    return new ByteWriter(desc);
  }

  public static UnboxedWriter<Short> shorts(ColumnDescriptor desc) {
    return new ShortWriter(desc);
  }

  public static UnboxedWriter<Integer> ints(ColumnDescriptor desc) {
    return new UnboxedWriter<>(desc);
  }

  public static UnboxedWriter<Long> longs(ColumnDescriptor desc) {
    return new UnboxedWriter<>(desc);
  }

  public static UnboxedWriter<Float> floats(ColumnDescriptor desc) {
    return new FloatWriter(desc);
  }

  public static UnboxedWriter<Double> doubles(ColumnDescriptor desc) {
    return new DoubleWriter(desc);
  }

  public static PrimitiveWriter<CharSequence> strings(ColumnDescriptor desc) {
    return new StringWriter(desc);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsInteger(
      ColumnDescriptor desc, int precision, int scale) {
    return new IntegerDecimalWriter(desc, precision, scale);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsLong(
      ColumnDescriptor desc, int precision, int scale) {
    return new LongDecimalWriter(desc, precision, scale);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsFixed(
      ColumnDescriptor desc, int precision, int scale) {
    return new FixedDecimalWriter(desc, precision, scale);
  }

  public static PrimitiveWriter<ByteBuffer> byteBuffers(ColumnDescriptor desc) {
    return new BytesWriter(desc);
  }

  public static <E> CollectionWriter<E> collections(int dl, int rl, ParquetValueWriter<E> writer) {
    return new CollectionWriter<>(dl, rl, writer);
  }

  public static <K, V> MapWriter<K, V> maps(
      int dl, int rl, ParquetValueWriter<K> keyWriter, ParquetValueWriter<V> valueWriter) {
    return new MapWriter<>(dl, rl, keyWriter, valueWriter);
  }

  public abstract static class PrimitiveWriter<T> implements ParquetValueWriter<T> {
    @SuppressWarnings("checkstyle:VisibilityModifier")
    protected final ColumnWriter<T> column;

    private final List<TripleWriter<?>> children;

    protected PrimitiveWriter(ColumnDescriptor desc) {
      this.column = ColumnWriter.newWriter(desc);
      this.children = ImmutableList.of(column);
    }

    @Override
    public void write(int repetitionLevel, T value) {
      column.write(repetitionLevel, value);
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      this.column.setColumnStore(columnStore);
    }
  }

  private static class UnboxedWriter<T> extends PrimitiveWriter<T> {
    private UnboxedWriter(ColumnDescriptor desc) {
      super(desc);
    }

    public void writeInteger(int repetitionLevel, int value) {
      column.writeInteger(repetitionLevel, value);
    }

    public void writeFloat(int repetitionLevel, float value) {
      column.writeFloat(repetitionLevel, value);
    }

    public void writeDouble(int repetitionLevel, double value) {
      column.writeDouble(repetitionLevel, value);
    }
  }

  private static class FloatWriter extends UnboxedWriter<Float> {
    private final FloatFieldMetrics.Builder floatFieldMetricsBuilder;

    private FloatWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      this.floatFieldMetricsBuilder = new FloatFieldMetrics.Builder(id);
    }

    @Override
    public void write(int repetitionLevel, Float value) {
      writeFloat(repetitionLevel, value);
      floatFieldMetricsBuilder.addValue(value);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(floatFieldMetricsBuilder.build());
    }
  }

  private static class DoubleWriter extends UnboxedWriter<Double> {
    private final DoubleFieldMetrics.Builder doubleFieldMetricsBuilder;

    private DoubleWriter(ColumnDescriptor desc) {
      super(desc);
      int id = desc.getPrimitiveType().getId().intValue();
      this.doubleFieldMetricsBuilder = new DoubleFieldMetrics.Builder(id);
    }

    @Override
    public void write(int repetitionLevel, Double value) {
      writeDouble(repetitionLevel, value);
      doubleFieldMetricsBuilder.addValue(value);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(doubleFieldMetricsBuilder.build());
    }
  }

  private static class ByteWriter extends UnboxedWriter<Byte> {
    private ByteWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Byte value) {
      writeInteger(repetitionLevel, value.intValue());
    }
  }

  private static class ShortWriter extends UnboxedWriter<Short> {
    private ShortWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, Short value) {
      writeInteger(repetitionLevel, value.intValue());
    }
  }

  private static class IntegerDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private IntegerDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(
          decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s",
          precision,
          scale,
          decimal);
      Preconditions.checkArgument(
          decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s",
          precision,
          scale,
          decimal);

      column.writeInteger(repetitionLevel, decimal.unscaledValue().intValue());
    }
  }

  private static class LongDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private LongDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(
          decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s",
          precision,
          scale,
          decimal);
      Preconditions.checkArgument(
          decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s",
          precision,
          scale,
          decimal);

      column.writeLong(repetitionLevel, decimal.unscaledValue().longValue());
    }
  }

  private static class FixedDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private FixedDecimalWriter(ColumnDescriptor desc, int precision, int scale) {
      super(desc);
      this.precision = precision;
      this.scale = scale;
      this.bytes =
          ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      byte[] binary = DecimalUtil.toReusedFixLengthBytes(precision, scale, decimal, bytes.get());
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(binary));
    }
  }

  private static class BytesWriter extends PrimitiveWriter<ByteBuffer> {
    private BytesWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, ByteBuffer buffer) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(buffer));
    }
  }

  private static class StringWriter extends PrimitiveWriter<CharSequence> {
    private StringWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, CharSequence value) {
      if (value instanceof Utf8) {
        Utf8 utf8 = (Utf8) value;
        column.writeBinary(
            repetitionLevel, Binary.fromReusedByteArray(utf8.getBytes(), 0, utf8.getByteLength()));
      } else {
        column.writeBinary(repetitionLevel, Binary.fromString(value.toString()));
      }
    }
  }

  static class OptionWriter<T> implements ParquetValueWriter<T> {
    private final int definitionLevel;
    private final ParquetValueWriter<T> writer;
    private final List<TripleWriter<?>> children;
    private long nullValueCount = 0;

    OptionWriter(int definitionLevel, ParquetValueWriter<T> writer) {
      this.definitionLevel = definitionLevel;
      this.writer = writer;
      this.children = writer.columns();
    }

    @Override
    public void write(int repetitionLevel, T value) {
      if (value != null) {
        writer.write(repetitionLevel, value);

      } else {
        nullValueCount++;
        for (TripleWriter<?> column : children) {
          column.writeNull(repetitionLevel, definitionLevel - 1);
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      writer.setColumnStore(columnStore);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      if (writer instanceof PrimitiveWriter) {
        List<FieldMetrics<?>> fieldMetricsFromWriter =
            writer.metrics().collect(Collectors.toList());

        if (fieldMetricsFromWriter.isEmpty()) {
          // we are not tracking field metrics for this type ourselves
          return Stream.empty();
        } else if (fieldMetricsFromWriter.size() == 1) {
          FieldMetrics<?> metrics = fieldMetricsFromWriter.get(0);
          return Stream.of(
              new FieldMetrics<>(
                  metrics.id(),
                  metrics.valueCount() + nullValueCount,
                  nullValueCount,
                  metrics.nanValueCount(),
                  metrics.lowerBound(),
                  metrics.upperBound()));
        } else {
          throw new IllegalStateException(
              String.format(
                  "OptionWriter should only expect at most one field metric from a primitive writer."
                      + "Current number of fields: %s, primitive writer type: %s",
                  fieldMetricsFromWriter.size(), writer.getClass().getSimpleName()));
        }
      }

      // skipping updating null stats for non-primitive types since we don't use them today, to
      // avoid unnecessary work
      return writer.metrics();
    }
  }

  public abstract static class RepeatedWriter<L, E> implements ParquetValueWriter<L> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueWriter<E> writer;
    private final List<TripleWriter<?>> children;

    protected RepeatedWriter(
        int definitionLevel, int repetitionLevel, ParquetValueWriter<E> writer) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.writer = writer;
      this.children = writer.columns();
    }

    @Override
    public void write(int parentRepetition, L value) {
      Iterator<E> elements = elements(value);

      if (!elements.hasNext()) {
        // write the empty list to each column
        // TODO: make sure this definition level is correct
        for (TripleWriter<?> column : children) {
          column.writeNull(parentRepetition, definitionLevel - 1);
        }

      } else {
        boolean first = true;
        while (elements.hasNext()) {
          E element = elements.next();

          int rl = repetitionLevel;
          if (first) {
            rl = parentRepetition;
            first = false;
          }

          writer.write(rl, element);
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      writer.setColumnStore(columnStore);
    }

    protected abstract Iterator<E> elements(L value);

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writer.metrics();
    }
  }

  private static class CollectionWriter<E> extends RepeatedWriter<Collection<E>, E> {
    private CollectionWriter(
        int definitionLevel, int repetitionLevel, ParquetValueWriter<E> writer) {
      super(definitionLevel, repetitionLevel, writer);
    }

    @Override
    protected Iterator<E> elements(Collection<E> list) {
      return list.iterator();
    }
  }

  public abstract static class RepeatedKeyValueWriter<M, K, V> implements ParquetValueWriter<M> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueWriter<K> keyWriter;
    private final ParquetValueWriter<V> valueWriter;
    private final List<TripleWriter<?>> children;

    protected RepeatedKeyValueWriter(
        int definitionLevel,
        int repetitionLevel,
        ParquetValueWriter<K> keyWriter,
        ParquetValueWriter<V> valueWriter) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
      this.children =
          ImmutableList.<TripleWriter<?>>builder()
              .addAll(keyWriter.columns())
              .addAll(valueWriter.columns())
              .build();
    }

    @Override
    public void write(int parentRepetition, M value) {
      Iterator<Map.Entry<K, V>> pairs = pairs(value);

      if (!pairs.hasNext()) {
        // write the empty map to each column
        for (TripleWriter<?> column : children) {
          column.writeNull(parentRepetition, definitionLevel - 1);
        }

      } else {
        boolean first = true;
        while (pairs.hasNext()) {
          Map.Entry<K, V> pair = pairs.next();

          int rl = repetitionLevel;
          if (first) {
            rl = parentRepetition;
            first = false;
          }

          keyWriter.write(rl, pair.getKey());
          valueWriter.write(rl, pair.getValue());
        }
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      keyWriter.setColumnStore(columnStore);
      valueWriter.setColumnStore(columnStore);
    }

    protected abstract Iterator<Map.Entry<K, V>> pairs(M value);

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
    }
  }

  private static class MapWriter<K, V> extends RepeatedKeyValueWriter<Map<K, V>, K, V> {
    private MapWriter(
        int definitionLevel,
        int repetitionLevel,
        ParquetValueWriter<K> keyWriter,
        ParquetValueWriter<V> valueWriter) {
      super(definitionLevel, repetitionLevel, keyWriter, valueWriter);
    }

    @Override
    protected Iterator<Map.Entry<K, V>> pairs(Map<K, V> map) {
      return map.entrySet().iterator();
    }
  }

  public abstract static class StructWriter<S> implements ParquetValueWriter<S> {
    private final ParquetValueWriter<Object>[] writers;
    private final List<TripleWriter<?>> children;

    @SuppressWarnings("unchecked")
    protected StructWriter(List<ParquetValueWriter<?>> writers) {
      this.writers =
          (ParquetValueWriter<Object>[])
              Array.newInstance(ParquetValueWriter.class, writers.size());

      ImmutableList.Builder<TripleWriter<?>> columnsBuilder = ImmutableList.builder();
      for (int i = 0; i < writers.size(); i += 1) {
        ParquetValueWriter<?> writer = writers.get(i);
        this.writers[i] = (ParquetValueWriter<Object>) writer;
        columnsBuilder.addAll(writer.columns());
      }

      this.children = columnsBuilder.build();
    }

    @Override
    public void write(int repetitionLevel, S value) {
      for (int i = 0; i < writers.length; i += 1) {
        Object fieldValue = get(value, i);
        writers[i].write(repetitionLevel, fieldValue);
      }
    }

    @Override
    public List<TripleWriter<?>> columns() {
      return children;
    }

    @Override
    public void setColumnStore(ColumnWriteStore columnStore) {
      for (ParquetValueWriter<?> writer : writers) {
        writer.setColumnStore(columnStore);
      }
    }

    protected abstract Object get(S struct, int index);

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Arrays.stream(writers).flatMap(ParquetValueWriter::metrics);
    }
  }

  public static class PositionDeleteStructWriter<R> extends StructWriter<PositionDelete<R>> {
    private final Function<CharSequence, ?> pathTransformFunc;

    public PositionDeleteStructWriter(
        StructWriter<?> replacedWriter, Function<CharSequence, ?> pathTransformFunc) {
      super(Arrays.asList(replacedWriter.writers));
      this.pathTransformFunc = pathTransformFunc;
    }

    @Override
    protected Object get(PositionDelete<R> delete, int index) {
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
  }
}
