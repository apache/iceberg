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
import java.util.stream.Stream;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.FieldMetrics;
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
  private ParquetValueWriters() {
  }

  public static <T> ParquetValueWriter<T> option(Type type,
                                                 int definitionLevel,
                                                 ParquetValueWriter<T> writer) {
    if (type.isRepetition(Type.Repetition.OPTIONAL)) {
      return new OptionWriter<>(definitionLevel, writer);
    }

    return writer;
  }

  public static UnboxedWriter<Boolean> booleans(ColumnDescriptor desc, Type.ID id) {
    return new UnboxedWriter<>(desc, id);
  }

  public static UnboxedWriter<Byte> tinyints(ColumnDescriptor desc, Type.ID id) {
    return new ByteWriter(desc, id);
  }

  public static UnboxedWriter<Short> shorts(ColumnDescriptor desc, Type.ID id) {
    return new ShortWriter(desc, id);
  }

  public static UnboxedWriter<Integer> ints(ColumnDescriptor desc, Type.ID id) {
    return new UnboxedWriter<>(desc, id);
  }

  public static UnboxedWriter<Long> longs(ColumnDescriptor desc, Type.ID id) {
    return new UnboxedWriter<>(desc, id);
  }

  public static UnboxedWriter<Float> floats(ColumnDescriptor desc, Type.ID id) {
    return new FloatWriter(desc, id);
  }

  public static UnboxedWriter<Double> doubles(ColumnDescriptor desc, Type.ID id) {
    return new DoubleWriter(desc, id);
  }

  public static PrimitiveWriter<CharSequence> strings(ColumnDescriptor desc, Type.ID id) {
    return new StringWriter(desc, id);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsInteger(ColumnDescriptor desc, Type.ID id,
                                                             int precision, int scale) {
    return new IntegerDecimalWriter(desc, id, precision, scale);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsLong(ColumnDescriptor desc, Type.ID id,
                                                          int precision, int scale) {
    return new LongDecimalWriter(desc, id, precision, scale);
  }

  public static PrimitiveWriter<BigDecimal> decimalAsFixed(ColumnDescriptor desc, Type.ID id,
                                                           int precision, int scale) {
    return new FixedDecimalWriter(desc, id, precision, scale);
  }

  public static PrimitiveWriter<ByteBuffer> byteBuffers(ColumnDescriptor desc, Type.ID id) {
    return new BytesWriter(desc, id);
  }

  public static <E> CollectionWriter<E> collections(int dl, int rl, ParquetValueWriter<E> writer) {
    return new CollectionWriter<>(dl, rl, writer);
  }

  public static <K, V> MapWriter<K, V> maps(int dl, int rl,
                                            ParquetValueWriter<K> keyWriter,
                                            ParquetValueWriter<V> valueWriter) {
    return new MapWriter<>(dl, rl, keyWriter, valueWriter);
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  public abstract static class PrimitiveWriter<T> implements ParquetValueWriter<T> {
    protected final ColumnWriter<T> column;
    private final List<TripleWriter<?>> children;
    protected final Type.ID id;

    protected PrimitiveWriter(ColumnDescriptor desc, Type.ID id) {
      this.column = ColumnWriter.newWriter(desc);
      this.children = ImmutableList.of(column);
      this.id = id;
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

    @Override
    public Stream<FieldMetrics> metrics() {
      if (id != null) {
        return Stream.of(new FieldMetrics(id.intValue(), 0L, 0L, 0L, null, null));
      } else {
        return Stream.empty();
      }
    }
  }

  private static class UnboxedWriter<T> extends PrimitiveWriter<T> {
    private UnboxedWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
    }

    public void writeBoolean(int repetitionLevel, boolean value) {
      column.writeBoolean(repetitionLevel, value);
    }

    public void writeInteger(int repetitionLevel, int value) {
      column.writeInteger(repetitionLevel, value);
    }

    public void writeLong(int repetitionLevel, long  value) {
      column.writeLong(repetitionLevel, value);
    }

    public void writeFloat(int repetitionLevel, float value) {
      column.writeFloat(repetitionLevel, value);
    }

    public void writeDouble(int repetitionLevel, double value) {
      column.writeDouble(repetitionLevel, value);
    }
  }

  private static class FloatWriter extends UnboxedWriter<Float> {
    private final ColumnDescriptor desc;
    private long nanCount;

    private FloatWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
      this.desc = desc;
      this.nanCount = 0;
    }

    @Override
    public void write(int repetitionLevel, Float value) {
      super.write(repetitionLevel, value);
      if (Float.compare(Float.NaN, value) == 0) {
        nanCount++;
      }
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      if (id != null) {
        return Stream.of(new FieldMetrics(id.intValue(), 0L, 0L, nanCount, null, null));
      } else {
        return Stream.empty();
      }
    }
  }

  private static class DoubleWriter extends UnboxedWriter<Double> {
    private final ColumnDescriptor desc;
    private long nanCount;

    private DoubleWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
      this.desc = desc;
      this.nanCount = 0;
    }

    @Override
    public void write(int repetitionLevel, Double value) {
      super.write(repetitionLevel, value);
      if (Double.compare(Double.NaN, value) == 0) {
        nanCount++;
      }
    }

    @Override
    public Stream<FieldMetrics> metrics() {
      if (id != null) {
        return Stream.of(new FieldMetrics(id.intValue(), 0L, 0L, nanCount, null, null));
      } else {
        return Stream.empty();
      }
    }
  }

  private static class ByteWriter extends UnboxedWriter<Byte> {
    private ByteWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
    }

    @Override
    public void write(int repetitionLevel, Byte value) {
      writeInteger(repetitionLevel, value.intValue());
    }
  }

  private static class ShortWriter extends UnboxedWriter<Short> {
    private ShortWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
    }

    @Override
    public void write(int repetitionLevel, Short value) {
      writeInteger(repetitionLevel, value.intValue());
    }
  }

  private static class IntegerDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private IntegerDecimalWriter(ColumnDescriptor desc, Type.ID id, int precision, int scale) {
      super(desc, id);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeInteger(repetitionLevel, decimal.unscaledValue().intValue());
    }
  }

  private static class LongDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;

    private LongDecimalWriter(ColumnDescriptor desc, Type.ID id, int precision, int scale) {
      super(desc, id);
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      Preconditions.checkArgument(decimal.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, decimal);
      Preconditions.checkArgument(decimal.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, decimal);

      column.writeLong(repetitionLevel, decimal.unscaledValue().longValue());
    }
  }

  private static class FixedDecimalWriter extends PrimitiveWriter<BigDecimal> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private FixedDecimalWriter(ColumnDescriptor desc, Type.ID id, int precision, int scale) {
      super(desc, id);
      this.precision = precision;
      this.scale = scale;
      this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    public void write(int repetitionLevel, BigDecimal decimal) {
      byte[] binary = DecimalUtil.toReusedFixLengthBytes(precision, scale, decimal, bytes.get());
      column.writeBinary(repetitionLevel, Binary.fromReusedByteArray(binary));
    }
  }

  private static class BytesWriter extends PrimitiveWriter<ByteBuffer> {
    private BytesWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
    }

    @Override
    public void write(int repetitionLevel, ByteBuffer buffer) {
      column.writeBinary(repetitionLevel, Binary.fromReusedByteBuffer(buffer));
    }
  }

  private static class StringWriter extends PrimitiveWriter<CharSequence> {
    private StringWriter(ColumnDescriptor desc, Type.ID id) {
      super(desc, id);
    }

    @Override
    public void write(int repetitionLevel, CharSequence value) {
      if (value instanceof Utf8) {
        Utf8 utf8 = (Utf8) value;
        column.writeBinary(repetitionLevel,
            Binary.fromReusedByteArray(utf8.getBytes(), 0, utf8.getByteLength()));
      } else {
        column.writeBinary(repetitionLevel, Binary.fromString(value.toString()));
      }
    }
  }

  static class OptionWriter<T> implements ParquetValueWriter<T> {
    private final int definitionLevel;
    private final ParquetValueWriter<T> writer;
    private final List<TripleWriter<?>> children;

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
    public Stream<FieldMetrics> metrics() {
      return writer.metrics();
    }
  }

  public abstract static class RepeatedWriter<L, E> implements ParquetValueWriter<L> {
    private final int definitionLevel;
    private final int repetitionLevel;
    private final ParquetValueWriter<E> writer;
    private final List<TripleWriter<?>> children;

    protected RepeatedWriter(int definitionLevel, int repetitionLevel,
                             ParquetValueWriter<E> writer) {
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
    public Stream<FieldMetrics> metrics() {
      return writer.metrics();
    }
  }

  private static class CollectionWriter<E> extends RepeatedWriter<Collection<E>, E> {
    private CollectionWriter(int definitionLevel, int repetitionLevel,
                             ParquetValueWriter<E> writer) {
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

    protected RepeatedKeyValueWriter(int definitionLevel, int repetitionLevel,
                                     ParquetValueWriter<K> keyWriter,
                                     ParquetValueWriter<V> valueWriter) {
      this.definitionLevel = definitionLevel;
      this.repetitionLevel = repetitionLevel;
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
      this.children = ImmutableList.<TripleWriter<?>>builder()
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
    public Stream<FieldMetrics> metrics() {
      return Stream.of(keyWriter, valueWriter).flatMap(ParquetValueWriter::metrics);
    }
  }

  private static class MapWriter<K, V> extends RepeatedKeyValueWriter<Map<K, V>, K, V> {
    private MapWriter(int definitionLevel, int repetitionLevel,
                      ParquetValueWriter<K> keyWriter, ParquetValueWriter<V> valueWriter) {
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
      this.writers = (ParquetValueWriter<Object>[]) Array.newInstance(
          ParquetValueWriter.class, writers.size());

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
    public Stream<FieldMetrics> metrics() {
      return Arrays.stream(writers).flatMap(ParquetValueWriter::metrics);
    }
  }

  public static class PositionDeleteStructWriter<R> extends StructWriter<PositionDelete<R>> {
    public PositionDeleteStructWriter(StructWriter<?> replacedWriter) {
      super(Arrays.asList(replacedWriter.writers));
    }

    @Override
    protected Object get(PositionDelete<R> delete, int index) {
      switch (index) {
        case 0:
          return delete.path();
        case 1:
          return delete.pos();
        case 2:
          return delete.row();
      }
      throw new IllegalArgumentException("Cannot get value for invalid index: " + index);
    }
  }
}
