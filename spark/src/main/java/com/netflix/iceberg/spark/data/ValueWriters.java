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

package com.netflix.iceberg.spark.data;

import com.google.common.base.Preconditions;
import com.netflix.iceberg.types.TypeUtil;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;

 class ValueWriters {
  private ValueWriters() {
  }

  static ValueWriter<Void> nulls() {
    return NullWriter.INSTANCE;
  }

  static ValueWriter<Boolean> booleans() {
    return BooleanWriter.INSTANCE;
  }

  static ValueWriter<Integer> ints() {
    return IntegerWriter.INSTANCE;
  }

  static ValueWriter<Long> longs() {
    return LongWriter.INSTANCE;
  }

  static ValueWriter<Float> floats() {
    return FloatWriter.INSTANCE;
  }

  static ValueWriter<Double> doubles() {
    return DoubleWriter.INSTANCE;
  }

  static ValueWriter<UTF8String> strings() {
    return StringWriter.INSTANCE;
  }

  static ValueWriter<UTF8String> uuids() {
    return UUIDWriter.INSTANCE;
  }

  static ValueWriter<byte[]> fixed(int length) {
    return new FixedWriter(length);
  }

  static ValueWriter<byte[]> bytes() {
    return BytesWriter.INSTANCE;
  }

  static ValueWriter<Decimal> decimal(int precision, int scale) {
    return new DecimalWriter(precision, scale);
  }

  static <T> ValueWriter<T> option(int nullIndex, ValueWriter<T> writer) {
    return new OptionWriter<>(nullIndex, writer);
  }

  static <T> ValueWriter<ArrayData> array(ValueWriter<T> elementWriter, DataType elementType) {
    return new ArrayWriter<>(elementWriter, elementType);
  }

  static <K, V> ValueWriter<MapData> map(
      ValueWriter<K> keyReader, DataType keyType, ValueWriter<V> valueReader, DataType valueType) {
    return new MapWriter<>(keyReader, keyType, valueReader, valueType);
  }

  static ValueWriter<InternalRow> struct(List<ValueWriter<?>> writers, List<DataType> types) {
    return new StructWriter(writers, types);
  }

  private static class NullWriter implements ValueWriter<Void> {
    private static NullWriter INSTANCE = new NullWriter();

    private NullWriter() {
    }

    @Override
    public void write(Void ignored, Encoder encoder) throws IOException {
      encoder.writeNull();
    }
  }

  private static class BooleanWriter implements ValueWriter<Boolean> {
    private static BooleanWriter INSTANCE = new BooleanWriter();

    private BooleanWriter() {
    }

    @Override
    public void write(Boolean bool, Encoder encoder) throws IOException {
      encoder.writeBoolean(bool);
    }
  }

  private static class IntegerWriter implements ValueWriter<Integer> {
    private static IntegerWriter INSTANCE = new IntegerWriter();

    private IntegerWriter() {
    }

    @Override
    public void write(Integer i, Encoder encoder) throws IOException {
      encoder.writeInt(i);
    }
  }

  private static class LongWriter implements ValueWriter<Long> {
    private static LongWriter INSTANCE = new LongWriter();

    private LongWriter() {
    }

    @Override
    public void write(Long l, Encoder encoder) throws IOException {
      encoder.writeLong(l);
    }
  }

  private static class FloatWriter implements ValueWriter<Float> {
    private static FloatWriter INSTANCE = new FloatWriter();

    private FloatWriter() {
    }

    @Override
    public void write(Float f, Encoder encoder) throws IOException {
      encoder.writeFloat(f);
    }
  }

  private static class DoubleWriter implements ValueWriter<Double> {
    private static DoubleWriter INSTANCE = new DoubleWriter();

    private DoubleWriter() {
    }

    @Override
    public void write(Double d, Encoder encoder) throws IOException {
      encoder.writeDouble(d);
    }
  }

  private static class StringWriter implements ValueWriter<UTF8String> {
    private static StringWriter INSTANCE = new StringWriter();

    private StringWriter() {
    }

    @Override
    public void write(UTF8String s, Encoder encoder) throws IOException {
      // use getBytes because it may return the backing byte array if available.
      // otherwise, it copies to a new byte array, which is still cheaper than Avro
      // calling toString, which incurs encoding costs
      encoder.writeString(new Utf8(s.getBytes()));
    }
  }

  private static class UUIDWriter implements ValueWriter<UTF8String> {
    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    });

    private static UUIDWriter INSTANCE = new UUIDWriter();

    private UUIDWriter() {
    }

    @Override
    public void write(UTF8String s, Encoder encoder) throws IOException {
      // TODO: direct conversion from string to byte buffer
      UUID uuid = UUID.fromString(s.toString());
      ByteBuffer buffer = BUFFER.get();
      buffer.rewind();
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      encoder.writeFixed(buffer.array());
    }
  }

  private static class FixedWriter implements ValueWriter<byte[]> {
    private final int length;

    private FixedWriter(int length) {
      this.length = length;
    }

    @Override
    public void write(byte[] bytes, Encoder encoder) throws IOException {
      Preconditions.checkArgument(bytes.length == length,
          "Cannot write byte array of length %s as fixed[%s]", bytes.length, length);
      encoder.writeFixed(bytes);
    }
  }

  private static class BytesWriter implements ValueWriter<byte[]> {
    private static BytesWriter INSTANCE = new BytesWriter();

    private BytesWriter() {
    }

    @Override
    public void write(byte[] bytes, Encoder encoder) throws IOException {
      encoder.writeBytes(bytes);
    }
  }

  private static class DecimalWriter implements ValueWriter<Decimal> {
    private final int precision;
    private final int scale;
    private final int length;
    private final ThreadLocal<byte[]> bytes;

    private DecimalWriter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
      this.length = TypeUtil.decimalRequriedBytes(precision);
      this.bytes = ThreadLocal.withInitial(() -> new byte[length]);
    }

    @Override
    public void write(Decimal d, Encoder encoder) throws IOException {
      Preconditions.checkArgument(d.scale() == scale,
          "Cannot write value as decimal(%s,%s), wrong scale: %s", precision, scale, d);
      Preconditions.checkArgument(d.precision() <= precision,
          "Cannot write value as decimal(%s,%s), too large: %s", precision, scale, d);

      BigDecimal decimal = d.toJavaBigDecimal();

      byte fillByte = (byte) (decimal.signum() < 0 ? 0xFF : 0x00);
      byte[] unscaled = decimal.unscaledValue().toByteArray();
      byte[] buf = bytes.get();
      int offset = length - unscaled.length;

      for (int i = 0; i < length; i += 1) {
        if (i < offset) {
          buf[i] = fillByte;
        } else {
          buf[i] = unscaled[i - offset];
        }
      }

      encoder.writeFixed(buf);
    }
  }

  private static class OptionWriter<T> implements ValueWriter<T> {
    private final int nullIndex;
    private final int valueIndex;
    private final ValueWriter<T> valueWriter;

    private OptionWriter(int nullIndex, ValueWriter<T> valueWriter) {
      this.nullIndex = nullIndex;
      if (nullIndex == 0) {
        this.valueIndex = 1;
      } else if (nullIndex == 1) {
        this.valueIndex = 0;
      } else {
        throw new IllegalArgumentException("Invalid option index: " + nullIndex);
      }
      this.valueWriter = valueWriter;
    }

    @Override
    public void write(T option, Encoder encoder) throws IOException {
      if (option == null) {
        encoder.writeIndex(nullIndex);
      } else {
        encoder.writeIndex(valueIndex);
        valueWriter.write(option, encoder);
      }
    }
  }

  private static class ArrayWriter<T> implements ValueWriter<ArrayData> {
    private final ValueWriter<T> elementWriter;
    private final DataType elementType;

    private ArrayWriter(ValueWriter<T> elementWriter, DataType elementType) {
      this.elementWriter = elementWriter;
      this.elementType = elementType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(ArrayData array, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = array.numElements();
      encoder.setItemCount(numElements);
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        elementWriter.write((T) array.get(i, elementType), encoder);
      }
      encoder.writeArrayEnd();
    }
  }

  private static class MapWriter<K, V> implements ValueWriter<MapData> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;
    private final DataType keyType;
    private final DataType valueType;

    private MapWriter(ValueWriter<K> keyWriter, DataType keyType,
                      ValueWriter<V> valueWriter, DataType valueType) {
      this.keyWriter = keyWriter;
      this.keyType = keyType;
      this.valueWriter = valueWriter;
      this.valueType = valueType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(MapData map, Encoder encoder) throws IOException {
      encoder.writeMapStart();
      int numElements = map.numElements();
      encoder.setItemCount(numElements);
      ArrayData keyArray = map.keyArray();
      ArrayData valueArray = map.valueArray();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        keyWriter.write((K) keyArray.get(i, keyType), encoder);
        valueWriter.write((V) valueArray.get(i, valueType), encoder);
      }
      encoder.writeMapEnd();
    }
  }

  private static class StructWriter implements ValueWriter<InternalRow> {
    private final List<ValueWriter<?>> writers;
    private final DataType[] types;

    private StructWriter(List<ValueWriter<?>> writers, List<DataType> types) {
      this.writers = writers;
      this.types = new DataType[writers.size()];
      for (int i = 0; i < writers.size(); i += 1) {
        this.types[i] = types.get(i);
      }
    }

    @Override
    public void write(InternalRow row, Encoder encoder) throws IOException {
      for (int i = 0; i < types.length; i += 1) {
        if (row.isNullAt(i)) {
          writers.get(i).write(null, encoder);
        } else {
          write(row, i, writers.get(i), encoder);
        }
      }
    }

    @SuppressWarnings("unchecked")
    private <T> void write(InternalRow row, int pos, ValueWriter<T> writer, Encoder encoder)
        throws IOException {
      writer.write((T) row.get(pos, types[pos]), encoder);
    }
  }
}
