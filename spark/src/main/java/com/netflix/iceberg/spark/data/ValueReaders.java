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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;

class ValueReaders {
  private ValueReaders() {
  }

  static ValueReader<Object> nulls() {
    return NullReader.INSTANCE;
  }

  static ValueReader<Boolean> booleans() {
    return BooleanReader.INSTANCE;
  }

  static ValueReader<Integer> ints() {
    return IntegerReader.INSTANCE;
  }

  static ValueReader<Long> longs() {
    return LongReader.INSTANCE;
  }

  static ValueReader<Float> floats() {
    return FloatReader.INSTANCE;
  }

  static ValueReader<Double> doubles() {
    return DoubleReader.INSTANCE;
  }

  static ValueReader<UTF8String> strings() {
    return StringReader.INSTANCE;
  }

  static ValueReader<UTF8String> uuids() {
    return UUIDReader.INSTANCE;
  }

  static ValueReader<byte[]> fixed(int length) {
    return new FixedReader(length);
  }

  static ValueReader<byte[]> bytes() {
    return BytesReader.INSTANCE;
  }

  static ValueReader<Decimal> decimal(ValueReader<byte[]> unscaledReader, int scale) {
    return new DecimalReader(unscaledReader, scale);
  }

  static ValueReader<Object> union(List<ValueReader<?>> readers) {
    return new UnionReader(readers);
  }

  static ValueReader<ArrayData> array(ValueReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static ValueReader<ArrayBasedMapData> map(ValueReader<?> keyReader, ValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  static ValueReader<InternalRow> struct(List<ValueReader<?>> readers) {
    return new StructReader(readers);
  }

  private static class NullReader implements ValueReader<Object> {
    private static NullReader INSTANCE = new NullReader();

    private NullReader() {
    }

    @Override
    public Object read(Decoder decoder) throws IOException {
      decoder.readNull();
      return null;
    }
  }

  private static class BooleanReader implements ValueReader<Boolean> {
    private static BooleanReader INSTANCE = new BooleanReader();

    private BooleanReader() {
    }

    @Override
    public Boolean read(Decoder decoder) throws IOException {
      return decoder.readBoolean();
    }
  }

  private static class IntegerReader implements ValueReader<Integer> {
    private static IntegerReader INSTANCE = new IntegerReader();

    private IntegerReader() {
    }

    @Override
    public Integer read(Decoder decoder) throws IOException {
      return decoder.readInt();
    }
  }

  private static class LongReader implements ValueReader<Long> {
    private static LongReader INSTANCE = new LongReader();

    private LongReader() {
    }

    @Override
    public Long read(Decoder decoder) throws IOException {
      return decoder.readLong();
    }
  }

  private static class FloatReader implements ValueReader<Float> {
    private static FloatReader INSTANCE = new FloatReader();

    private FloatReader() {
    }

    @Override
    public Float read(Decoder decoder) throws IOException {
      return decoder.readFloat();
    }
  }

  private static class DoubleReader implements ValueReader<Double> {
    private static DoubleReader INSTANCE = new DoubleReader();

    private DoubleReader() {
    }

    @Override
    public Double read(Decoder decoder) throws IOException {
      return decoder.readDouble();
    }
  }

  private static class StringReader implements ValueReader<UTF8String> {
    private static StringReader INSTANCE = new StringReader();

    private StringReader() {
    }

    @Override
    public UTF8String read(Decoder decoder) throws IOException {
      // use the decoder's readString(Utf8) method because it may be a resolving decoder
      Utf8 string = decoder.readString(null);
      return UTF8String.fromBytes(string.getBytes(), 0, string.getByteLength());
//      int length = decoder.readInt();
//      byte[] bytes = new byte[length];
//      decoder.readFixed(bytes, 0, length);
//      return UTF8String.fromBytes(bytes);
    }
  }

  private static class UUIDReader implements ValueReader<UTF8String> {
    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    });

    private static UUIDReader INSTANCE = new UUIDReader();

    private UUIDReader() {
    }

    @Override
    public UTF8String read(Decoder decoder) throws IOException {
      ByteBuffer buffer = BUFFER.get();
      buffer.rewind();

      decoder.readFixed(buffer.array(), 0, 16);
      long mostSigBits = buffer.getLong();
      long leastSigBits = buffer.getLong();

      return UTF8String.fromString(new UUID(mostSigBits, leastSigBits).toString());
    }
  }

  private static class FixedReader implements ValueReader<byte[]> {
    private final int length;

    private FixedReader(int length) {
      this.length = length;
    }

    @Override
    public byte[] read(Decoder decoder) throws IOException {
      byte[] bytes = new byte[length];
      decoder.readFixed(bytes, 0, length);
      return bytes;
    }
  }

  private static class BytesReader implements ValueReader<byte[]> {
    private static BytesReader INSTANCE = new BytesReader();

    private BytesReader() {
    }

    @Override
    public byte[] read(Decoder decoder) throws IOException {
      // use the decoder's readBytes method because it may be a resolving decoder
      return decoder.readBytes(null).array();
//      int length = decoder.readInt();
//      byte[] bytes = new byte[length];
//      decoder.readFixed(bytes, 0, length);
//      return bytes;
    }
  }

  private static class DecimalReader implements ValueReader<Decimal> {
    private final ValueReader<byte[]> bytesReader;
    private final int scale;

    private DecimalReader(ValueReader<byte[]> bytesReader, int scale) {
      this.bytesReader = bytesReader;
      this.scale = scale;
    }

    @Override
    public Decimal read(Decoder decoder) throws IOException {
      byte[] bytes = bytesReader.read(decoder);
      return Decimal.apply(new BigDecimal(new BigInteger(bytes), scale));
    }
  }

  private static class UnionReader implements ValueReader<Object> {
    private final ValueReader[] readers;

    private UnionReader(List<ValueReader<?>> readers) {
      this.readers = new ValueReader[readers.size()];
      for (int i = 0; i < this.readers.length; i += 1) {
        this.readers[i] = readers.get(i);
      }
    }

    @Override
    public Object read(Decoder decoder) throws IOException {
      int index = decoder.readIndex();
      return readers[index].read(decoder);
    }
  }

  private static class EnumReader implements ValueReader<UTF8String> {
    private final UTF8String[] symbols;

    private EnumReader(List<String> symbols) {
      this.symbols = new UTF8String[symbols.size()];
      for (int i = 0; i < this.symbols.length; i += 1) {
        this.symbols[i] = UTF8String.fromString(symbols.get(i));
      }
    }

    @Override
    public UTF8String read(Decoder decoder) throws IOException {
      int index = decoder.readEnum();
      return symbols[index];
    }
  }

  private static class ArrayReader implements ValueReader<ArrayData> {
    private final ValueReader<?> elementReader;
    private final List<Object> reusedList = Lists.newArrayList();

    private ArrayReader(ValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public GenericArrayData read(Decoder decoder) throws IOException {
      reusedList.clear();
      long chunkLength = decoder.readArrayStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedList.add(elementReader.read(decoder));
        }

        chunkLength = decoder.arrayNext();
      }

      // this will convert the list to an array so it is okay to reuse the list
      return new GenericArrayData(reusedList.toArray());
    }
  }

  private static class MapReader implements ValueReader<ArrayBasedMapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private MapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public ArrayBasedMapData read(Decoder decoder) throws IOException {
      reusedKeyList.clear();
      reusedValueList.clear();

      long chunkLength = decoder.readMapStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedKeyList.add(keyReader.read(decoder));
          reusedValueList.add(valueReader.read(decoder));
        }

        chunkLength = decoder.mapNext();
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  private static class StructReader implements ValueReader<InternalRow> {
    private final ValueReader<?>[] readers;

    private StructReader(List<ValueReader<?>> readers) {
      this.readers = new ValueReader[readers.size()];
      for (int i = 0; i < this.readers.length; i += 1) {
        this.readers[i] = readers.get(i);
      }
    }

    @Override
    public InternalRow read(Decoder decoder) throws IOException {
      GenericInternalRow row = new GenericInternalRow(readers.length);
      if (decoder instanceof ResolvingDecoder) {
        // this may not set all of the fields. nulls are set by default.
        for (Schema.Field field : ((ResolvingDecoder) decoder).readFieldOrder()) {
          Object value = readers[field.pos()].read(decoder);
          if (value != null) {
            row.update(field.pos(), value);
          } else {
            row.setNullAt(field.pos());
          }
        }

      } else {
        for (int i = 0; i < readers.length; i += 1) {
          Object value = readers[i].read(decoder);
          if (value != null) {
            row.update(i, value);
          } else {
            row.setNullAt(i);
          }
        }
      }

      return row;
    }
  }
}
