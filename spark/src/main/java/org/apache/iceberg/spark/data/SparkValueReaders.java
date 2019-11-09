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

package org.apache.iceberg.spark.data;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.ValueReader;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkValueReaders {

  private SparkValueReaders() {}

  static ValueReader<UTF8String> strings() {
    return StringReader.INSTANCE;
  }

  static ValueReader<UTF8String> enums(List<String> symbols) {
    return new EnumReader(symbols);
  }

  static ValueReader<UTF8String> uuids() {
    return UUIDReader.INSTANCE;
  }

  static ValueReader<Decimal> decimal(ValueReader<byte[]> unscaledReader, int scale) {
    return new DecimalReader(unscaledReader, scale);
  }

  static ValueReader<ArrayData> array(ValueReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static ValueReader<ArrayBasedMapData> arrayMap(ValueReader<?> keyReader,
                                                 ValueReader<?> valueReader) {
    return new ArrayMapReader(keyReader, valueReader);
  }

  static ValueReader<ArrayBasedMapData> map(ValueReader<?> keyReader, ValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  static ValueReader<InternalRow> struct(List<ValueReader<?>> readers) {
    return new StructReader(readers);
  }

  private static class StringReader implements ValueReader<UTF8String> {
    private static final StringReader INSTANCE = new StringReader();

    private StringReader() {
    }

    @Override
    public UTF8String read(Decoder decoder, Object reuse) throws IOException {
      // use the decoder's readString(Utf8) method because it may be a resolving decoder
      Utf8 utf8 = null;
      if (reuse instanceof UTF8String) {
        utf8 = new Utf8(((UTF8String) reuse).getBytes());
      }

      Utf8 string = decoder.readString(utf8);
      return UTF8String.fromBytes(string.getBytes(), 0, string.getByteLength());
//      int length = decoder.readInt();
//      byte[] bytes = new byte[length];
//      decoder.readFixed(bytes, 0, length);
//      return UTF8String.fromBytes(bytes);
    }
  }

  private static class EnumReader implements ValueReader<UTF8String> {
    private final UTF8String[] symbols;

    private EnumReader(List<String> symbols) {
      this.symbols = new UTF8String[symbols.size()];
      for (int i = 0; i < this.symbols.length; i += 1) {
        this.symbols[i] = UTF8String.fromBytes(symbols.get(i).getBytes(StandardCharsets.UTF_8));
      }
    }

    @Override
    public UTF8String read(Decoder decoder, Object ignore) throws IOException {
      int index = decoder.readEnum();
      return symbols[index];
    }
  }

  private static class UUIDReader implements ValueReader<UTF8String> {
    private static final ThreadLocal<ByteBuffer> BUFFER = ThreadLocal.withInitial(() -> {
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.order(ByteOrder.BIG_ENDIAN);
      return buffer;
    });

    private static final UUIDReader INSTANCE = new UUIDReader();

    private UUIDReader() {
    }

    @Override
    public UTF8String read(Decoder decoder, Object reuse) throws IOException {
      ByteBuffer buffer = BUFFER.get();
      buffer.rewind();

      decoder.readFixed(buffer.array(), 0, 16);
      long mostSigBits = buffer.getLong();
      long leastSigBits = buffer.getLong();

      return UTF8String.fromString(new UUID(mostSigBits, leastSigBits).toString());
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
    public Decimal read(Decoder decoder, Object reuse) throws IOException {
      byte[] bytes = bytesReader.read(decoder, null);
      return Decimal.apply(new BigDecimal(new BigInteger(bytes), scale));
    }
  }

  private static class ArrayReader implements ValueReader<ArrayData> {
    private final ValueReader<?> elementReader;
    private final List<Object> reusedList = Lists.newArrayList();

    private ArrayReader(ValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public GenericArrayData read(Decoder decoder, Object reuse) throws IOException {
      reusedList.clear();
      long chunkLength = decoder.readArrayStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedList.add(elementReader.read(decoder, null));
        }

        chunkLength = decoder.arrayNext();
      }

      // this will convert the list to an array so it is okay to reuse the list
      return new GenericArrayData(reusedList.toArray());
    }
  }

  private static class ArrayMapReader implements ValueReader<ArrayBasedMapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private ArrayMapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public ArrayBasedMapData read(Decoder decoder, Object reuse) throws IOException {
      reusedKeyList.clear();
      reusedValueList.clear();

      long chunkLength = decoder.readArrayStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedKeyList.add(keyReader.read(decoder, null));
          reusedValueList.add(valueReader.read(decoder, null));
        }

        chunkLength = decoder.arrayNext();
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
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
    public ArrayBasedMapData read(Decoder decoder, Object reuse) throws IOException {
      reusedKeyList.clear();
      reusedValueList.clear();

      long chunkLength = decoder.readMapStart();

      while (chunkLength > 0) {
        for (int i = 0; i < chunkLength; i += 1) {
          reusedKeyList.add(keyReader.read(decoder, null));
          reusedValueList.add(valueReader.read(decoder, null));
        }

        chunkLength = decoder.mapNext();
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  static class StructReader implements ValueReader<InternalRow> {
    private final ValueReader<?>[] readers;

    private StructReader(List<ValueReader<?>> readers) {
      this.readers = new ValueReader[readers.size()];
      for (int i = 0; i < this.readers.length; i += 1) {
        this.readers[i] = readers.get(i);
      }
    }

    ValueReader<?>[] readers() {
      return readers;
    }

    @Override
    public InternalRow read(Decoder decoder, Object reuse) throws IOException {
      GenericInternalRow row = new GenericInternalRow(readers.length);
      if (decoder instanceof ResolvingDecoder) {
        // this may not set all of the fields. nulls are set by default.
        for (Schema.Field field : ((ResolvingDecoder) decoder).readFieldOrder()) {
          Object value = readers[field.pos()].read(decoder, null);
          if (value != null) {
            row.update(field.pos(), value);
          } else {
            row.setNullAt(field.pos());
          }
        }

      } else {
        for (int i = 0; i < readers.length; i += 1) {
          Object value = readers[i].read(decoder, null);
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
