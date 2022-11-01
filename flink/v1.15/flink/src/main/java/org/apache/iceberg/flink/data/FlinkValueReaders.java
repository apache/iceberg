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
package org.apache.iceberg.flink.data;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.avro.ValueReader;
import org.apache.iceberg.avro.ValueReaders;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;

public class FlinkValueReaders {

  private FlinkValueReaders() {}

  static ValueReader<StringData> strings() {
    return StringReader.INSTANCE;
  }

  static ValueReader<StringData> enums(List<String> symbols) {
    return new EnumReader(symbols);
  }

  static ValueReader<byte[]> uuids() {
    return ValueReaders.fixed(16);
  }

  static ValueReader<Integer> timeMicros() {
    return TimeMicrosReader.INSTANCE;
  }

  static ValueReader<TimestampData> timestampMills() {
    return TimestampMillsReader.INSTANCE;
  }

  static ValueReader<TimestampData> timestampMicros() {
    return TimestampMicrosReader.INSTANCE;
  }

  static ValueReader<DecimalData> decimal(
      ValueReader<byte[]> unscaledReader, int precision, int scale) {
    return new DecimalReader(unscaledReader, precision, scale);
  }

  static ValueReader<ArrayData> array(ValueReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static ValueReader<MapData> arrayMap(ValueReader<?> keyReader, ValueReader<?> valueReader) {
    return new ArrayMapReader(keyReader, valueReader);
  }

  static ValueReader<MapData> map(ValueReader<?> keyReader, ValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  static ValueReader<RowData> struct(
      List<ValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  private static class StringReader implements ValueReader<StringData> {
    private static final StringReader INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public StringData read(Decoder decoder, Object reuse) throws IOException {
      // use the decoder's readString(Utf8) method because it may be a resolving decoder
      Utf8 utf8 = null;
      if (reuse instanceof StringData) {
        utf8 = new Utf8(((StringData) reuse).toBytes());
      }

      Utf8 string = decoder.readString(utf8);
      return StringData.fromBytes(string.getBytes(), 0, string.getByteLength());
    }
  }

  private static class EnumReader implements ValueReader<StringData> {
    private final StringData[] symbols;

    private EnumReader(List<String> symbols) {
      this.symbols = new StringData[symbols.size()];
      for (int i = 0; i < this.symbols.length; i += 1) {
        this.symbols[i] = StringData.fromBytes(symbols.get(i).getBytes(StandardCharsets.UTF_8));
      }
    }

    @Override
    public StringData read(Decoder decoder, Object ignore) throws IOException {
      int index = decoder.readEnum();
      return symbols[index];
    }
  }

  private static class DecimalReader implements ValueReader<DecimalData> {
    private final ValueReader<byte[]> bytesReader;
    private final int precision;
    private final int scale;

    private DecimalReader(ValueReader<byte[]> bytesReader, int precision, int scale) {
      this.bytesReader = bytesReader;
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public DecimalData read(Decoder decoder, Object reuse) throws IOException {
      byte[] bytes = bytesReader.read(decoder, null);
      return DecimalData.fromBigDecimal(
          new BigDecimal(new BigInteger(bytes), scale), precision, scale);
    }
  }

  private static class TimeMicrosReader implements ValueReader<Integer> {
    private static final TimeMicrosReader INSTANCE = new TimeMicrosReader();

    @Override
    public Integer read(Decoder decoder, Object reuse) throws IOException {
      long micros = decoder.readLong();
      // Flink only support time mills, just erase micros.
      return (int) (micros / 1000);
    }
  }

  private static class TimestampMillsReader implements ValueReader<TimestampData> {
    private static final TimestampMillsReader INSTANCE = new TimestampMillsReader();

    @Override
    public TimestampData read(Decoder decoder, Object reuse) throws IOException {
      return TimestampData.fromEpochMillis(decoder.readLong());
    }
  }

  private static class TimestampMicrosReader implements ValueReader<TimestampData> {
    private static final TimestampMicrosReader INSTANCE = new TimestampMicrosReader();

    @Override
    public TimestampData read(Decoder decoder, Object reuse) throws IOException {
      long micros = decoder.readLong();
      long mills = micros / 1000;
      int nanos = ((int) (micros % 1000)) * 1000;
      if (nanos < 0) {
        nanos += 1_000_000;
        mills -= 1;
      }
      return TimestampData.fromEpochMillis(mills, nanos);
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

  private static MapData kvArrayToMap(List<Object> keyList, List<Object> valueList) {
    Map<Object, Object> map = Maps.newHashMap();
    Object[] keys = keyList.toArray();
    Object[] values = valueList.toArray();
    for (int i = 0; i < keys.length; i++) {
      map.put(keys[i], values[i]);
    }

    return new GenericMapData(map);
  }

  private static class ArrayMapReader implements ValueReader<MapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private ArrayMapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public MapData read(Decoder decoder, Object reuse) throws IOException {
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

      return kvArrayToMap(reusedKeyList, reusedValueList);
    }
  }

  private static class MapReader implements ValueReader<MapData> {
    private final ValueReader<?> keyReader;
    private final ValueReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private MapReader(ValueReader<?> keyReader, ValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public MapData read(Decoder decoder, Object reuse) throws IOException {
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

      return kvArrayToMap(reusedKeyList, reusedValueList);
    }
  }

  private static class StructReader extends ValueReaders.StructReader<RowData> {
    private final int numFields;

    private StructReader(
        List<ValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.numFields = readers.size();
    }

    @Override
    protected RowData reuseOrCreate(Object reuse) {
      if (reuse instanceof GenericRowData && ((GenericRowData) reuse).getArity() == numFields) {
        return (GenericRowData) reuse;
      }
      return new GenericRowData(numFields);
    }

    @Override
    protected Object get(RowData struct, int pos) {
      return null;
    }

    @Override
    protected void set(RowData struct, int pos, Object value) {
      ((GenericRowData) struct).setField(pos, value);
    }
  }
}
