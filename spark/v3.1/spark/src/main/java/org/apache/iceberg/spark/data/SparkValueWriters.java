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

import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class SparkValueWriters {

  private SparkValueWriters() {
  }

  static ValueWriter<UTF8String> strings() {
    return StringWriter.INSTANCE;
  }

  static ValueWriter<UTF8String> uuids() {
    return UUIDWriter.INSTANCE;
  }

  static ValueWriter<Decimal> decimal(int precision, int scale) {
    return new DecimalWriter(precision, scale);
  }

  static <T> ValueWriter<ArrayData> array(ValueWriter<T> elementWriter, DataType elementType) {
    return new ArrayWriter<>(elementWriter, elementType);
  }

  static <K, V> ValueWriter<MapData> arrayMap(
      ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
    return new ArrayMapWriter<>(keyWriter, keyType, valueWriter, valueType);
  }

  static <K, V> ValueWriter<MapData> map(
      ValueWriter<K> keyWriter, DataType keyType, ValueWriter<V> valueWriter, DataType valueType) {
    return new MapWriter<>(keyWriter, keyType, valueWriter, valueType);
  }

  static ValueWriter<InternalRow> struct(List<ValueWriter<?>> writers, List<DataType> types) {
    return new StructWriter(writers, types);
  }

  private static class StringWriter implements ValueWriter<UTF8String> {
    private static final StringWriter INSTANCE = new StringWriter();

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

    private static final UUIDWriter INSTANCE = new UUIDWriter();

    private UUIDWriter() {
    }

    @Override
    @SuppressWarnings("ByteBufferBackingArray")
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

  private static class DecimalWriter implements ValueWriter<Decimal> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private DecimalWriter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
      this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    public void write(Decimal d, Encoder encoder) throws IOException {
      encoder.writeFixed(DecimalUtil.toReusedFixLengthBytes(precision, scale, d.toJavaBigDecimal(), bytes.get()));
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

  private static class ArrayMapWriter<K, V> implements ValueWriter<MapData> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;
    private final DataType keyType;
    private final DataType valueType;

    private ArrayMapWriter(ValueWriter<K> keyWriter, DataType keyType,
                           ValueWriter<V> valueWriter, DataType valueType) {
      this.keyWriter = keyWriter;
      this.keyType = keyType;
      this.valueWriter = valueWriter;
      this.valueType = valueType;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(MapData map, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = map.numElements();
      encoder.setItemCount(numElements);
      ArrayData keyArray = map.keyArray();
      ArrayData valueArray = map.valueArray();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        keyWriter.write((K) keyArray.get(i, keyType), encoder);
        valueWriter.write((V) valueArray.get(i, valueType), encoder);
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

  static class StructWriter implements ValueWriter<InternalRow> {
    private final ValueWriter<?>[] writers;
    private final DataType[] types;

    @SuppressWarnings("unchecked")
    private StructWriter(List<ValueWriter<?>> writers, List<DataType> types) {
      this.writers = (ValueWriter<?>[]) Array.newInstance(ValueWriter.class, writers.size());
      this.types = new DataType[writers.size()];
      for (int i = 0; i < writers.size(); i += 1) {
        this.writers[i] = writers.get(i);
        this.types[i] = types.get(i);
      }
    }

    ValueWriter<?>[] writers() {
      return writers;
    }

    @Override
    public void write(InternalRow row, Encoder encoder) throws IOException {
      for (int i = 0; i < types.length; i += 1) {
        if (row.isNullAt(i)) {
          writers[i].write(null, encoder);
        } else {
          write(row, i, writers[i], encoder);
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
