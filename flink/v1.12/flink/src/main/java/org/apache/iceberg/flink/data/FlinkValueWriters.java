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
import java.lang.reflect.Array;
import java.util.List;
import org.apache.avro.io.Encoder;
import org.apache.avro.util.Utf8;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.util.DecimalUtil;

public class FlinkValueWriters {

  private FlinkValueWriters() {
  }

  static ValueWriter<StringData> strings() {
    return StringWriter.INSTANCE;
  }

  static ValueWriter<Integer> timeMicros() {
    return TimeMicrosWriter.INSTANCE;
  }

  static ValueWriter<TimestampData> timestampMicros() {
    return TimestampMicrosWriter.INSTANCE;
  }

  static ValueWriter<DecimalData> decimal(int precision, int scale) {
    return new DecimalWriter(precision, scale);
  }

  static <T> ValueWriter<ArrayData> array(ValueWriter<T> elementWriter, LogicalType elementType) {
    return new ArrayWriter<>(elementWriter, elementType);
  }

  static <K, V> ValueWriter<MapData> arrayMap(ValueWriter<K> keyWriter, LogicalType keyType,
                                              ValueWriter<V> valueWriter, LogicalType valueType) {
    return new ArrayMapWriter<>(keyWriter, keyType, valueWriter, valueType);
  }

  static <K, V> ValueWriter<MapData> map(ValueWriter<K> keyWriter, LogicalType keyType,
                                         ValueWriter<V> valueWriter, LogicalType valueType) {
    return new MapWriter<>(keyWriter, keyType, valueWriter, valueType);
  }

  static ValueWriter<RowData> row(List<ValueWriter<?>> writers, List<LogicalType> types) {
    return new RowWriter(writers, types);
  }

  private static class StringWriter implements ValueWriter<StringData> {
    private static final StringWriter INSTANCE = new StringWriter();

    private StringWriter() {
    }

    @Override
    public void write(StringData s, Encoder encoder) throws IOException {
      // toBytes is cheaper than Avro calling toString, which incurs encoding costs
      encoder.writeString(new Utf8(s.toBytes()));
    }
  }

  private static class DecimalWriter implements ValueWriter<DecimalData> {
    private final int precision;
    private final int scale;
    private final ThreadLocal<byte[]> bytes;

    private DecimalWriter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
      this.bytes = ThreadLocal.withInitial(() -> new byte[TypeUtil.decimalRequiredBytes(precision)]);
    }

    @Override
    public void write(DecimalData d, Encoder encoder) throws IOException {
      encoder.writeFixed(DecimalUtil.toReusedFixLengthBytes(precision, scale, d.toBigDecimal(), bytes.get()));
    }
  }

  private static class TimeMicrosWriter implements ValueWriter<Integer> {
    private static final TimeMicrosWriter INSTANCE = new TimeMicrosWriter();

    @Override
    public void write(Integer timeMills, Encoder encoder) throws IOException {
      encoder.writeLong(timeMills * 1000);
    }
  }

  private static class TimestampMicrosWriter implements ValueWriter<TimestampData> {
    private static final TimestampMicrosWriter INSTANCE = new TimestampMicrosWriter();

    @Override
    public void write(TimestampData timestampData, Encoder encoder) throws IOException {
      long micros = timestampData.getMillisecond() * 1000 + timestampData.getNanoOfMillisecond() / 1000;
      encoder.writeLong(micros);
    }
  }

  private static class ArrayWriter<T> implements ValueWriter<ArrayData> {
    private final ValueWriter<T> elementWriter;
    private final ArrayData.ElementGetter elementGetter;

    private ArrayWriter(ValueWriter<T> elementWriter, LogicalType elementType) {
      this.elementWriter = elementWriter;
      this.elementGetter = ArrayData.createElementGetter(elementType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(ArrayData array, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = array.size();
      encoder.setItemCount(numElements);
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        elementWriter.write((T) elementGetter.getElementOrNull(array, i), encoder);
      }
      encoder.writeArrayEnd();
    }
  }

  private static class ArrayMapWriter<K, V> implements ValueWriter<MapData> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    private ArrayMapWriter(ValueWriter<K> keyWriter, LogicalType keyType,
                           ValueWriter<V> valueWriter, LogicalType valueType) {
      this.keyWriter = keyWriter;
      this.keyGetter = ArrayData.createElementGetter(keyType);
      this.valueWriter = valueWriter;
      this.valueGetter = ArrayData.createElementGetter(valueType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(MapData map, Encoder encoder) throws IOException {
      encoder.writeArrayStart();
      int numElements = map.size();
      encoder.setItemCount(numElements);
      ArrayData keyArray = map.keyArray();
      ArrayData valueArray = map.valueArray();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        keyWriter.write((K) keyGetter.getElementOrNull(keyArray, i), encoder);
        valueWriter.write((V) valueGetter.getElementOrNull(valueArray, i), encoder);
      }
      encoder.writeArrayEnd();
    }
  }

  private static class MapWriter<K, V> implements ValueWriter<MapData> {
    private final ValueWriter<K> keyWriter;
    private final ValueWriter<V> valueWriter;
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;

    private MapWriter(ValueWriter<K> keyWriter, LogicalType keyType,
                      ValueWriter<V> valueWriter, LogicalType valueType) {
      this.keyWriter = keyWriter;
      this.keyGetter = ArrayData.createElementGetter(keyType);
      this.valueWriter = valueWriter;
      this.valueGetter = ArrayData.createElementGetter(valueType);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(MapData map, Encoder encoder) throws IOException {
      encoder.writeMapStart();
      int numElements = map.size();
      encoder.setItemCount(numElements);
      ArrayData keyArray = map.keyArray();
      ArrayData valueArray = map.valueArray();
      for (int i = 0; i < numElements; i += 1) {
        encoder.startItem();
        keyWriter.write((K) keyGetter.getElementOrNull(keyArray, i), encoder);
        valueWriter.write((V) valueGetter.getElementOrNull(valueArray, i), encoder);
      }
      encoder.writeMapEnd();
    }
  }

  static class RowWriter implements ValueWriter<RowData> {
    private final ValueWriter<?>[] writers;
    private final RowData.FieldGetter[] getters;

    private RowWriter(List<ValueWriter<?>> writers, List<LogicalType> types) {
      this.writers = (ValueWriter<?>[]) Array.newInstance(ValueWriter.class, writers.size());
      this.getters = new RowData.FieldGetter[writers.size()];
      for (int i = 0; i < writers.size(); i += 1) {
        this.writers[i] = writers.get(i);
        this.getters[i] = RowData.createFieldGetter(types.get(i), i);
      }
    }

    @Override
    public void write(RowData row, Encoder encoder) throws IOException {
      for (int i = 0; i < writers.length; i += 1) {
        if (row.isNullAt(i)) {
          writers[i].write(null, encoder);
        } else {
          write(row, i, writers[i], encoder);
        }
      }
    }

    @SuppressWarnings("unchecked")
    private <T> void write(RowData row, int pos, ValueWriter<T> writer, Encoder encoder)
        throws IOException {
      writer.write((T) getters[pos].getFieldOrNull(row), encoder);
    }
  }
}
