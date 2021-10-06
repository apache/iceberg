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

import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.DoubleFieldMetrics;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FloatFieldMetrics;
import org.apache.iceberg.orc.OrcValueWriter;
import org.apache.orc.TypeDescription;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

class SparkOrcValueWriters {
  private SparkOrcValueWriters() {
  }

  static OrcValueWriter<?> booleans() {
    return BooleanWriter.INSTANCE;
  }

  static OrcValueWriter<?> bytes() {
    return ByteWriter.INSTANCE;
  }

  static OrcValueWriter<?> shorts() {
    return ShortWriter.INSTANCE;
  }

  static OrcValueWriter<?> ints() {
    return IntWriter.INSTANCE;
  }

  static OrcValueWriter<?> longs() {
    return LongWriter.INSTANCE;
  }

  static OrcValueWriter<?> floats(int id) {
    return new FloatWriter(id);
  }

  static OrcValueWriter<?> doubles(int id) {
    return new DoubleWriter(id);
  }

  static OrcValueWriter<?> byteArrays() {
    return BytesWriter.INSTANCE;
  }

  static OrcValueWriter<?> strings() {
    return StringWriter.INSTANCE;
  }

  static OrcValueWriter<?> timestampTz() {
    return TimestampTzWriter.INSTANCE;
  }

  static OrcValueWriter<?> decimal(int precision, int scale) {
    if (precision <= 18) {
      return new Decimal18Writer(scale);
    } else {
      return new Decimal38Writer();
    }
  }

  static OrcValueWriter<?> list(OrcValueWriter<?> element, List<TypeDescription> orcType) {
    return new ListWriter(element, orcType);
  }

  static OrcValueWriter<?> map(OrcValueWriter<?> keyWriter, OrcValueWriter<?> valueWriter,
      List<TypeDescription> orcType) {
    return new MapWriter(keyWriter, valueWriter, orcType);
  }

  private static class BooleanWriter implements OrcValueWriter<Boolean> {
    private static final BooleanWriter INSTANCE = new BooleanWriter();

    @Override
    public void nonNullWrite(int rowId, Boolean data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data ? 1 : 0;
    }

    @Override
    public Class<?> getJavaClass() {
      return Boolean.class;
    }
  }

  private static class ByteWriter implements OrcValueWriter<Byte> {
    private static final ByteWriter INSTANCE = new ByteWriter();

    @Override
    public void nonNullWrite(int rowId, Byte data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }

    @Override
    public Class<?> getJavaClass() {
      return Byte.class;
    }
  }

  private static class ShortWriter implements OrcValueWriter<Short> {
    private static final ShortWriter INSTANCE = new ShortWriter();

    @Override
    public void nonNullWrite(int rowId, Short data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }

    @Override
    public Class<?> getJavaClass() {
      return Short.class;
    }
  }

  private static class IntWriter implements OrcValueWriter<Integer> {
    private static final IntWriter INSTANCE = new IntWriter();

    @Override
    public void nonNullWrite(int rowId, Integer data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }

    @Override
    public Class<?> getJavaClass() {
      return Integer.class;
    }
  }

  private static class LongWriter implements OrcValueWriter<Long> {
    private static final LongWriter INSTANCE = new LongWriter();

    @Override
    public void nonNullWrite(int rowId, Long data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }

    @Override
    public Class<?> getJavaClass() {
      return Long.class;
    }
  }

  private static class FloatWriter implements OrcValueWriter<Float> {
    private final FloatFieldMetrics.Builder floatFieldMetricsBuilder;

    private FloatWriter(int id) {
      this.floatFieldMetricsBuilder = new FloatFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, Float floatValue, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = floatValue;
      floatFieldMetricsBuilder.addValue(floatValue);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(floatFieldMetricsBuilder.build());
    }
    @Override
    public Class<?> getJavaClass() {
      return Float.class;
    }
  }

  private static class DoubleWriter implements OrcValueWriter<Double> {
    private final DoubleFieldMetrics.Builder doubleFieldMetricsBuilder;

    private DoubleWriter(int id) {
      this.doubleFieldMetricsBuilder = new DoubleFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, Double doubleValue, ColumnVector output) {
      ((DoubleColumnVector) output).vector[rowId] = doubleValue;
      doubleFieldMetricsBuilder.addValue(doubleValue);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(doubleFieldMetricsBuilder.build());
    }

    @Override
    public Class<?> getJavaClass() {
      return Double.class;
    }
  }

  private static class BytesWriter implements OrcValueWriter<byte[]> {
    private static final BytesWriter INSTANCE = new BytesWriter();

    @Override
    public void nonNullWrite(int rowId, byte[] value, ColumnVector output) {
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }

    @Override
    public Class<?> getJavaClass() {
      return byte[].class;
    }
  }

  private static class StringWriter implements OrcValueWriter<UTF8String> {
    private static final StringWriter INSTANCE = new StringWriter();

    @Override
    public void nonNullWrite(int rowId, UTF8String data, ColumnVector output) {
      byte[] value = data.getBytes();
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }

    @Override
    public Class<?> getJavaClass() {
      return UTF8String.class;
    }
  }

  private static class TimestampTzWriter implements OrcValueWriter<Long> {
    private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

    @Override
    public void nonNullWrite(int rowId, Long micros, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      cv.time[rowId] = Math.floorDiv(micros, 1_000); // millis
      cv.nanos[rowId] = (int) Math.floorMod(micros, 1_000_000) * 1_000; // nanos
    }

    @Override
    public Class<?> getJavaClass() {
      return Long.class;
    }
  }

  private static class Decimal18Writer implements OrcValueWriter<Decimal> {
    private final int scale;

    Decimal18Writer(int scale) {
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, Decimal decimal, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(
          decimal.toUnscaledLong(), scale);
    }

    @Override
    public Class<?> getJavaClass() {
      return Decimal.class;
    }
  }

  private static class Decimal38Writer implements OrcValueWriter<Decimal> {

    @Override
    public void nonNullWrite(int rowId, Decimal decimal, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].set(
          HiveDecimal.create(decimal.toJavaBigDecimal()));
    }

    @Override
    public Class<?> getJavaClass() {
      return Decimal.class;
    }
  }

  private static class ListWriter implements OrcValueWriter<ArrayData> {
    private final OrcValueWriter writer;
    private final SparkOrcWriter.FieldGetter fieldGetter;

    ListWriter(OrcValueWriter<?> writer, List<TypeDescription> orcTypes) {
      if (orcTypes.size() != 1) {
        throw new IllegalArgumentException("Expected one (and same) ORC type for list elements, got: " + orcTypes);
      }
      this.writer = writer;
      this.fieldGetter = SparkOrcWriter.createFieldGetter(orcTypes.get(0), 0, 1);
    }

    @Override
    public void nonNullWrite(int rowId, ArrayData value, ColumnVector output) {
      ListColumnVector cv = (ListColumnVector) output;
      // record the length and start of the list elements
      cv.lengths[rowId] = value.numElements();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
      // make sure the child is big enough
      growColumnVector(cv.child, cv.childCount);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        writer.write((int) (e + cv.offsets[rowId]), fieldGetter.getFieldOrNull(value, e), cv.child);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writer.metrics();
    }

    @Override
    public Class<?> getJavaClass() {
      return ArrayData.class;
    }
  }

  private static class MapWriter implements OrcValueWriter<MapData> {
    private final OrcValueWriter keyWriter;
    private final OrcValueWriter valueWriter;
    private final SparkOrcWriter.FieldGetter keyFieldGetter;
    private final SparkOrcWriter.FieldGetter valueFieldGetter;

    MapWriter(OrcValueWriter<?> keyWriter, OrcValueWriter<?> valueWriter, List<TypeDescription> orcTypes) {
      if (orcTypes.size() != 2) {
        throw new IllegalArgumentException("Expected two ORC type descriptions for a map, got: " + orcTypes);
      }
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
      this.keyFieldGetter = SparkOrcWriter.createFieldGetter(orcTypes.get(0), 0, 1);
      this.valueFieldGetter = SparkOrcWriter.createFieldGetter(orcTypes.get(1), 0, 1);
    }

    @Override
    public void nonNullWrite(int rowId, MapData map, ColumnVector output) {
      ArrayData key = map.keyArray();
      ArrayData value = map.valueArray();
      MapColumnVector cv = (MapColumnVector) output;
      // record the length and start of the list elements
      cv.lengths[rowId] = value.numElements();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
      // make sure the child is big enough
      growColumnVector(cv.keys, cv.childCount);
      growColumnVector(cv.values, cv.childCount);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        int pos = (int) (e + cv.offsets[rowId]);
        keyWriter.write(pos, keyFieldGetter.getFieldOrNull(key, e), cv.keys);
        valueWriter.write(pos, valueFieldGetter.getFieldOrNull(value, e), cv.values);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.concat(keyWriter.metrics(), valueWriter.metrics());
    }

    @Override
    public Class<?> getJavaClass() {
      return MapData.class;
    }
  }

  private static void growColumnVector(ColumnVector cv, int requestedSize) {
    if (cv.isNull.length < requestedSize) {
      // Use growth factor of 3 to avoid frequent array allocations
      cv.ensureSize(requestedSize * 3, true);
    }
  }
}
