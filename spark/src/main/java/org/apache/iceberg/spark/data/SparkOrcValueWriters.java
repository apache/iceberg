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

  static SparkOrcValueWriter booleans() {
    return BooleanWriter.INSTANCE;
  }

  static SparkOrcValueWriter bytes() {
    return ByteWriter.INSTANCE;
  }

  static SparkOrcValueWriter shorts() {
    return ShortWriter.INSTANCE;
  }

  static SparkOrcValueWriter ints() {
    return IntWriter.INSTANCE;
  }

  static SparkOrcValueWriter longs() {
    return LongWriter.INSTANCE;
  }

  static SparkOrcValueWriter floats(int id) {
    return new FloatWriter(id);
  }

  static SparkOrcValueWriter doubles(int id) {
    return new DoubleWriter(id);
  }

  static SparkOrcValueWriter byteArrays() {
    return BytesWriter.INSTANCE;
  }

  static SparkOrcValueWriter strings() {
    return StringWriter.INSTANCE;
  }

  static SparkOrcValueWriter timestampTz() {
    return TimestampTzWriter.INSTANCE;
  }

  static SparkOrcValueWriter decimal(int precision, int scale) {
    if (precision <= 18) {
      return new Decimal18Writer(precision, scale);
    } else {
      return new Decimal38Writer(precision, scale);
    }
  }

  static SparkOrcValueWriter list(SparkOrcValueWriter element, List<TypeDescription> orcType) {
    return new ListWriter(element, orcType);
  }

  static SparkOrcValueWriter map(SparkOrcValueWriter keyWriter, SparkOrcValueWriter valueWriter,
      List<TypeDescription> orcType) {
    return new MapWriter(keyWriter, valueWriter, orcType);
  }

  private static class BooleanWriter implements SparkOrcValueWriter<Boolean> {
    private static final BooleanWriter INSTANCE = new BooleanWriter();

    @Override
    public void nonNullWrite(int rowId, Boolean data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data ? 1 : 0;
    }
  }

  private static class ByteWriter implements SparkOrcValueWriter<Byte> {
    private static final ByteWriter INSTANCE = new ByteWriter();

    @Override
    public void nonNullWrite(int rowId, Byte data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class ShortWriter implements SparkOrcValueWriter<Short> {
    private static final ShortWriter INSTANCE = new ShortWriter();

    @Override
    public void nonNullWrite(int rowId, Short data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class IntWriter implements SparkOrcValueWriter<Integer> {
    private static final IntWriter INSTANCE = new IntWriter();

    @Override
    public void nonNullWrite(int rowId, Integer data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class LongWriter implements SparkOrcValueWriter<Long> {
    private static final LongWriter INSTANCE = new LongWriter();

    @Override
    public void nonNullWrite(int rowId, Long data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data;
    }
  }

  private static class FloatWriter implements SparkOrcValueWriter<Float> {
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
  }

  private static class DoubleWriter implements SparkOrcValueWriter<Double> {
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
  }

  private static class BytesWriter implements SparkOrcValueWriter<byte[]> {
    private static final BytesWriter INSTANCE = new BytesWriter();

    @Override
    public void nonNullWrite(int rowId, byte[] value, ColumnVector output) {
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class StringWriter implements SparkOrcValueWriter<UTF8String> {
    private static final StringWriter INSTANCE = new StringWriter();

    @Override
    public void nonNullWrite(int rowId, UTF8String data, ColumnVector output) {
      byte[] value = data.getBytes();
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class TimestampTzWriter implements SparkOrcValueWriter<Long> {
    private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

    @Override
    public void nonNullWrite(int rowId, Long micros, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      cv.time[rowId] = Math.floorDiv(micros, 1_000); // millis
      cv.nanos[rowId] = (int) Math.floorMod(micros, 1_000_000) * 1_000; // nanos
    }
  }

  private static class Decimal18Writer implements SparkOrcValueWriter<Decimal> {
    private final int precision;
    private final int scale;

    Decimal18Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, Decimal decimal, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(
          decimal.toUnscaledLong(), scale);
    }
  }

  private static class Decimal38Writer implements SparkOrcValueWriter<Decimal> {
    private final int precision;
    private final int scale;

    Decimal38Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, Decimal decimal, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].set(
          HiveDecimal.create(decimal.toJavaBigDecimal()));
    }
  }

  private static class ListWriter implements SparkOrcValueWriter<ArrayData> {
    private final SparkOrcValueWriter writer;
    private final SparkOrcWriter.FieldGetter fieldGetter;

    ListWriter(SparkOrcValueWriter writer, List<TypeDescription> orcTypes) {
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
  }

  private static class MapWriter implements SparkOrcValueWriter<MapData> {
    private final SparkOrcValueWriter keyWriter;
    private final SparkOrcValueWriter valueWriter;
    private final SparkOrcWriter.FieldGetter keyFieldGetter;
    private final SparkOrcWriter.FieldGetter valueFieldGetter;

    MapWriter(SparkOrcValueWriter keyWriter, SparkOrcValueWriter valueWriter, List<TypeDescription> orcTypes) {
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
  }

  private static void growColumnVector(ColumnVector cv, int requestedSize) {
    if (cv.isNull.length < requestedSize) {
      // Use growth factor of 3 to avoid frequent array allocations
      cv.ensureSize(requestedSize * 3, true);
    }
  }
}
