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

import java.util.stream.Stream;
import org.apache.iceberg.DoubleFieldMetrics;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.FloatFieldMetrics;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;

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

  static SparkOrcValueWriter list(SparkOrcValueWriter element) {
    return new ListWriter(element);
  }

  static SparkOrcValueWriter map(SparkOrcValueWriter keyWriter, SparkOrcValueWriter valueWriter) {
    return new MapWriter(keyWriter, valueWriter);
  }

  private static class BooleanWriter implements SparkOrcValueWriter {
    private static final BooleanWriter INSTANCE = new BooleanWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.getBoolean(column) ? 1 : 0;
    }
  }

  private static class ByteWriter implements SparkOrcValueWriter {
    private static final ByteWriter INSTANCE = new ByteWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.getByte(column);
    }
  }

  private static class ShortWriter implements SparkOrcValueWriter {
    private static final ShortWriter INSTANCE = new ShortWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.getShort(column);
    }
  }

  private static class IntWriter implements SparkOrcValueWriter {
    private static final IntWriter INSTANCE = new IntWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.getInt(column);
    }
  }

  private static class LongWriter implements SparkOrcValueWriter {
    private static final LongWriter INSTANCE = new LongWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((LongColumnVector) output).vector[rowId] = data.getLong(column);
    }
  }

  private static class FloatWriter implements SparkOrcValueWriter {
    private final FloatFieldMetrics.Builder floatFieldMetricsBuilder;

    private FloatWriter(int id) {
      this.floatFieldMetricsBuilder = new FloatFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      float floatValue = data.getFloat(column);
      ((DoubleColumnVector) output).vector[rowId] = floatValue;
      floatFieldMetricsBuilder.addValue(floatValue);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(floatFieldMetricsBuilder.build());
    }
  }

  private static class DoubleWriter implements SparkOrcValueWriter {
    private final DoubleFieldMetrics.Builder doubleFieldMetricsBuilder;

    private DoubleWriter(int id) {
      this.doubleFieldMetricsBuilder = new DoubleFieldMetrics.Builder(id);
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      double doubleValue = data.getDouble(column);
      ((DoubleColumnVector) output).vector[rowId] = doubleValue;
      doubleFieldMetricsBuilder.addValue(doubleValue);
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return Stream.of(doubleFieldMetricsBuilder.build());
    }
  }

  private static class BytesWriter implements SparkOrcValueWriter {
    private static final BytesWriter INSTANCE = new BytesWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      // getBinary always makes a copy, so we don't need to worry about it
      // being changed behind our back.
      byte[] value = data.getBinary(column);
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class StringWriter implements SparkOrcValueWriter {
    private static final StringWriter INSTANCE = new StringWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      byte[] value = data.getUTF8String(column).getBytes();
      ((BytesColumnVector) output).setRef(rowId, value, 0, value.length);
    }
  }

  private static class TimestampTzWriter implements SparkOrcValueWriter {
    private static final TimestampTzWriter INSTANCE = new TimestampTzWriter();

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      TimestampColumnVector cv = (TimestampColumnVector) output;
      long micros = data.getLong(column); // it could be negative.
      cv.time[rowId] = Math.floorDiv(micros, 1_000); // millis
      cv.nanos[rowId] = (int) Math.floorMod(micros, 1_000_000) * 1_000; // nanos
    }
  }

  private static class Decimal18Writer implements SparkOrcValueWriter {
    private final int precision;
    private final int scale;

    Decimal18Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].setFromLongAndScale(
          data.getDecimal(column, precision, scale).toUnscaledLong(), scale);
    }
  }

  private static class Decimal38Writer implements SparkOrcValueWriter {
    private final int precision;
    private final int scale;

    Decimal38Writer(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ((DecimalColumnVector) output).vector[rowId].set(
          HiveDecimal.create(data.getDecimal(column, precision, scale)
              .toJavaBigDecimal()));
    }
  }

  private static class ListWriter implements SparkOrcValueWriter {
    private final SparkOrcValueWriter writer;

    ListWriter(SparkOrcValueWriter writer) {
      this.writer = writer;
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      ArrayData value = data.getArray(column);
      ListColumnVector cv = (ListColumnVector) output;
      // record the length and start of the list elements
      cv.lengths[rowId] = value.numElements();
      cv.offsets[rowId] = cv.childCount;
      cv.childCount = (int) (cv.childCount + cv.lengths[rowId]);
      // make sure the child is big enough
      growColumnVector(cv.child, cv.childCount);
      // Add each element
      for (int e = 0; e < cv.lengths[rowId]; ++e) {
        writer.write((int) (e + cv.offsets[rowId]), e, value, cv.child);
      }
    }

    @Override
    public Stream<FieldMetrics<?>> metrics() {
      return writer.metrics();
    }
  }

  private static class MapWriter implements SparkOrcValueWriter {
    private final SparkOrcValueWriter keyWriter;
    private final SparkOrcValueWriter valueWriter;

    MapWriter(SparkOrcValueWriter keyWriter, SparkOrcValueWriter valueWriter) {
      this.keyWriter = keyWriter;
      this.valueWriter = valueWriter;
    }

    @Override
    public void nonNullWrite(int rowId, int column, SpecializedGetters data, ColumnVector output) {
      MapData map = data.getMap(column);
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
        keyWriter.write(pos, e, key, cv.keys);
        valueWriter.write(pos, e, value, cv.values);
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
