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
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.orc.OrcValReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.types.Types;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.ListColumnVector;
import org.apache.orc.storage.ql.exec.vector.MapColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;


class SparkOrcValueReaders {
  private SparkOrcValueReaders() {
  }

  static OrcValReader<UTF8String> strings() {
    return StringReader.INSTANCE;
  }

  static OrcValReader<?> timestampTzs() {
    return TimestampTzReader.INSTANCE;
  }

  static OrcValReader<?> struct(
      List<OrcValReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  static OrcValReader<?> array(OrcValReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static OrcValReader<?> map(OrcValReader<?> keyReader, OrcValReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  private static class ArrayReader implements OrcValReader<ArrayData> {
    private final OrcValReader<?> elementReader;
    private final List<Object> reusedList = Lists.newArrayList();

    private ArrayReader(OrcValReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public ArrayData nonNullRead(ColumnVector vector, int row) {
      reusedList.clear();
      ListColumnVector listVector = (ListColumnVector) vector;
      int offset = (int) listVector.offsets[row];
      int length = (int) listVector.lengths[row];
      for (int c = 0; c < length; ++c) {
        reusedList.add(elementReader.read(listVector.child, offset + c));
      }
      return new GenericArrayData(reusedList.toArray());
    }
  }

  private static class MapReader implements OrcValReader<MapData> {
    private final OrcValReader<?> keyReader;
    private final OrcValReader<?> valueReader;

    private final List<Object> reusedKeyList = Lists.newArrayList();
    private final List<Object> reusedValueList = Lists.newArrayList();

    private MapReader(OrcValReader<?> keyReader, OrcValReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public MapData nonNullRead(ColumnVector vector, int row) {
      reusedKeyList.clear();
      reusedValueList.clear();
      MapColumnVector mapVector = (MapColumnVector) vector;
      int offset = (int) mapVector.offsets[row];
      long length = mapVector.lengths[row];
      for (int c = 0; c < length; c++) {
        reusedKeyList.add(keyReader.read(mapVector.keys, offset + c));
        reusedValueList.add(valueReader.read(mapVector.values, offset + c));
      }

      return new ArrayBasedMapData(
          new GenericArrayData(reusedKeyList.toArray()),
          new GenericArrayData(reusedValueList.toArray()));
    }
  }

  static class StructReader extends OrcValueReaders.StructReader<InternalRow> {
    private final int numFields;
    private InternalRow internalRow;

    protected StructReader(List<OrcValReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.numFields = readers.size();
    }

    @Override
    protected InternalRow create() {
      return new GenericInternalRow(numFields);
    }

    @Override
    protected InternalRow reuseOrCreate() {
      if (internalRow == null) {
        internalRow = new GenericInternalRow(numFields);
      }
      return internalRow;
    }

    @Override
    protected void set(InternalRow struct, int pos, Object value) {
      if (value != null) {
        struct.update(pos, value);
      } else {
        struct.setNullAt(pos);
      }
    }
  }

  private static class StringReader implements OrcValReader<UTF8String> {
    private static final StringReader INSTANCE = new StringReader();

    private StringReader() {
    }

    @Override
    public UTF8String nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return UTF8String.fromBytes(bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
    }
  }

  private static class TimestampTzReader implements OrcValReader<Long> {
    private static final TimestampTzReader INSTANCE = new TimestampTzReader();

    private TimestampTzReader() {
    }

    @Override
    public Long nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector timestampVector = (TimestampColumnVector) vector;
      return (timestampVector.time[row] / 1000) * 1_000_000 + timestampVector.nanos[row] / 1000;
    }
  }

  static class Decimal18Reader implements OrcValReader<Decimal> {
    //TODO: these are being unused. check for bug
    private final int precision;
    private final int scale;

    Decimal18Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal nonNullRead(ColumnVector vector, int row) {
      HiveDecimalWritable value = ((DecimalColumnVector) vector).vector[row];
      return new Decimal().set(value.serialize64(value.scale()), value.precision(), value.scale());
    }
  }

  static class Decimal38Reader implements OrcValReader<Decimal> {
    private final int precision;
    private final int scale;

    Decimal38Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal nonNullRead(ColumnVector vector, int row) {
      BigDecimal value = ((DecimalColumnVector) vector).vector[row]
          .getHiveDecimal().bigDecimalValue();
      return new Decimal().set(new scala.math.BigDecimal(value), precision, scale);
    }
  }
}
