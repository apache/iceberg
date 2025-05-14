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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.orc.OrcValueReader;
import org.apache.iceberg.orc.OrcValueReaders;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
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

public class SparkOrcValueReaders {
  private SparkOrcValueReaders() {}

  public static OrcValueReader<UTF8String> utf8String() {
    return StringReader.INSTANCE;
  }

  public static OrcValueReader<UTF8String> uuids() {
    return UUIDReader.INSTANCE;
  }

  public static OrcValueReader<Long> timestampTzs() {
    return TimestampTzReader.INSTANCE;
  }

  public static OrcValueReader<Decimal> decimals(int precision, int scale) {
    if (precision <= Decimal.MAX_LONG_DIGITS()) {
      return new SparkOrcValueReaders.Decimal18Reader(precision, scale);
    } else if (precision <= 38) {
      return new SparkOrcValueReaders.Decimal38Reader(precision, scale);
    } else {
      throw new IllegalArgumentException("Invalid precision: " + precision);
    }
  }

  static OrcValueReader<?> struct(
      List<OrcValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
    return new StructReader(readers, struct, idToConstant);
  }

  static OrcValueReader<?> array(OrcValueReader<?> elementReader) {
    return new ArrayReader(elementReader);
  }

  static OrcValueReader<?> map(OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
    return new MapReader(keyReader, valueReader);
  }

  private static class ArrayReader implements OrcValueReader<ArrayData> {
    private final OrcValueReader<?> elementReader;

    private ArrayReader(OrcValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public ArrayData nonNullRead(ColumnVector vector, int row) {
      ListColumnVector listVector = (ListColumnVector) vector;
      int offset = (int) listVector.offsets[row];
      int length = (int) listVector.lengths[row];
      List<Object> elements = Lists.newArrayListWithExpectedSize(length);
      for (int c = 0; c < length; ++c) {
        elements.add(elementReader.read(listVector.child, offset + c));
      }
      return new GenericArrayData(elements.toArray());
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      elementReader.setBatchContext(batchOffsetInFile);
    }
  }

  private static class MapReader implements OrcValueReader<MapData> {
    private final OrcValueReader<?> keyReader;
    private final OrcValueReader<?> valueReader;

    private MapReader(OrcValueReader<?> keyReader, OrcValueReader<?> valueReader) {
      this.keyReader = keyReader;
      this.valueReader = valueReader;
    }

    @Override
    public MapData nonNullRead(ColumnVector vector, int row) {
      MapColumnVector mapVector = (MapColumnVector) vector;
      int offset = (int) mapVector.offsets[row];
      long length = mapVector.lengths[row];
      List<Object> keys = Lists.newArrayListWithExpectedSize((int) length);
      List<Object> values = Lists.newArrayListWithExpectedSize((int) length);
      for (int c = 0; c < length; c++) {
        keys.add(keyReader.read(mapVector.keys, offset + c));
        values.add(valueReader.read(mapVector.values, offset + c));
      }

      return new ArrayBasedMapData(
          new GenericArrayData(keys.toArray()), new GenericArrayData(values.toArray()));
    }

    @Override
    public void setBatchContext(long batchOffsetInFile) {
      keyReader.setBatchContext(batchOffsetInFile);
      valueReader.setBatchContext(batchOffsetInFile);
    }
  }

  static class StructReader extends OrcValueReaders.StructReader<InternalRow> {
    private final int numFields;

    protected StructReader(
        List<OrcValueReader<?>> readers, Types.StructType struct, Map<Integer, ?> idToConstant) {
      super(readers, struct, idToConstant);
      this.numFields = struct.fields().size();
    }

    @Override
    protected InternalRow create() {
      return new GenericInternalRow(numFields);
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

  private static class StringReader implements OrcValueReader<UTF8String> {
    private static final StringReader INSTANCE = new StringReader();

    private StringReader() {}

    @Override
    public UTF8String nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      return UTF8String.fromBytes(
          bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
    }
  }

  private static class UUIDReader implements OrcValueReader<UTF8String> {
    private static final UUIDReader INSTANCE = new UUIDReader();

    private UUIDReader() {}

    @Override
    public UTF8String nonNullRead(ColumnVector vector, int row) {
      BytesColumnVector bytesVector = (BytesColumnVector) vector;
      ByteBuffer buffer =
          ByteBuffer.wrap(bytesVector.vector[row], bytesVector.start[row], bytesVector.length[row]);
      return UTF8String.fromString(UUIDUtil.convert(buffer).toString());
    }
  }

  private static class TimestampTzReader implements OrcValueReader<Long> {
    private static final TimestampTzReader INSTANCE = new TimestampTzReader();

    private TimestampTzReader() {}

    @Override
    public Long nonNullRead(ColumnVector vector, int row) {
      TimestampColumnVector tcv = (TimestampColumnVector) vector;
      return Math.floorDiv(tcv.time[row], 1_000) * 1_000_000 + Math.floorDiv(tcv.nanos[row], 1000);
    }
  }

  private static class Decimal18Reader implements OrcValueReader<Decimal> {
    private final int precision;
    private final int scale;

    Decimal18Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal nonNullRead(ColumnVector vector, int row) {
      HiveDecimalWritable value = ((DecimalColumnVector) vector).vector[row];

      // The scale of decimal read from hive ORC file may be not equals to the expected scale. For
      // data type
      // decimal(10,3) and the value 10.100, the hive ORC writer will remove its trailing zero and
      // store it
      // as 101*10^(-1), its scale will adjust from 3 to 1. So here we could not assert that
      // value.scale() == scale.
      // we also need to convert the hive orc decimal to a decimal with expected precision and
      // scale.
      Preconditions.checkArgument(
          value.precision() <= precision,
          "Cannot read value as decimal(%s,%s), too large: %s",
          precision,
          scale,
          value);

      return new Decimal().set(value.serialize64(scale), precision, scale);
    }
  }

  private static class Decimal38Reader implements OrcValueReader<Decimal> {
    private final int precision;
    private final int scale;

    Decimal38Reader(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override
    public Decimal nonNullRead(ColumnVector vector, int row) {
      BigDecimal value =
          ((DecimalColumnVector) vector).vector[row].getHiveDecimal().bigDecimalValue();

      Preconditions.checkArgument(
          value.precision() <= precision,
          "Cannot read value as decimal(%s,%s), too large: %s",
          precision,
          scale,
          value);

      return new Decimal().set(new scala.math.BigDecimal(value), precision, scale);
    }
  }
}
