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
package org.apache.iceberg.spark.source;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.formats.StitchLayout;
import org.apache.iceberg.formats.Stitcher;
import org.apache.iceberg.formats.StitcherBuilder;
import org.apache.iceberg.formats.StitcherRegistry;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.BinaryView;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class SparkStitchers {
  private SparkStitchers() {}

  public static void register() {
    StitcherRegistry.register(new InternalRowStitcherBuilder());
  }

  private static class InternalRowStitcherBuilder implements StitcherBuilder<InternalRow> {
    @Override
    public Class<InternalRow> type() {
      return InternalRow.class;
    }

    @Override
    public Stitcher<InternalRow> build(Schema projection, List<Schema> verticalSplits) {
      StitchLayout layout = StitchLayout.of(projection, verticalSplits);
      StructType type = SparkSchemaUtil.convert(projection);
      return (rows, count) ->
          new StitchedInternalRow(type, layout, rows.toArray(new InternalRow[0]));
    }
  }

  /** A row whose fields are read from the rows of the parts it was stitched from. */
  private static class StitchedInternalRow extends InternalRow {
    private final StructType type;
    private final StitchLayout layout;
    private final InternalRow[] parts;

    private StitchedInternalRow(StructType type, StitchLayout layout, InternalRow[] parts) {
      this.type = type;
      this.layout = layout;
      this.parts = parts;
    }

    private InternalRow part(int pos) {
      return parts[layout.split(pos)];
    }

    private int ordinal(int pos) {
      return layout.ordinal(pos);
    }

    @Override
    public int numFields() {
      return layout.size();
    }

    @Override
    public void setNullAt(int pos) {
      part(pos).setNullAt(ordinal(pos));
    }

    @Override
    public void update(int pos, Object value) {
      part(pos).update(ordinal(pos), value);
    }

    @Override
    public InternalRow copy() {
      StructField[] fields = type.fields();
      Object[] values = new Object[fields.length];
      for (int pos = 0; pos < fields.length; pos += 1) {
        values[pos] = get(pos, fields[pos].dataType());
      }

      return new GenericInternalRow(values).copy();
    }

    @Override
    public boolean isNullAt(int pos) {
      return part(pos).isNullAt(ordinal(pos));
    }

    @Override
    public boolean getBoolean(int pos) {
      return part(pos).getBoolean(ordinal(pos));
    }

    @Override
    public byte getByte(int pos) {
      return part(pos).getByte(ordinal(pos));
    }

    @Override
    public short getShort(int pos) {
      return part(pos).getShort(ordinal(pos));
    }

    @Override
    public int getInt(int pos) {
      return part(pos).getInt(ordinal(pos));
    }

    @Override
    public long getLong(int pos) {
      return part(pos).getLong(ordinal(pos));
    }

    @Override
    public float getFloat(int pos) {
      return part(pos).getFloat(ordinal(pos));
    }

    @Override
    public double getDouble(int pos) {
      return part(pos).getDouble(ordinal(pos));
    }

    @Override
    public Decimal getDecimal(int pos, int precision, int scale) {
      return part(pos).getDecimal(ordinal(pos), precision, scale);
    }

    @Override
    public UTF8String getUTF8String(int pos) {
      return part(pos).getUTF8String(ordinal(pos));
    }

    @Override
    public byte[] getBinary(int pos) {
      return part(pos).getBinary(ordinal(pos));
    }

    @Override
    public BinaryView getBinaryView(int pos) {
      return part(pos).getBinaryView(ordinal(pos));
    }

    @Override
    public CalendarInterval getInterval(int pos) {
      return part(pos).getInterval(ordinal(pos));
    }

    @Override
    public VariantVal getVariant(int pos) {
      return part(pos).getVariant(ordinal(pos));
    }

    @Override
    public InternalRow getStruct(int pos, int numFields) {
      return part(pos).getStruct(ordinal(pos), numFields);
    }

    @Override
    public ArrayData getArray(int pos) {
      return part(pos).getArray(ordinal(pos));
    }

    @Override
    public MapData getMap(int pos) {
      return part(pos).getMap(ordinal(pos));
    }

    @Override
    public Object get(int pos, DataType dataType) {
      return part(pos).get(ordinal(pos), dataType);
    }
  }
}
