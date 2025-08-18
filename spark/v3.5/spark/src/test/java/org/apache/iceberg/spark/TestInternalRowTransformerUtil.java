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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.function.Function;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestInternalRowTransformerUtil {
  private static final StructType STRUCT_TYPE =
      new StructType(
          new StructField[] {
            new StructField("array", new ArrayType(DataTypes.ByteType, true), false, null),
            new StructField(
                "map", new MapType(DataTypes.ByteType, DataTypes.ByteType, true), true, null),
            new StructField(
                "struct",
                new StructType(
                    new StructField[] {
                      new StructField("struct_1", DataTypes.ByteType, true, null),
                      new StructField("struct_2", DataTypes.ByteType, true, null)
                    }),
                true,
                null),
            new StructField("byte", DataTypes.ByteType, false, null),
            new StructField("short", DataTypes.ShortType, false, null),
            new StructField("int", DataTypes.IntegerType, true, null)
          });

  @Test
  public void testPromotedSparkDataType() {
    InternalRow row = createRowToTransform();

    InternalRow transformed = InternalRowTransformerUtil.transformer(STRUCT_TYPE).apply(row);
    checkRow(transformed);
  }

  @Test
  public void testNullValues() {
    InternalRow row =
        new GenericInternalRow(new Object[] {null, null, null, (byte) 0x01, (short) -32768, null});

    InternalRow transformed = InternalRowTransformerUtil.transformer(STRUCT_TYPE).apply(row);
    assertThat(transformed.isNullAt(0)).isTrue();
    assertThat(transformed.getArray(0)).isNull();
    assertThat(transformed.isNullAt(1)).isTrue();
    assertThat(transformed.getMap(1)).isNull();
    assertThat(transformed.isNullAt(2)).isTrue();
    assertThat(transformed.getStruct(2, 2)).isNull();
    assertThat(transformed.isNullAt(3)).isFalse();
    assertThat(transformed.getInt(3)).isEqualTo(1);
    assertThat(transformed.isNullAt(3)).isFalse();
    assertThat(transformed.getInt(4)).isEqualTo(-32768);
    assertThat(transformed.isNullAt(5)).isTrue();
  }

  @Test
  public void testPositionDelete() {
    Function<PositionDelete<InternalRow>, InternalRow> transformer =
        InternalRowTransformerUtil.deleteTransformer();
    PositionDelete<InternalRow> delete = PositionDelete.create();
    InternalRow data = transformer.apply(delete.set("path", 1L));
    assertThat(data.getString(0)).isEqualTo("path");
    assertThat(data.getLong(1)).isEqualTo(1L);
    assertThat(data.getStruct(2, 0)).isNull();
    assertThat(data.isNullAt(2)).isTrue();
  }

  @Test
  public void testPositionDeleteWithRow() {
    Function<PositionDelete<InternalRow>, InternalRow> transformer =
        InternalRowTransformerUtil.deleteTransformer();
    PositionDelete<InternalRow> delete = PositionDelete.create();
    InternalRow row = createRowIntegers();
    InternalRow transformed = transformer.apply(delete.set("path", 1L, row));
    assertThat(transformed.getString(0)).isEqualTo("path");
    assertThat(transformed.getLong(1)).isEqualTo(1L);
    assertThat(transformed.isNullAt(2)).isFalse();
    checkRow(transformed.getStruct(2, row.numFields()));
  }

  private static InternalRow createRowToTransform() {
    return new GenericInternalRow(
        new Object[] {
          new GenericArrayData(new byte[] {(byte) 0x04, (byte) 0x05}),
          new ArrayBasedMapData(
              new GenericArrayData(new byte[] {(byte) 0x06}),
              new GenericArrayData(new byte[] {(byte) 0x07})),
          new GenericInternalRow(new Object[] {(byte) 0x08, (byte) 0x09}),
          (byte) 0x01,
          (short) -32768,
          101
        });
  }

  private static InternalRow createRowIntegers() {
    return new GenericInternalRow(
        new Object[] {
          new GenericArrayData(new int[] {0x04, 0x05}),
          new ArrayBasedMapData(
              new GenericArrayData(new int[] {0x06}), new GenericArrayData(new int[] {0x07})),
          new GenericInternalRow(new Object[] {0x08, 0x09}),
          0x01,
          -32768,
          101
        });
  }

  private static void checkRow(InternalRow row) {
    assertThat(row.getArray(0).getInt(0)).isEqualTo(4);
    assertThat(row.getArray(0).getInt(1)).isEqualTo(5);
    assertThat(row.getMap(1).keyArray().getInt(0)).isEqualTo(6);
    assertThat(row.getMap(1).valueArray().getInt(0)).isEqualTo(7);
    assertThat(row.getStruct(2, 2).getInt(0)).isEqualTo(8);
    assertThat(row.getStruct(2, 2).getInt(1)).isEqualTo(9);
    assertThat(row.getInt(3)).isEqualTo(1);
    assertThat(row.getInt(4)).isEqualTo(-32768);
    assertThat(row.getInt(5)).isEqualTo(101);
  }
}
