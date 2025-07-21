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

import org.apache.iceberg.data.RowTransformer;
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

public class TestInternalRowTransformer {
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
  private static final RowTransformer<InternalRow> TRANSFORMER =
      new InternalRowTransformer(STRUCT_TYPE);

  @Test
  public void testPromotedSparkDataType() {
    InternalRow row =
        new GenericInternalRow(
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

    InternalRow transformed = TRANSFORMER.transform(row);
    assertThat(transformed.getArray(0).getInt(0)).isEqualTo(4);
    assertThat(transformed.getArray(0).getInt(1)).isEqualTo(5);
    assertThat(transformed.getMap(1).keyArray().getInt(0)).isEqualTo(6);
    assertThat(transformed.getMap(1).valueArray().getInt(0)).isEqualTo(7);
    assertThat(transformed.getStruct(2, 2).getInt(0)).isEqualTo(8);
    assertThat(transformed.getStruct(2, 2).getInt(1)).isEqualTo(9);
    assertThat(transformed.getInt(3)).isEqualTo(1);
    assertThat(transformed.getInt(4)).isEqualTo(-32768);
    assertThat(transformed.getInt(5)).isEqualTo(101);
  }

  @Test
  public void testNullValues() {
    InternalRow row =
        new GenericInternalRow(new Object[] {null, null, null, (byte) 0x01, (short) -32768, null});

    InternalRow transformed = TRANSFORMER.transform(row);
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
}
