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

package org.apache.iceberg.orc;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class TestTypeConversion {

  @Test
  public void testRoundtripConversion() {
    ColumnMap mapping = new ColumnMap();
    Schema expectedSchema = new Schema(
        optional(1, "intCol", Types.IntegerType.get()),
        optional(3, "longCol", Types.LongType.get()),
        optional(6, "intCol2", Types.IntegerType.get()),
        optional(20, "intCol3", Types.IntegerType.get()),
        required(9, "doubleCol", Types.DoubleType.get())
    );
    TypeDescription orcSchema = TypeConversion.toOrc(expectedSchema, mapping);
    assertEquals(expectedSchema.asStruct(),
        TypeConversion.fromOrc(orcSchema, mapping).asStruct());
  }

  @Test
  public void testCreateSchemaWithoutMapping() {
    Types.StructType leafStructType = Types.StructType.of(
        optional(6, "leafLongCol", Types.LongType.get()),
        optional(7, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructType = Types.StructType.of(
        optional(4, "longCol", Types.LongType.get()),
        optional(5, "leafStructCol", leafStructType)
    );
    // all fields in expected iceberg schema will be optional since we don't have a column mapping
    Schema expectedSchema = new Schema(
        optional(1, "intCol", Types.IntegerType.get()),
        optional(2, "longCol", Types.LongType.get()),
        optional(3, "nestedStructCol", nestedStructType),
        optional(8, "intCol3", Types.IntegerType.get()),
        optional(9, "doubleCol", Types.DoubleType.get())
    );
    TypeDescription orcSchema = TypeDescription
        .fromString("struct<intCol:int,longCol:bigint," +
            "nestedStructCol:struct<longCol:bigint," +
            "leafStructCol:struct<leafLongCol:bigint,leafBinaryCol:binary>>," +
            "intCol3:int,doubleCol:double>");
    assertEquals(expectedSchema.asStruct(),
        TypeConversion.fromOrc(orcSchema, new ColumnMap()).asStruct());
  }
}
