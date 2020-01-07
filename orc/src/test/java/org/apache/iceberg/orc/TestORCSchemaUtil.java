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

import static org.apache.iceberg.AssertHelpers.assertThrows;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

public class TestORCSchemaUtil {

  @Test
  public void testRoundtripConversionPrimitive() {
    Schema expectedSchema = new Schema(
        optional(1, "intCol", Types.IntegerType.get()),
        optional(3, "longCol", Types.LongType.get()),
        optional(6, "intCol2", Types.IntegerType.get()),
        optional(20, "intCol3", Types.IntegerType.get()),
        required(9, "doubleCol", Types.DoubleType.get()),
        required(10, "uuidCol", Types.UUIDType.get()),
        optional(2, "booleanCol", Types.BooleanType.get()),
        optional(21, "fixedCol", Types.FixedType.ofLength(4096)),
        required(22, "binaryCol", Types.BinaryType.get()),
        required(23, "stringCol", Types.StringType.get()),
        required(24, "decimalCol", Types.DecimalType.of(15, 3)),
        required(25, "floatCol", Types.FloatType.get()),
        optional(30, "dateCol", Types.DateType.get()),
        required(32, "timeCol", Types.TimeType.get()),
        required(34, "timestampCol", Types.TimestampType.withZone())
    );
    TypeDescription orcSchema = ORCSchemaUtil.convert(expectedSchema);
    assertEquals(expectedSchema.asStruct(), ORCSchemaUtil.convert(orcSchema).asStruct());
  }

  @Test
  public void testRoundtripConversionNested() {
    Types.StructType leafStructType = Types.StructType.of(
        optional(6, "leafLongCol", Types.LongType.get()),
        optional(7, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructType = Types.StructType.of(
        optional(4, "longCol", Types.LongType.get()),
        optional(5, "leafStructCol", leafStructType)
    );
    Types.StructType structPrimTypeForList = Types.StructType.of(
        optional(506, "leafLongCol", Types.LongType.get()),
        optional(507, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType leafStructTypeForList = Types.StructType.of(
        optional(516, "leafLongCol", Types.LongType.get()),
        optional(517, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructTypeForList = Types.StructType.of(
        optional(504, "longCol", Types.LongType.get()),
        optional(505, "leafStructCol", leafStructTypeForList)
    );
    Types.StructType structPrimTypeForMap = Types.StructType.of(
        optional(606, "leafLongCol", Types.LongType.get()),
        optional(607, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType leafStructTypeForMap = Types.StructType.of(
        optional(616, "leafLongCol", Types.LongType.get()),
        optional(617, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructTypeForMap = Types.StructType.of(
        optional(604, "longCol", Types.LongType.get()),
        optional(605, "leafStructCol", leafStructTypeForMap)
    );
    Types.StructType leafStructTypeForStruct = Types.StructType.of(
        optional(716, "leafLongCol", Types.LongType.get()),
        optional(717, "leafBinaryCol", Types.BinaryType.get())
    );
    Types.StructType nestedStructTypeForStruct = Types.StructType.of(
        optional(704, "longCol", Types.LongType.get()),
        optional(705, "leafStructCol", leafStructTypeForStruct)
    );
    // all fields in expected iceberg schema will be optional since we don't have a column mapping
    Schema expectedSchema = new Schema(
        optional(1, "intCol", Types.IntegerType.get()),
        optional(2, "longCol", Types.LongType.get()),
        optional(3, "nestedStructCol", nestedStructType),
        optional(8, "intCol3", Types.IntegerType.get()),
        optional(9, "doubleCol", Types.DoubleType.get()),
        required(10, "uuidCol", Types.UUIDType.get()),
        optional(20, "booleanCol", Types.BooleanType.get()),
        optional(21, "fixedCol", Types.FixedType.ofLength(4096)),
        required(22, "binaryCol", Types.BinaryType.get()),
        required(23, "stringCol", Types.StringType.get()),
        required(24, "decimalCol", Types.DecimalType.of(15, 3)),
        required(25, "floatCol", Types.FloatType.get()),
        optional(30, "dateCol", Types.DateType.get()),
        required(32, "timeCol", Types.TimeType.get()),
        required(34, "timestampCol", Types.TimestampType.withZone()),
        required(35, "listPrimCol",
            Types.ListType.ofRequired(135, Types.LongType.get())),
        required(36, "listPrimNestCol",
            Types.ListType.ofRequired(136, structPrimTypeForList)),
        required(37, "listNestedCol",
            Types.ListType.ofRequired(137, nestedStructTypeForList)),
        optional(38, "mapPrimCol",
            Types.MapType.ofRequired(138, 238, Types.StringType.get(), Types.FixedType.ofLength(4096))),
        required(39, "mapPrimNestCol",
            Types.MapType.ofRequired(139, 239, Types.StringType.get(), structPrimTypeForMap)),
        required(40, "mapNestedCol",
            Types.MapType.ofRequired(140, 240, Types.StringType.get(), nestedStructTypeForMap)),
        required(41, "structListNestCol",
            Types.ListType.ofRequired(241,
                Types.StructType.of(
                    optional(816, "leafLongCol", Types.LongType.get()),
                    optional(817, "leafBinaryCol", Types.BinaryType.get())
                ))
            ),
        required(42, "structMapNestCol",
            Types.MapType.ofRequired(242, 342, Types.StringType.get(),
                Types.StructType.of(
                    optional(916, "leafLongCol", Types.LongType.get()),
                    optional(917, "leafBinaryCol", Types.BinaryType.get())
                )
            )),
        required(43, "structStructNestCol",
            Types.StructType.of(required(243, "innerStructNest",
                  Types.StructType.of(
                      optional(1016, "leafLongCol", Types.LongType.get()),
                      optional(1017, "leafBinaryCol", Types.BinaryType.get())
                  ))
            )),
        required(44, "structStructComplexNestCol",
            Types.StructType.of(required(244, "innerStructNest",
                Types.StructType.of(
                    optional(1116, "leafLongCol", Types.LongType.get()),
                    optional(1117, "leftMapOfListStructCol",
                        Types.MapType.ofRequired(1150, 1151,
                            Types.StringType.get(),
                            Types.ListType.ofRequired(1250, nestedStructTypeForStruct))
                        )
                ))
            ))
    );
    TypeDescription orcSchema = ORCSchemaUtil.convert(expectedSchema);
    assertEquals(expectedSchema.asStruct(), ORCSchemaUtil.convert(orcSchema).asStruct());
  }

  @Test
  public void testTypePromotions() {
    Schema originalSchema = new Schema(
        optional(1, "a", Types.IntegerType.get()),
        optional(2, "b", Types.FloatType.get()),
        optional(3, "c", Types.DecimalType.of(10, 2))
    );

    // Original mapping (stored in ORC)
    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);

    // Evolve schema
    Schema evolveSchema = new Schema(
        optional(1, "a", Types.LongType.get()),
        optional(2, "b", Types.DoubleType.get()),
        optional(3, "c", Types.DecimalType.of(15, 2))
    );

    TypeDescription newOrcSchema = ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
    assertEquals(3, newOrcSchema.getChildren().size());
    assertEquals(1, newOrcSchema.findSubtype("a").getId());
    assertEquals(TypeDescription.Category.LONG, newOrcSchema.findSubtype("a").getCategory());
    assertEquals(2, newOrcSchema.findSubtype("b").getId());
    assertEquals(TypeDescription.Category.DOUBLE, newOrcSchema.findSubtype("b").getCategory());
    TypeDescription decimalC = newOrcSchema.findSubtype("c");
    assertEquals(3, decimalC.getId());
    assertEquals(TypeDescription.Category.DECIMAL, decimalC.getCategory());
    assertEquals(15, decimalC.getPrecision());
    assertEquals(2, decimalC.getScale());
  }

  @Test
  public void testInvalidTypePromotions() {
    Schema originalSchema = new Schema(
        optional(1, "a", Types.LongType.get())
    );

    TypeDescription orcSchema = ORCSchemaUtil.convert(originalSchema);
    Schema evolveSchema = new Schema(
        optional(1, "a", Types.IntegerType.get())
    );

    assertThrows("Should not allow invalid type promotion",
        IllegalArgumentException.class, "Can not promote", () -> {
          ORCSchemaUtil.buildOrcProjection(evolveSchema, orcSchema);
        });
  }
}
