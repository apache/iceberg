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
package org.apache.iceberg.types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.junit.jupiter.api.Test;

public class TestSerializableTypes {
  @Test
  public void testIdentityTypes() throws Exception {
    // these types make a strong guarantee than equality, instances are identical
    Type[] identityPrimitives =
        new Type[] {
          Types.BooleanType.get(),
          Types.IntegerType.get(),
          Types.LongType.get(),
          Types.FloatType.get(),
          Types.DoubleType.get(),
          Types.DateType.get(),
          Types.TimeType.get(),
          Types.TimestampType.microsWithoutZone(),
          Types.TimestampType.microsWithZone(),
          Types.TimestampType.nanosWithoutZone(),
          Types.TimestampType.nanosWithZone(),
          Types.StringType.get(),
          Types.UUIDType.get(),
          Types.BinaryType.get()
        };

    for (Type type : identityPrimitives) {
      assertThat(TestHelpers.roundTripSerialize(type))
          .as("Serialization result should be identical to starting type")
          .isSameAs(type);
    }
  }

  @Test
  public void testEqualTypes() throws Exception {
    Type[] equalityPrimitives =
        new Type[] {
          Types.DecimalType.of(9, 3),
          Types.DecimalType.of(11, 0),
          Types.FixedType.ofLength(4),
          Types.FixedType.ofLength(34)
        };

    for (Type type : equalityPrimitives) {
      assertThat(TestHelpers.roundTripSerialize(type))
          .as("Serialization result should be equal to starting type")
          .isEqualTo(type);
    }
  }

  @Test
  public void testStructs() throws Exception {
    Types.StructType struct =
        Types.StructType.of(
            Types.NestedField.required(34, "Name!", Types.StringType.get()),
            Types.NestedField.optional(35, "col", Types.DecimalType.of(38, 2)));

    Type copy = TestHelpers.roundTripSerialize(struct);
    assertThat(copy).as("Struct serialization should be equal to starting type").isEqualTo(struct);

    Type stringType = copy.asNestedType().asStructType().fieldType("Name!");
    assertThat(stringType)
        .as("Struct serialization should preserve identity type")
        .isSameAs(Types.StringType.get());

    Type decimalType = copy.asNestedType().asStructType().field(35).type();
    assertThat(decimalType)
        .as("Struct serialization should support id lookup")
        .isEqualTo(Types.DecimalType.of(38, 2));
  }

  @Test
  public void testMaps() throws Exception {
    Type[] maps =
        new Type[] {
          Types.MapType.ofOptional(1, 2, Types.StringType.get(), Types.LongType.get()),
          Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.LongType.get())
        };

    for (Type map : maps) {
      Type copy = TestHelpers.roundTripSerialize(map);
      assertThat(copy).as("Map serialization should be equal to starting type").isEqualTo(map);
      assertThat(map.asNestedType().asMapType().valueType())
          .as("Map serialization should preserve identity type")
          .isSameAs(Types.LongType.get());
    }
  }

  @Test
  public void testLists() throws Exception {
    Type[] maps =
        new Type[] {
          Types.ListType.ofOptional(2, Types.DoubleType.get()),
          Types.ListType.ofRequired(5, Types.DoubleType.get())
        };

    for (Type list : maps) {
      Type copy = TestHelpers.roundTripSerialize(list);
      assertThat(copy).as("List serialization should be equal to starting type").isEqualTo(list);
      assertThat(list.asNestedType().asListType().elementType())
          .as("List serialization should preserve identity type")
          .isSameAs(Types.DoubleType.get());
    }
  }

  @Test
  public void testSchema() throws Exception {
    Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "feature2", Types.BooleanType.get()))),
            required(
                4,
                "locations",
                Types.MapType.ofRequired(
                    10,
                    11,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(12, "lat", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get())))),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get())))),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get())),
            required(
                20,
                "complex_key_map",
                Types.MapType.ofOptional(
                    21,
                    22,
                    Types.StructType.of(
                        required(23, "x", Types.LongType.get()),
                        optional(24, "y", Types.LongType.get())),
                    Types.StringType.get())));

    assertThat(TestHelpers.roundTripSerialize(schema).asStruct())
        .as("Schema serialization should be equal to starting schema")
        .isEqualTo(schema.asStruct());
  }
}
