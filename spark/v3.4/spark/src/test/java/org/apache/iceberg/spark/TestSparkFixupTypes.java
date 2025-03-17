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

import java.util.stream.Stream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSparkFixupTypes {

  private static Stream<Arguments> primitiveTypes() {
    return Stream.of(
        Arguments.of(Types.UnknownType.get()),
        Arguments.of(Types.BooleanType.get()),
        Arguments.of(Types.IntegerType.get()),
        Arguments.of(Types.LongType.get()),
        Arguments.of(Types.FloatType.get()),
        Arguments.of(Types.DoubleType.get()),
        Arguments.of(Types.DateType.get()),
        Arguments.of(Types.TimeType.get()),
        Arguments.of(Types.TimestampType.withoutZone()),
        Arguments.of(Types.TimestampType.withZone()),
        Arguments.of(Types.TimestampNanoType.withoutZone()),
        Arguments.of(Types.TimestampNanoType.withZone()),
        Arguments.of(Types.StringType.get()),
        Arguments.of(Types.UUIDType.get()),
        Arguments.of(Types.FixedType.ofLength(3)),
        Arguments.of(Types.FixedType.ofLength(4)),
        Arguments.of(Types.BinaryType.get()),
        Arguments.of(Types.DecimalType.of(9, 2)),
        Arguments.of(Types.DecimalType.of(11, 2)),
        Arguments.of(Types.DecimalType.of(9, 3)),
        Arguments.of(Types.VariantType.get()));
  }

  @Test
  void fixupShouldCorrectlyFixUUID() {
    Schema schema = new Schema(Types.NestedField.required(1, "field", Types.StringType.get()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.UUIDType.get()));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isSameAs(Types.UUIDType.get());
  }

  @Test
  void fixupShouldCorrectlyFixBinary() {
    Schema schema = new Schema(Types.NestedField.required(1, "field", Types.BinaryType.get()));
    Schema referenceSchema =
        new Schema(Types.NestedField.required(1, "field", Types.FixedType.ofLength(16)));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isEqualTo(Types.FixedType.ofLength(16));
  }

  @ParameterizedTest
  @MethodSource("primitiveTypes")
  void fixupShouldNotChangeNonMatchingPrimitiveTypes(Type type) {
    Schema schema = new Schema(Types.NestedField.optional(1, "field", type));
    Schema referenceSchema =
        new Schema(Types.NestedField.optional(1, "field", Types.IntegerType.get()));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("field")).isSameAs(type);
  }

  @Test
  void fixupShouldHandleNestedStructs() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "struct",
                Types.StructType.of(
                    Types.NestedField.required(2, "field", Types.StringType.get()))));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "struct",
                Types.StructType.of(Types.NestedField.required(2, "field", Types.UUIDType.get()))));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("struct"))
        .isEqualTo(
            Types.StructType.of(Types.NestedField.required(2, "field", Types.UUIDType.get())));
  }

  @Test
  void fixupShouldHandleLists() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1, "list", Types.ListType.ofRequired(2, Types.StringType.get())));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1, "list", Types.ListType.ofRequired(2, Types.UUIDType.get())));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("list"))
        .isEqualTo(Types.ListType.ofRequired(2, Types.UUIDType.get()));
  }

  @Test
  void fixupShouldHandleMaps() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.StringType.get())));
    Schema referenceSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "map",
                Types.MapType.ofRequired(2, 3, Types.UUIDType.get(), Types.UUIDType.get())));

    Schema fixedSchema = SparkFixupTypes.fixup(schema, referenceSchema);

    assertThat(fixedSchema.findType("map"))
        .isEqualTo(Types.MapType.ofRequired(2, 3, Types.UUIDType.get(), Types.UUIDType.get()));
  }
}
