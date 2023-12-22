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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestAccessors {

  private static Accessor<StructLike> direct(Type type, boolean structAccessor) {
    Schema schema = new Schema(required(17, "field_" + type.typeId(), type));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private static Accessor<StructLike> nested1(Type type, boolean structAccessor) {
    Schema schema =
        new Schema(
            required(
                11,
                "struct1",
                Types.StructType.of(
                    Types.NestedField.required(17, "field_" + type.typeId(), type))));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private static Accessor<StructLike> nested2(Type type, boolean structAccessor) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(17, "field_" + type.typeId(), type))))));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private static Accessor<StructLike> nested3(Type type, boolean structAccessor) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.required(
                                        17, "field_" + type.typeId(), type))))))));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private static Accessor<StructLike> nested3optional(Type type, boolean structAccessor) {
    Schema schema =
        new Schema(
            optional(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.optional(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.optional(
                                        17, "field_" + type.typeId(), type))))))));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private static Accessor<StructLike> nested4(Type type, boolean structAccessor) {
    Schema schema =
        new Schema(
            required(
                11,
                "s",
                Types.StructType.of(
                    Types.NestedField.required(
                        22,
                        "s2",
                        Types.StructType.of(
                            Types.NestedField.required(
                                33,
                                "s3",
                                Types.StructType.of(
                                    Types.NestedField.required(
                                        44,
                                        "s4",
                                        Types.StructType.of(
                                            Types.NestedField.required(
                                                17, "field_" + type.typeId(), type))))))))));
    if (structAccessor) {
      return schema.asStruct().accessorForField(17);
    } else {
      return schema.accessorForField(17);
    }
  }

  private void assertAccessorReturns(Type type, Object value, boolean structAccessor) {
    assertThat(direct(type, structAccessor).get(Row.of(value))).isEqualTo(value);

    assertThat(nested1(type, structAccessor).get(Row.of(Row.of(value)))).isEqualTo(value);
    assertThat(nested2(type, structAccessor).get(Row.of(Row.of(Row.of(value))))).isEqualTo(value);
    assertThat(nested3(type, structAccessor).get(Row.of(Row.of(Row.of(Row.of(value))))))
        .isEqualTo(value);
    assertThat(nested4(type, structAccessor).get(Row.of(Row.of(Row.of(Row.of(Row.of(value)))))))
        .isEqualTo(value);

    assertThat(nested3optional(type, structAccessor).get(Row.of(Row.of(Row.of(Row.of(value))))))
        .isEqualTo(value);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testBoolean(boolean structAccessor) {
    assertAccessorReturns(Types.BooleanType.get(), true, structAccessor);
    assertAccessorReturns(Types.BooleanType.get(), false, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testInt(boolean structAccessor) {
    assertAccessorReturns(Types.IntegerType.get(), 123, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testLong(boolean structAccessor) {
    assertAccessorReturns(Types.LongType.get(), 123L, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testFloat(boolean structAccessor) {
    assertAccessorReturns(Types.FloatType.get(), 1.23f, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testDouble(boolean structAccessor) {
    assertAccessorReturns(Types.DoubleType.get(), 1.23d, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testDate(boolean structAccessor) {
    assertAccessorReturns(Types.DateType.get(), 123, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testTime(boolean structAccessor) {
    assertAccessorReturns(Types.TimeType.get(), 123L, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testTimestamp(boolean structAccessor) {
    assertAccessorReturns(Types.TimestampType.withoutZone(), 123L, structAccessor);
    assertAccessorReturns(Types.TimestampType.withZone(), 123L, structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testString(boolean structAccessor) {
    assertAccessorReturns(Types.StringType.get(), "abc", structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testUuid(boolean structAccessor) {
    assertAccessorReturns(Types.UUIDType.get(), UUID.randomUUID(), structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testFixed(boolean structAccessor) {
    assertAccessorReturns(
        Types.FixedType.ofLength(3), ByteBuffer.wrap(new byte[] {1, 2, 3}), structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testBinary(boolean structAccessor) {
    assertAccessorReturns(
        Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {1, 2, 3}), structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testDecimal(boolean structAccessor) {
    assertAccessorReturns(Types.DecimalType.of(5, 7), BigDecimal.valueOf(123.456), structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testList(boolean structAccessor) {
    assertAccessorReturns(
        Types.ListType.ofRequired(18, Types.IntegerType.get()),
        ImmutableList.of(1, 2, 3),
        structAccessor);
    assertAccessorReturns(
        Types.ListType.ofRequired(18, Types.StringType.get()),
        ImmutableList.of("a", "b", "c"),
        structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testMap(boolean structAccessor) {
    assertAccessorReturns(
        Types.MapType.ofRequired(18, 19, Types.StringType.get(), Types.IntegerType.get()),
        ImmutableMap.of("a", 1, "b", 2),
        structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testStructAsObject(boolean structAccessor) {
    assertAccessorReturns(
        Types.StructType.of(
            Types.NestedField.optional(18, "str19", Types.StringType.get()),
            Types.NestedField.optional(19, "int19", Types.IntegerType.get())),
        Row.of("a", 1),
        structAccessor);
  }

  @ParameterizedTest(name = "Accessor for struct {0}")
  @ValueSource(booleans = {true, false})
  public void testEmptyStructAsObject(boolean structAccessor) {
    assertAccessorReturns(
        Types.StructType.of(Types.NestedField.optional(19, "int19", Types.IntegerType.get())),
        Row.of(),
        structAccessor);

    assertAccessorReturns(Types.StructType.of(), Row.of(), structAccessor);
  }

  @Test
  public void testEmptySchema() {
    Schema emptySchema = new Schema();
    assertThat(emptySchema.accessorForField(17)).isNull();
  }

  @Test
  public void testEmptyStruct() {
    Types.StructType emptyStruct = Types.StructType.of();
    assertThat(emptyStruct.accessorForField(17)).isNull();
  }
}
