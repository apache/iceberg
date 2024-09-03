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

public class TestAccessors {

  private static Accessor<StructLike> direct(Type type) {
    Schema schema = new Schema(required(17, "field_" + type.typeId(), type));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested1(Type type) {
    Schema schema =
        new Schema(
            required(
                11,
                "struct1",
                Types.StructType.of(
                    Types.NestedField.required(17, "field_" + type.typeId(), type))));
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested2(Type type) {
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
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested3(Type type) {
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
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested3optional(Type type) {
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
    return schema.accessorForField(17);
  }

  private static Accessor<StructLike> nested4(Type type) {
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
    return schema.accessorForField(17);
  }

  private void assertAccessorReturns(Type type, Object value) {
    assertThat(direct(type).get(Row.of(value))).isEqualTo(value);

    assertThat(nested1(type).get(Row.of(Row.of(value)))).isEqualTo(value);
    assertThat(nested2(type).get(Row.of(Row.of(Row.of(value))))).isEqualTo(value);
    assertThat(nested3(type).get(Row.of(Row.of(Row.of(Row.of(value)))))).isEqualTo(value);
    assertThat(nested4(type).get(Row.of(Row.of(Row.of(Row.of(Row.of(value))))))).isEqualTo(value);

    assertThat(nested3optional(type).get(Row.of(Row.of(Row.of(Row.of(value)))))).isEqualTo(value);
  }

  @Test
  public void testBoolean() {
    assertAccessorReturns(Types.BooleanType.get(), true);
    assertAccessorReturns(Types.BooleanType.get(), false);
  }

  @Test
  public void testInt() {
    assertAccessorReturns(Types.IntegerType.get(), 123);
  }

  @Test
  public void testLong() {
    assertAccessorReturns(Types.LongType.get(), 123L);
  }

  @Test
  public void testFloat() {
    assertAccessorReturns(Types.FloatType.get(), 1.23f);
  }

  @Test
  public void testDouble() {
    assertAccessorReturns(Types.DoubleType.get(), 1.23d);
  }

  @Test
  public void testDate() {
    assertAccessorReturns(Types.DateType.get(), 123);
  }

  @Test
  public void testTime() {
    assertAccessorReturns(Types.TimeType.get(), 123L);
  }

  @Test
  public void testTimestamp() {
    assertAccessorReturns(Types.TimestampType.withoutZone(), 123L);
    assertAccessorReturns(Types.TimestampType.withZone(), 123L);
    assertAccessorReturns(Types.TimestampNanoType.withoutZone(), 123L);
    assertAccessorReturns(Types.TimestampNanoType.withZone(), 123L);
  }

  @Test
  public void testString() {
    assertAccessorReturns(Types.StringType.get(), "abc");
  }

  @Test
  public void testUuid() {
    assertAccessorReturns(Types.UUIDType.get(), UUID.randomUUID());
  }

  @Test
  public void testFixed() {
    assertAccessorReturns(Types.FixedType.ofLength(3), ByteBuffer.wrap(new byte[] {1, 2, 3}));
  }

  @Test
  public void testBinary() {
    assertAccessorReturns(Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {1, 2, 3}));
  }

  @Test
  public void testDecimal() {
    assertAccessorReturns(Types.DecimalType.of(5, 7), BigDecimal.valueOf(123.456));
  }

  @Test
  public void testList() {
    assertAccessorReturns(
        Types.ListType.ofRequired(18, Types.IntegerType.get()), ImmutableList.of(1, 2, 3));
    assertAccessorReturns(
        Types.ListType.ofRequired(18, Types.StringType.get()), ImmutableList.of("a", "b", "c"));
  }

  @Test
  public void testMap() {
    assertAccessorReturns(
        Types.MapType.ofRequired(18, 19, Types.StringType.get(), Types.IntegerType.get()),
        ImmutableMap.of("a", 1, "b", 2));
  }

  @Test
  public void testStructAsObject() {
    assertAccessorReturns(
        Types.StructType.of(
            Types.NestedField.optional(18, "str19", Types.StringType.get()),
            Types.NestedField.optional(19, "int19", Types.IntegerType.get())),
        Row.of("a", 1));
  }

  @Test
  public void testEmptyStructAsObject() {
    assertAccessorReturns(
        Types.StructType.of(Types.NestedField.optional(19, "int19", Types.IntegerType.get())),
        Row.of());

    assertAccessorReturns(Types.StructType.of(), Row.of());
  }

  @Test
  public void testEmptySchema() {
    Schema emptySchema = new Schema();
    assertThat(emptySchema.accessorForField(17)).isNull();
  }
}
