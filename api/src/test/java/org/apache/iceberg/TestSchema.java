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

import static org.apache.iceberg.Schema.DEFAULT_VALUES_MIN_FORMAT_VERSION;
import static org.apache.iceberg.Schema.MIN_FORMAT_VERSIONS;
import static org.apache.iceberg.TestHelpers.MAX_FORMAT_VERSION;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSchema {

  private static final List<Type> TEST_TYPES =
      ImmutableList.of(
          Types.TimestampNanoType.withoutZone(),
          Types.TimestampNanoType.withZone(),
          Types.VariantType.get());

  private static final Schema INITIAL_DEFAULT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required("has_default")
              .withId(2)
              .ofType(Types.StringType.get())
              .withInitialDefault(Literal.of("--"))
              .withWriteDefault(Literal.of("--"))
              .build());

  private static final Schema WRITE_DEFAULT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required("has_default")
              .withId(2)
              .ofType(Types.StringType.get())
              .withWriteDefault(Literal.of("--"))
              .build());

  private Schema generateTypeSchema(Type type) {
    return new Schema(
        Types.NestedField.required(1, "id", Types.LongType.get()),
        Types.NestedField.optional(2, "top", type),
        Types.NestedField.optional(3, "arr", Types.ListType.ofRequired(4, type)),
        Types.NestedField.required(
            5,
            "struct",
            Types.StructType.of(
                Types.NestedField.optional(6, "inner_op", type),
                Types.NestedField.required(7, "inner_req", type),
                Types.NestedField.optional(
                    8,
                    "struct_arr",
                    Types.StructType.of(Types.NestedField.optional(9, "deep", type))))));
  }

  private static Stream<Arguments> unsupportedTypes() {
    return TEST_TYPES.stream()
        .flatMap(
            type ->
                IntStream.range(1, MIN_FORMAT_VERSIONS.get(type.typeId()))
                    .mapToObj(unsupportedVersion -> Arguments.of(type, unsupportedVersion)));
  }

  @ParameterizedTest
  @MethodSource("unsupportedTypes")
  public void testUnsupportedTypes(Type type, int unsupportedVersion) {
    assertThatThrownBy(
            () -> Schema.checkCompatibility(generateTypeSchema(type), unsupportedVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid type for top: %s is not supported until v%s\n"
                + "- Invalid type for arr.element: %s is not supported until v%s\n"
                + "- Invalid type for struct.inner_op: %s is not supported until v%s\n"
                + "- Invalid type for struct.inner_req: %s is not supported until v%s\n"
                + "- Invalid type for struct.struct_arr.deep: %s is not supported until v%s",
            unsupportedVersion,
            type,
            MIN_FORMAT_VERSIONS.get(type.typeId()),
            type,
            MIN_FORMAT_VERSIONS.get(type.typeId()),
            type,
            MIN_FORMAT_VERSIONS.get(type.typeId()),
            type,
            MIN_FORMAT_VERSIONS.get(type.typeId()),
            type,
            MIN_FORMAT_VERSIONS.get(type.typeId()));
  }

  private static Stream<Arguments> supportedTypes() {
    return TEST_TYPES.stream()
        .flatMap(
            type ->
                IntStream.rangeClosed(MIN_FORMAT_VERSIONS.get(type.typeId()), MAX_FORMAT_VERSION)
                    .mapToObj(supportedVersion -> Arguments.of(type, supportedVersion)));
  }

  @Test
  public void testUnknownSupport() {
    // this needs a different schema because it cannot be used in required fields
    Schema schemaWithUnknown =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "top", Types.UnknownType.get()),
            Types.NestedField.optional(
                3, "arr", Types.ListType.ofOptional(4, Types.UnknownType.get())),
            Types.NestedField.required(
                5,
                "struct",
                Types.StructType.of(
                    Types.NestedField.optional(6, "inner_op", Types.UnknownType.get()),
                    Types.NestedField.optional(
                        7,
                        "inner_map",
                        Types.MapType.ofOptional(
                            8, 9, Types.StringType.get(), Types.UnknownType.get())),
                    Types.NestedField.optional(
                        10,
                        "struct_arr",
                        Types.StructType.of(
                            Types.NestedField.optional(11, "deep", Types.UnknownType.get()))))));

    assertThatThrownBy(() -> Schema.checkCompatibility(schemaWithUnknown, 2))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid type for top: %s is not supported until v%s\n"
                + "- Invalid type for arr.element: %s is not supported until v%s\n"
                + "- Invalid type for struct.inner_op: %s is not supported until v%s\n"
                + "- Invalid type for struct.inner_map.value: %s is not supported until v%s\n"
                + "- Invalid type for struct.struct_arr.deep: %s is not supported until v%s",
            2,
            Types.UnknownType.get(),
            MIN_FORMAT_VERSIONS.get(Type.TypeID.UNKNOWN),
            Types.UnknownType.get(),
            MIN_FORMAT_VERSIONS.get(Type.TypeID.UNKNOWN),
            Types.UnknownType.get(),
            MIN_FORMAT_VERSIONS.get(Type.TypeID.UNKNOWN),
            Types.UnknownType.get(),
            MIN_FORMAT_VERSIONS.get(Type.TypeID.UNKNOWN),
            Types.UnknownType.get(),
            MIN_FORMAT_VERSIONS.get(Type.TypeID.UNKNOWN));

    assertThatCode(() -> Schema.checkCompatibility(schemaWithUnknown, 3))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @MethodSource("supportedTypes")
  public void testTypeSupported(Type type, int supportedVersion) {
    assertThatCode(() -> Schema.checkCompatibility(generateTypeSchema(type), supportedVersion))
        .doesNotThrowAnyException();
  }

  private static int[] unsupportedInitialDefault =
      IntStream.range(1, DEFAULT_VALUES_MIN_FORMAT_VERSION).toArray();

  @ParameterizedTest
  @FieldSource("unsupportedInitialDefault")
  public void testUnsupportedInitialDefault(int formatVersion) {
    assertThatThrownBy(() -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid initial default for has_default: "
                + "non-null default (--) is not supported until v3",
            formatVersion);
  }

  private static int[] supportedInitialDefault =
      IntStream.rangeClosed(DEFAULT_VALUES_MIN_FORMAT_VERSION, MAX_FORMAT_VERSION).toArray();

  @ParameterizedTest
  @FieldSource("supportedInitialDefault")
  public void testSupportedInitialDefault(int formatVersion) {
    assertThatCode(() -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, formatVersion))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testSupportedWriteDefault(int formatVersion) {
    // only the initial default is a forward-incompatible change
    assertThatCode(() -> Schema.checkCompatibility(WRITE_DEFAULT_SCHEMA, formatVersion))
        .doesNotThrowAnyException();
  }
}
