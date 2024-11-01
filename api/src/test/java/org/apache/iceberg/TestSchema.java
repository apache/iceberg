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

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestSchema {

  private static final List<Type> TESTTYPES =
      ImmutableList.of(Types.TimestampNanoType.withoutZone(), Types.TimestampNanoType.withZone());

  private static final Map<Type.TypeID, Integer> MIN_FORMAT_VERSIONS =
      ImmutableMap.of(Type.TypeID.TIMESTAMP_NANO, 3);

  private static final Integer MIN_FORMAT_INITIAL_DEFAULT = 3;

  private static final Integer MAX_FORMAT_VERSION = 3;

  private static final Schema INITIAL_DEFAULT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required("has_default")
              .withId(2)
              .ofType(Types.StringType.get())
              .withInitialDefault("--")
              .withWriteDefault("--")
              .build());

  private static final Schema WRITE_DEFAULT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required("has_default")
              .withId(2)
              .ofType(Types.StringType.get())
              .withWriteDefault("--")
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

  private static Stream<Arguments> testTypeUnsupported() {
    return TESTTYPES.stream()
        .flatMap(
            type ->
                IntStream.range(1, MIN_FORMAT_VERSIONS.get(type.typeId()))
                    .mapToObj(unsupportedVersion -> Arguments.of(type, unsupportedVersion)));
  }

  @ParameterizedTest
  @MethodSource
  public void testTypeUnsupported(Type type, int unsupportedVersion) {
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

  private static Stream<Arguments> testTypeSupported() {
    return TESTTYPES.stream()
        .flatMap(
            type ->
                IntStream.range(MIN_FORMAT_VERSIONS.get(type.typeId()), MAX_FORMAT_VERSION + 1)
                    .mapToObj(unsupportedVersion -> Arguments.of(type, unsupportedVersion)));
  }

  @ParameterizedTest
  @MethodSource
  public void testTypeSupported(Type type, int supportedVersion) {
    assertThatCode(() -> Schema.checkCompatibility(generateTypeSchema(type), supportedVersion))
        .doesNotThrowAnyException();
  }

  private static int[] testUnsupportedInitialDefault =
      IntStream.range(1, MIN_FORMAT_INITIAL_DEFAULT).toArray();

  @ParameterizedTest
  @FieldSource
  public void testUnsupportedInitialDefault(int formatVersion) {
    assertThatThrownBy(() -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid initial default for has_default: "
                + "non-null default (--) is not supported until v3",
            formatVersion);
  }

  private static int[] testSupportedInitialDefault =
      IntStream.rangeClosed(MIN_FORMAT_INITIAL_DEFAULT, MAX_FORMAT_VERSION).toArray();

  @ParameterizedTest
  @FieldSource
  public void testSupportedInitialDefault() {
    assertThatCode(() -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, 3))
        .doesNotThrowAnyException();
  }

  private static int[] testSupportedWriteDefault =
      IntStream.rangeClosed(1, MAX_FORMAT_VERSION).toArray();

  @ParameterizedTest
  @FieldSource
  public void testSupportedWriteDefault(int formatVersion) {
    // only the initial default is a forward-incompatible change
    assertThatCode(() -> Schema.checkCompatibility(WRITE_DEFAULT_SCHEMA, formatVersion))
        .doesNotThrowAnyException();
  }
}
