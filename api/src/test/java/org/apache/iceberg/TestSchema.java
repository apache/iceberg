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

import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSchema {
  private static final Schema TS_NANO_CASES =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampNanoType.withZone()),
          Types.NestedField.optional(
              3, "arr", Types.ListType.ofRequired(4, Types.TimestampNanoType.withoutZone())),
          Types.NestedField.required(
              5,
              "struct",
              Types.StructType.of(
                  Types.NestedField.optional(6, "inner_ts", Types.TimestampNanoType.withZone()),
                  Types.NestedField.required(7, "data", Types.StringType.get()))),
          Types.NestedField.optional(
              8,
              "struct_arr",
              Types.StructType.of(
                  Types.NestedField.optional(9, "ts", Types.TimestampNanoType.withoutZone()))));

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

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testUnsupportedTimestampNano(int formatVersion) {
    Assertions.assertThatThrownBy(() -> Schema.checkCompatibility(TS_NANO_CASES, formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid type for ts: timestamptz_ns is not supported until v3\n"
                + "- Invalid type for arr.element: timestamp_ns is not supported until v3\n"
                + "- Invalid type for struct.inner_ts: timestamptz_ns is not supported until v3\n"
                + "- Invalid type for struct_arr.ts: timestamp_ns is not supported until v3",
            formatVersion);
  }

  @Test
  public void testSupportedTimestampNano() {
    Assertions.assertThatCode(() -> Schema.checkCompatibility(TS_NANO_CASES, 3))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2})
  public void testUnsupportedInitialDefault(int formatVersion) {
    Assertions.assertThatThrownBy(
            () -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Invalid schema for v%s:\n"
                + "- Invalid initial default for has_default: "
                + "non-null default (--) is not supported until v3",
            formatVersion);
  }

  @Test
  public void testSupportedInitialDefault() {
    Assertions.assertThatCode(() -> Schema.checkCompatibility(INITIAL_DEFAULT_SCHEMA, 3))
        .doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  public void testSupportedWriteDefault(int formatVersion) {
    // only the initial default is a forward-incompatible change
    Assertions.assertThatCode(() -> Schema.checkCompatibility(WRITE_DEFAULT_SCHEMA, formatVersion))
        .doesNotThrowAnyException();
  }
}
