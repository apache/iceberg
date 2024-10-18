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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestSingleValueParser {

  @Test
  public void testValidDefaults() throws IOException {
    Object[][] typesWithDefaults =
        new Object[][] {
          {Types.BooleanType.get(), "null"},
          {Types.BooleanType.get(), "true"},
          {Types.IntegerType.get(), "1"},
          {Types.LongType.get(), "9999999"},
          {Types.FloatType.get(), "1.23"},
          {Types.DoubleType.get(), "123.456"},
          {Types.DateType.get(), "\"2007-12-03\""},
          {Types.TimeType.get(), "\"10:15:30\""},
          {Types.TimestampType.withoutZone(), "\"2007-12-03T10:15:30\""},
          {Types.TimestampType.withZone(), "\"2007-12-03T10:15:30+00:00\""},
          {Types.StringType.get(), "\"foo\""},
          {Types.UUIDType.get(), "\"eb26bdb1-a1d8-4aa6-990e-da940875492c\""},
          {Types.FixedType.ofLength(2), "\"111f\""},
          {Types.BinaryType.get(), "\"0000ff\""},
          {Types.DecimalType.of(9, 4), "\"123.4500\""},
          {Types.DecimalType.of(9, 0), "\"2\""},
          {Types.DecimalType.of(9, -20), "\"2E+20\""},
          {Types.GeometryType.get(), "\"POINT (1 2)\""},
          {Types.GeometryType.get(), "\"POINT Z(1 2 3)\""},
          {Types.GeometryType.get(), "\"POINT M(1 2 3)\""},
          {Types.GeometryType.get(), "\"POINT ZM(1 2 3 4)\""},
          {
            Types.GeometryType.of("test_crs", Types.GeometryType.Edges.SPHERICAL), "\"POINT (1 2)\""
          },
          {
            Types.GeometryType.of("test_crs", Types.GeometryType.Edges.SPHERICAL),
            "\"POINT ZM(1 2 3 4)\""
          },
          {Types.ListType.ofOptional(1, Types.IntegerType.get()), "[1, 2, 3]"},
          {
            Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), Types.StringType.get()),
            "{\"keys\": [1, 2], \"values\": [\"foo\", \"bar\"]}"
          },
          {
            Types.StructType.of(
                required(4, "f1", Types.IntegerType.get()),
                optional(5, "f2", Types.StringType.get())),
            "{\"4\": 1, \"5\": \"bar\"}"
          },
          // deeply nested complex types
          {
            Types.ListType.ofOptional(
                6,
                Types.StructType.of(
                    required(7, "f1", Types.IntegerType.get()),
                    optional(8, "f2", Types.StringType.get()))),
            "[{\"7\": 1, \"8\": \"bar\"}, {\"7\": 2, \"8\": " + "\"foo\"}]"
          },
          {
            Types.MapType.ofOptional(
                9,
                10,
                Types.IntegerType.get(),
                Types.StructType.of(
                    required(11, "f1", Types.IntegerType.get()),
                    optional(12, "f2", Types.StringType.get()))),
            "{\"keys\": [1, 2], \"values\": [{\"11\": 1, \"12\": \"bar\"}, {\"11\": 2, \"12\": \"foo\"}]}"
          },
          {
            Types.StructType.of(
                required(
                    13,
                    "f1",
                    Types.StructType.of(
                        optional(14, "ff1", Types.IntegerType.get()),
                        optional(15, "ff2", Types.StringType.get()))),
                optional(
                    16,
                    "f2",
                    Types.StructType.of(
                        optional(17, "ff1", Types.StringType.get()),
                        optional(18, "ff2", Types.IntegerType.get())))),
            "{\"13\": {\"14\": 1, \"15\": \"bar\"}, \"16\": {\"17\": \"bar\", \"18\": 1}}"
          },
        };

    for (Object[] typeWithDefault : typesWithDefaults) {
      Type type = (Type) typeWithDefault[0];
      String defaultValue = (String) typeWithDefault[1];

      String roundTripDefaultValue = defaultValueParseAndUnParseRoundTrip(type, defaultValue);
      jsonStringEquals(
          defaultValue.toLowerCase(Locale.ROOT), roundTripDefaultValue.toLowerCase(Locale.ROOT));
    }
  }

  @Test
  public void testInvalidFixed() {
    Type expectedType = Types.FixedType.ofLength(2);
    String defaultJson = "\"111ff\"";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse default fixed[2] value");
  }

  @Test
  public void testInvalidUUID() {
    Type expectedType = Types.UUIDType.get();
    String defaultJson = "\"eb26bdb1-a1d8-4aa6-990e-da940875492c-abcde\"";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse default as a uuid value");
  }

  @Test
  public void testInvalidMap() {
    Type expectedType =
        Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get());
    String defaultJson = "{\"keys\": [1, 2, 3], \"values\": [\"foo\", \"bar\"]}";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse default as a map<int, string> value");
  }

  @Test
  public void testInvalidDecimal() {
    Type expectedType = Types.DecimalType.of(5, 2);
    String defaultJson = "123.456";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse default as a decimal(5, 2) value");
  }

  @Test
  public void testInvalidTimestamptz() {
    Type expectedType = Types.TimestampType.withZone();
    String defaultJson = "\"2007-12-03T10:15:30+01:00\"";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse default as a timestamptz value");
  }

  @Test
  public void testInvalidGeometry() {
    Type expectedType = Types.GeometryType.get();
    String defaultJson = "\"POINT (1 2 3 4 5 6)\"";
    assertThatThrownBy(() -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("Cannot parse default as a geometry.* value.*");
  }

  // serialize to json and deserialize back should return the same result
  private static String defaultValueParseAndUnParseRoundTrip(Type type, String defaultValue) {
    Object javaDefaultValue = SingleValueParser.fromJson(type, defaultValue);
    return SingleValueParser.toJson(type, javaDefaultValue);
  }

  private static void jsonStringEquals(String s1, String s2) throws IOException {
    assertThat(JsonUtil.mapper().readTree(s2)).isEqualTo(JsonUtil.mapper().readTree(s1));
  }
}
