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

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.Locale;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestDefaultValueParser {

  @Test
  public void testValidDefaults() throws IOException {
    Object[][] typesWithDefaults = new Object[][] {
        {Types.BooleanType.get(), "null"},
        {Types.BooleanType.get(), "true"},
        {Types.IntegerType.get(), "1"},
        {Types.LongType.get(), "9999999"},
        {Types.FloatType.get(), "1.23"},
        {Types.DoubleType.get(), "123.456"},
        {Types.DateType.get(), "\"2007-12-03\""},
        {Types.TimeType.get(), "\"10:15:30\""},
        {Types.TimestampType.withoutZone(), "\"2007-12-03T10:15:30\""},
        {Types.TimestampType.withZone(), "\"2007-12-03T10:15:30+01:00\""},
        {Types.StringType.get(), "\"foo\""},
        {Types.UUIDType.get(), "\"eb26bdb1-a1d8-4aa6-990e-da940875492c\""},
        {Types.FixedType.ofLength(2), "\"111f\""},
        {Types.BinaryType.get(), "\"0000ff\""},
        {Types.DecimalType.of(9, 2), "123.45"},
        {Types.ListType.ofOptional(1, Types.IntegerType.get()), "[1, 2, 3]"},
        {Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get()),
         "{\"keys\": [1, 2], \"values\": [\"foo\", \"bar\"]}"},
        {Types.StructType.of(
            required(1, "f1", Types.IntegerType.get(), "doc"),
            optional(2, "f2", Types.StringType.get(), "doc")),
         "{\"1\": 1, \"2\": \"bar\"}"},
        // deeply nested complex types
        {Types.ListType.ofOptional(1, Types.StructType.of(
            required(1, "f1", Types.IntegerType.get(), "doc"),
            optional(2, "f2", Types.StringType.get(), "doc"))), "[{\"1\": 1, \"2\": \"bar\"}, {\"1\": 2, \"2\": " +
             "\"foo\"}]"},
        {Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StructType.of(
            required(1, "f1", Types.IntegerType.get(), "doc"),
            optional(2, "f2", Types.StringType.get(), "doc"))),
         "{\"keys\": [1, 2], \"values\": [{\"1\": 1, \"2\": \"bar\"}, {\"1\": 2, \"2\": \"foo\"}]}"},
        {Types.StructType.of(
            required(1, "f1", Types.StructType.of(
                optional(2, "ff1", Types.IntegerType.get(), "doc"),
                optional(3, "ff2", Types.StringType.get(), "doc")), "doc"),
            optional(4, "f2", Types.StructType.of(
                optional(5, "ff1", Types.StringType.get(), "doc"),
                optional(6, "ff2", Types.IntegerType.get(), "doc")), "doc")),
         "{\"1\": {\"2\": 1, \"3\": \"bar\"}, \"4\": {\"5\": \"bar\", \"6\": 1}}"},
        };

    for (Object[] typeWithDefault : typesWithDefaults) {
      Type type = (Type) typeWithDefault[0];
      String defaultValue = (String) typeWithDefault[1];

      String roundTripDefaultValue = defaultValueParseAndUnParseRoundTrip(type, defaultValue);
      // Only if the type is a timestampWithZone type, the round-trip default value will always be standardized to the
      // UTC time zone, which might be different in the original value, but they should represent the same instant
      if (type.typeId() == Type.TypeID.TIMESTAMP && ((Types.TimestampType) type).shouldAdjustToUTC()) {
        Assert.assertTrue(OffsetDateTime.parse(JsonUtil.mapper().readTree(defaultValue).textValue())
            .isEqual(OffsetDateTime.parse(JsonUtil.mapper().readTree(roundTripDefaultValue).textValue())));
      } else {
        jsonStringEquals(defaultValue.toLowerCase(Locale.ROOT), roundTripDefaultValue.toLowerCase(Locale.ROOT));
      }
    }
  }

  @Test
  public void testInvalidFixed() {
    Type expectedType = Types.FixedType.ofLength(2);
    String defaultJson = "\"111ff\"";
    Exception exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson));
    Assert.assertTrue(exception.getMessage().startsWith("Cannot parse default fixed[2] value"));
  }

  @Test
  public void testInvalidUUID() {
    Type expectedType = Types.UUIDType.get();
    String defaultJson = "\"eb26bdb1-a1d8-4aa6-990e-da940875492c-abcde\"";
    Exception exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson));
    Assert.assertTrue(exception.getMessage().startsWith("Cannot parse default as a uuid value"));
  }

  @Test
  public void testInvalidMap() {
    Type expectedType = Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get());
    String defaultJson = "{\"keys\": [1, 2, 3], \"values\": [\"foo\", \"bar\"]}";
    Exception exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson));
    Assert.assertTrue(exception.getMessage().startsWith("Cannot parse default as a map<int, string> value"));
  }

  @Test
  public void testInvalidDecimal() {
    Type expectedType = Types.DecimalType.of(5, 2);
    String defaultJson = "123.456";
    Exception exception = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> defaultValueParseAndUnParseRoundTrip(expectedType, defaultJson));
    Assert.assertTrue(exception.getMessage().startsWith("Cannot parse default as a decimal(5, 2) value"));
  }

  // serialize to json and deserialize back should return the same result
  private static String defaultValueParseAndUnParseRoundTrip(Type type, String defaultValue) throws IOException {
    Object javaDefaultValue = DefaultValueParser.fromJson(type, defaultValue);
    return DefaultValueParser.toJson(type, javaDefaultValue);
  }

  private static void jsonStringEquals(String s1, String s2) throws IOException {
    Assert.assertEquals(JsonUtil.mapper().readTree(s1), JsonUtil.mapper().readTree(s2));
  }
}
