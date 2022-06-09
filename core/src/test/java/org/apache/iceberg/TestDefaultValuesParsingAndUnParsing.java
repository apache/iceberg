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
import java.util.Arrays;
import java.util.Collection;
import java.util.Locale;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestDefaultValuesParsingAndUnParsing {

  private final Type type;
  private final String defaultValue;

  public TestDefaultValuesParsingAndUnParsing(Type type, String defaultValue) {
    this.type = type;
    this.defaultValue = defaultValue;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
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
    });
  }

  // serialize to json and deserialize back should return the same result
  private static String defaultValueParseAndUnParseRoundTrip(Type type, String defaultValue) throws IOException {
    Object javaDefaultValue = DefaultValueParser.fromJson(type, defaultValue);
    return DefaultValueParser.toJson(type, javaDefaultValue);
  }

  @Test
  public void testTypeWithDefaultValue() throws IOException {
    String roundTripDefaultValue = defaultValueParseAndUnParseRoundTrip(type, defaultValue);
    // Only if the type is a timestampWithZone type, the round-trip default value will always be standardized to the
    // UTC time zone, which might be different in the original value, but they should represent the same instant
    if (type.typeId() == Type.TypeID.TIMESTAMP && ((Types.TimestampType) type).shouldAdjustToUTC()) {
      Assert.assertTrue(OffsetDateTime.parse(JsonUtil.mapper().readTree(defaultValue).textValue())
              .isEqual(OffsetDateTime.parse(JsonUtil.mapper().readTree(roundTripDefaultValue).textValue())));
    } else {
      jsonStringEquals(defaultValue.toLowerCase(Locale.ROOT), roundTripDefaultValue.toLowerCase(Locale.ROOT));
      System.out.println(roundTripDefaultValue);
    }
  }

  private static void jsonStringEquals(String s1, String s2) throws IOException {
    Assert.assertEquals(JsonUtil.mapper().readTree(s1), JsonUtil.mapper().readTree(s2));
  }
}
