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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collection;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestDefaultValuesValidation {

  private final Type type;
  private final JsonNode defaultValue;

  public TestDefaultValuesValidation(Type type, JsonNode defaultValue) {
    this.type = type;
    this.defaultValue = defaultValue;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {Types.BooleanType.get(), stringToJsonNode("true")},
        {Types.IntegerType.get(), stringToJsonNode("1")},
        {Types.LongType.get(), stringToJsonNode("9999999")},
        {Types.FloatType.get(), stringToJsonNode("1.23")},
        {Types.DoubleType.get(), stringToJsonNode("123.456")},
        {Types.DateType.get(), stringToJsonNode("\"2007-12-03\"")},
        {Types.TimeType.get(), stringToJsonNode("\"10:15:30\"")},
        {Types.TimestampType.withoutZone(), stringToJsonNode("\"2007-12-03T10:15:30\"")},
        {Types.TimestampType.withZone(), stringToJsonNode("\"2007-12-03T10:15:30+01:00\"")},
        {Types.StringType.get(), stringToJsonNode("\"foo\"")},
        {Types.UUIDType.get(), stringToJsonNode("\"eb26bdb1-a1d8-4aa6-990e-da940875492c\"")},
        {Types.FixedType.ofLength(2), stringToJsonNode("\"0x111f\"")},
        {Types.BinaryType.get(), stringToJsonNode("\"0x0000ff\"")},
        {Types.DecimalType.of(9, 2), stringToJsonNode("123.45")},
        {Types.ListType.ofOptional(1, Types.IntegerType.get()), stringToJsonNode("[1, 2, 3]")},
        {Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get()),
         stringToJsonNode("[[1,2], [\"foo\", \"bar\"]]")},
        {Types.StructType.of(
            required(1, "f1", Types.IntegerType.get(), "doc"),
            optional(2, "f2", Types.StringType.get(), "doc")),
         stringToJsonNode("{\"1\": 1, \"2\": \"bar\"}")}
    });
  }

  private static JsonNode stringToJsonNode(String json) {
    try {
      ObjectMapper mapper = new ObjectMapper();
      return mapper.readTree(json);
    } catch (JsonProcessingException e) {
      System.out.println("Failed to parse: " + json + "; reason: " + e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }

  // serialize to json and deserialize back should return the same result
  private static Schema jsonRoundTrip(Schema schema) {
    return SchemaParser.fromJson(SchemaParser.toJson(schema));
  }

  @Test
  public void testTypeWithDefaultValue() {
    Assert.assertTrue(DefaultValueParser.isValidDefault(type, defaultValue));
    Object javaDefaultValue = DefaultValueParser.parseDefaultFromJson(type, defaultValue);

    Schema schema = new Schema(Types.NestedField.required(999, "root", type, "doc", javaDefaultValue,
        javaDefaultValue));
    Assert.assertTrue(schema.sameSchema(jsonRoundTrip(schema)));
  }
}
