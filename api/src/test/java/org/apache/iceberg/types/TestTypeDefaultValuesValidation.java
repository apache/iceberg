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


package org.apache.iceberg.types;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.*;

@RunWith(Parameterized.class)
public class TestTypeDefaultValuesValidation {
    private final Type type;
    private final JsonNode defaultValue;

    public TestTypeDefaultValuesValidation(Type type, JsonNode defaultValue) {
        this.type = type;
        this.defaultValue = defaultValue;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {Types.BooleanType.get(), parseJsonStringToJsonNode("true")},
                {Types.IntegerType.get(), parseJsonStringToJsonNode("1")},
                {Types.LongType.get(), parseJsonStringToJsonNode("9999999")},
                {Types.FloatType.get(), parseJsonStringToJsonNode("1.23")},
                {Types.DoubleType.get(), parseJsonStringToJsonNode("123.456")},
                {Types.DateType.get(), parseJsonStringToJsonNode("3650")},
                {Types.TimeType.get(), parseJsonStringToJsonNode("36000000000")},
                {Types.TimestampType.withoutZone(), parseJsonStringToJsonNode("1649374911000000")},
                {Types.TimestampType.withZone(), parseJsonStringToJsonNode("1649374911000000")},
                {Types.StringType.get(), parseJsonStringToJsonNode("\"foo\"")},
                {Types.UUIDType.get(), parseJsonStringToJsonNode("\"eb26bdb1-a1d8-4aa6-990e-da940875492c\"")},
                {Types.FixedType.ofLength(4), parseJsonStringToJsonNode("\"0x111f\"")},
                {Types.BinaryType.get(), parseJsonStringToJsonNode("\"0x0000ff\"")},
                {Types.DecimalType.of(9, 2), parseJsonStringToJsonNode("123.45")},
                {Types.ListType.ofOptional(1, Types.IntegerType.get()), parseJsonStringToJsonNode("[1, 2, 3]")},
                {Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get()),
                    parseJsonStringToJsonNode("[[1,2], [\"foo\", \"bar\"]]")},
                {Types.StructType.of(
                        required(1, "f1", Types.IntegerType.get(), "doc"),
                        optional(2, "f2", Types.StringType.get(), "doc")),
                    parseJsonStringToJsonNode("{\"1\": 1, \"2\": \"bar\"}")}
        });
    }

    public static JsonNode parseJsonStringToJsonNode(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readTree(json);
        } catch (JsonProcessingException e) {
            System.out.println("Failed to parse: " + json + "; reason: " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    @Test
    public void testTypeWithDefaultValue() {
        Types.NestedField nestedField1 = optional(1, "f1", type, "doc", defaultValue, defaultValue);
        Types.NestedField nestedField2 = required(1, "f1", type, "doc", defaultValue, defaultValue);
        Types.NestedField nestedField3 = of(1, true, "f1", type, "doc", defaultValue, defaultValue);
    }
}
