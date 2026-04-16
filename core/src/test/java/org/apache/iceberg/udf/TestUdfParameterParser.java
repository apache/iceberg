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
package org.apache.iceberg.udf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestUdfParameterParser {

  @Test
  public void testParsePrimitiveTypeParameter() {
    String json = "{\"name\":\"x\",\"type\":\"int\"}";
    UdfParameter parameter = ImmutableUdfParameter.builder().name("x").type("int").build();

    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("x");
    assertThat(parsed.type()).isEqualTo("int");
    assertThat(parsed.doc()).isNull();
    assertThat(parsed).isEqualTo(parameter);
  }

  @Test
  public void testParseParameterWithDoc() {
    String json = "{\"name\":\"x\",\"type\":\"int\",\"doc\":\"Input integer\"}";
    UdfParameter parameter =
        ImmutableUdfParameter.builder().name("x").type("int").doc("Input integer").build();

    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("x");
    assertThat(parsed.type()).isEqualTo("int");
    assertThat(parsed.doc()).isEqualTo("Input integer");
    assertThat(parsed).isEqualTo(parameter);
  }

  @Test
  public void testParseDecimalTypeParameter() {
    String json = "{\"name\":\"amount\",\"type\":\"decimal(9,2)\"}";
    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("amount");
    assertThat(parsed.type()).isEqualTo("decimal(9,2)");
  }

  @Test
  public void testParseVariantTypeParameter() {
    String json = "{\"name\":\"data\",\"type\":\"variant\"}";
    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("data");
    assertThat(parsed.type()).isEqualTo("variant");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testParseListTypeParameter() {
    String json = "{\"name\":\"items\",\"type\":{\"type\":\"list\",\"element\":\"string\"}}";
    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("items");
    assertThat(parsed.type()).isInstanceOf(Map.class);

    Map<String, Object> typeMap = (Map<String, Object>) parsed.type();
    assertThat(typeMap).containsEntry("type", "list");
    assertThat(typeMap).containsEntry("element", "string");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testParseMapTypeParameter() {
    String json =
        "{\"name\":\"lookup\",\"type\":{\"type\":\"map\",\"key\":\"string\",\"value\":\"int\"}}";
    UdfParameter parsed = UdfParameterParser.fromJson(json);
    assertThat(parsed.name()).isEqualTo("lookup");
    assertThat(parsed.type()).isInstanceOf(Map.class);

    Map<String, Object> typeMap = (Map<String, Object>) parsed.type();
    assertThat(typeMap).containsEntry("type", "map");
    assertThat(typeMap).containsEntry("key", "string");
    assertThat(typeMap).containsEntry("value", "int");
  }

  @Test
  public void testRoundTripPrimitiveType() {
    UdfParameter parameter =
        ImmutableUdfParameter.builder().name("x").type("int").doc("Input integer").build();

    String serialized = UdfParameterParser.toJson(parameter);
    UdfParameter deserialized = UdfParameterParser.fromJson(serialized);

    assertThat(deserialized).isEqualTo(parameter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRoundTripNestedType() {
    Map<String, Object> listType = ImmutableMap.of("type", "list", "element", "string");
    UdfParameter parameter = ImmutableUdfParameter.builder().name("items").type(listType).build();

    String serialized = UdfParameterParser.toJson(parameter);
    UdfParameter deserialized = UdfParameterParser.fromJson(serialized);

    assertThat(deserialized.name()).isEqualTo("items");
    Map<String, Object> roundTrippedType = (Map<String, Object>) deserialized.type();
    assertThat(roundTrippedType).containsEntry("type", "list");
    assertThat(roundTrippedType).containsEntry("element", "string");
  }

  @Test
  public void testNullParameter() {
    assertThatThrownBy(() -> UdfParameterParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UDF parameter: null");

    assertThatThrownBy(() -> UdfParameterParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse UDF parameter from null object");
  }

  @Test
  public void testMissingRequiredFields() {
    String missingName = "{\"type\":\"int\"}";
    assertThatThrownBy(() -> UdfParameterParser.fromJson(missingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String missingType = "{\"name\":\"x\"}";
    assertThatThrownBy(() -> UdfParameterParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read type from null node");
  }
}
