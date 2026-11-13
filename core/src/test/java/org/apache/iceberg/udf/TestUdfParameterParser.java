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
import org.junit.jupiter.api.Test;

class TestUdfParameterParser {

  @Test
  void parsePrimitiveTypeParameter() {
    String json =
        """
        {"name":"x","type":"int"}""";
    UdfParameter expected =
        ImmutableUdfParameter.builder().name("x").type(UdfTypes.PrimitiveType.of("int")).build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void parseParameterWithDoc() {
    String json =
        """
        {"name":"x","type":"int","doc":"Input integer"}""";
    UdfParameter expected =
        ImmutableUdfParameter.builder()
            .name("x")
            .type(UdfTypes.PrimitiveType.of("int"))
            .doc("Input integer")
            .build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void parseListTypeParameter() {
    String json =
        """
        {
          "name": "items",
          "type": {
            "type": "list",
            "element": "string"
          }
        }""";
    UdfParameter expected =
        ImmutableUdfParameter.builder()
            .name("items")
            .type(UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string")))
            .build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void parseMapTypeParameter() {
    String json =
        """
        {
          "name": "lookup",
          "type": {
            "type": "map",
            "key": "string",
            "value": "int"
          }
        }""";
    UdfParameter expected =
        ImmutableUdfParameter.builder()
            .name("lookup")
            .type(
                UdfTypes.MapType.of(
                    UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int")))
            .build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void parseStructTypeParameter() {
    String json =
        """
        {
          "name": "row",
          "type": {
            "type": "struct",
            "fields": [
              {"name": "id", "type": "int"},
              {"name": "label", "type": "string"}
            ]
          }
        }""";
    UdfParameter expected =
        ImmutableUdfParameter.builder()
            .name("row")
            .type(
                UdfTypes.StructType.of(
                    UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")),
                    UdfTypes.NestedField.of("label", UdfTypes.PrimitiveType.of("string"))))
            .build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void parseNestedListOfStruct() {
    String json =
        """
        {
          "name": "records",
          "type": {
            "type": "list",
            "element": {
              "type": "struct",
              "fields": [
                {"name": "id", "type": "int"}
              ]
            }
          }
        }""";
    UdfParameter expected =
        ImmutableUdfParameter.builder()
            .name("records")
            .type(
                UdfTypes.ListType.of(
                    UdfTypes.StructType.of(
                        UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")))))
            .build();

    assertThat(UdfParameterParser.fromJson(json)).isEqualTo(expected);
  }

  @Test
  void roundTripPrimitiveType() {
    UdfParameter parameter =
        ImmutableUdfParameter.builder()
            .name("x")
            .type(UdfTypes.PrimitiveType.of("int"))
            .doc("Input integer")
            .build();

    String serialized = UdfParameterParser.toJson(parameter);
    assertThat(UdfParameterParser.fromJson(serialized)).isEqualTo(parameter);
  }

  @Test
  void roundTripListType() {
    UdfParameter parameter =
        ImmutableUdfParameter.builder()
            .name("items")
            .type(UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string")))
            .build();

    String serialized = UdfParameterParser.toJson(parameter);
    assertThat(UdfParameterParser.fromJson(serialized)).isEqualTo(parameter);
  }

  @Test
  void roundTripMapType() {
    UdfParameter parameter =
        ImmutableUdfParameter.builder()
            .name("lookup")
            .type(
                UdfTypes.MapType.of(
                    UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int")))
            .build();

    String serialized = UdfParameterParser.toJson(parameter);
    assertThat(UdfParameterParser.fromJson(serialized)).isEqualTo(parameter);
  }

  @Test
  void roundTripStructType() {
    UdfParameter parameter =
        ImmutableUdfParameter.builder()
            .name("row")
            .type(
                UdfTypes.StructType.of(
                    UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")),
                    UdfTypes.NestedField.of("label", UdfTypes.PrimitiveType.of("string"))))
            .build();

    String serialized = UdfParameterParser.toJson(parameter);
    assertThat(UdfParameterParser.fromJson(serialized)).isEqualTo(parameter);
  }

  @Test
  void nullParameter() {
    assertThatThrownBy(() -> UdfParameterParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UDF parameter: null");

    assertThatThrownBy(() -> UdfParameterParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse UDF parameter from null object");
  }

  @Test
  void missingRequiredFields() {
    String missingName =
        """
        {"type":"int"}""";
    assertThatThrownBy(() -> UdfParameterParser.fromJson(missingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String missingType =
        """
        {"name":"x"}""";
    assertThatThrownBy(() -> UdfParameterParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read type from null node");
  }
}
