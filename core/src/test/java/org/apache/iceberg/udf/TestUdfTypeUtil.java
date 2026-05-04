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
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

class TestUdfTypeUtil {

  @Test
  void readPrimitiveType() {
    JsonNode node = JsonUtil.mapper().valueToTree("int");
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfPrimitiveType.of("int"));
  }

  @Test
  void readDecimalType() {
    JsonNode node = JsonUtil.mapper().valueToTree("decimal(9,2)");
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfPrimitiveType.of("decimal(9,2)"));
  }

  @Test
  void readVariantType() {
    JsonNode node = JsonUtil.mapper().valueToTree("variant");
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfPrimitiveType.of("variant"));
  }

  @Test
  void readListType() {
    String json =
        """
        {"type":"list","element":"string"}""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfListType.of(UdfPrimitiveType.of("string")));
  }

  @Test
  void readMapType() {
    String json =
        """
        {"type":"map","key":"string","value":"int"}""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type)
        .isEqualTo(UdfMapType.of(UdfPrimitiveType.of("string"), UdfPrimitiveType.of("int")));
  }

  @Test
  void readStructType() {
    String structJson =
        """
        {
          "type": "struct",
          "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"}
          ]
        }""";
    JsonNode node = JsonUtil.parse(structJson, n -> n);
    UdfType expected =
        UdfStructType.of(
            UdfFieldType.of("id", UdfPrimitiveType.of("int")),
            UdfFieldType.of("name", UdfPrimitiveType.of("string")));

    assertThat(UdfTypeUtil.readType(node)).isEqualTo(expected);
  }

  @Test
  void readNestedListOfMap() {
    String json =
        """
        {
          "type": "list",
          "element": {
            "type": "map",
            "key": "string",
            "value": "int"
          }
        }""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType expected =
        UdfListType.of(UdfMapType.of(UdfPrimitiveType.of("string"), UdfPrimitiveType.of("int")));

    assertThat(UdfTypeUtil.readType(node)).isEqualTo(expected);
  }

  @Test
  void readNullNode() {
    assertThatThrownBy(() -> UdfTypeUtil.readType(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read type from null node");
  }

  @Test
  void readArrayNode() {
    JsonNode node = JsonUtil.mapper().valueToTree(new int[] {1, 2, 3});
    assertThatThrownBy(() -> UdfTypeUtil.readType(node))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse UDF type from node:");
  }

  @Test
  void readUnknownNestedType() {
    String json =
        """
        {"type":"set"}""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(node))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse UDF type from object with unknown type set: {\"type\":\"set\"}");
  }

  @Test
  void writePrimitiveType() {
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", UdfPrimitiveType.of("int"), gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json)
        .isEqualTo(
            """
            {"return-type":"int"}""");
  }

  @Test
  void writeListType() {
    UdfType listType = UdfListType.of(UdfPrimitiveType.of("string"));
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", listType, gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json)
        .isEqualTo(
            """
            {"return-type":{"type":"list","element":"string"}}""");
  }

  @Test
  void writeMapType() {
    UdfType mapType = UdfMapType.of(UdfPrimitiveType.of("string"), UdfPrimitiveType.of("int"));
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", mapType, gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json)
        .isEqualTo(
            """
            {"return-type":{"type":"map","key":"string","value":"int"}}""");
  }

  @Test
  void writeStructType() {
    UdfType structType =
        UdfStructType.of(
            UdfFieldType.of("id", UdfPrimitiveType.of("int")),
            UdfFieldType.of("name", UdfPrimitiveType.of("string")));
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", structType, gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json)
        .isEqualTo(
            """
            {"return-type":{"type":"struct","fields":[\
            {"name":"id","type":"int"},\
            {"name":"name","type":"string"}]}}""");
  }

  @Test
  void writeNullType() {
    assertThatThrownBy(
            () ->
                JsonUtil.generate(
                    gen -> {
                      gen.writeStartObject();
                      UdfTypeUtil.writeType("type", null, gen);
                      gen.writeEndObject();
                    },
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid type: null");
  }

  @Test
  void roundTripStructWithListAndMap() {
    UdfType structType =
        UdfStructType.of(
            UdfFieldType.of("id", UdfPrimitiveType.of("int")),
            UdfFieldType.of("tags", UdfListType.of(UdfPrimitiveType.of("string"))),
            UdfFieldType.of(
                "props", UdfMapType.of(UdfPrimitiveType.of("string"), UdfPrimitiveType.of("int"))));

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("type", structType, gen);
              gen.writeEndObject();
            },
            false);

    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType deserialized = UdfTypeUtil.readType(node.get("type"));
    assertThat(deserialized).isEqualTo(structType);
  }
}
