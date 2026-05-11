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
    assertThat(type).isEqualTo(UdfTypes.PrimitiveType.of("int"));
  }

  @Test
  void readDecimalType() {
    JsonNode node = JsonUtil.mapper().valueToTree("decimal(9,2)");
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfTypes.PrimitiveType.of("decimal(9,2)"));
  }

  @Test
  void readVariantType() {
    JsonNode node = JsonUtil.mapper().valueToTree("variant");
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfTypes.PrimitiveType.of("variant"));
  }

  @Test
  void readListType() {
    String json =
        """
        {"type":"list","element":"string"}""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo(UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string")));
  }

  @Test
  void readMapType() {
    String json =
        """
        {"type":"map","key":"string","value":"int"}""";
    JsonNode node = JsonUtil.parse(json, n -> n);
    UdfType type = UdfTypeUtil.readType(node);
    assertThat(type)
        .isEqualTo(
            UdfTypes.MapType.of(
                UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int")));
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
        UdfTypes.StructType.of(
            UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")),
            UdfTypes.NestedField.of("name", UdfTypes.PrimitiveType.of("string")));

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
        UdfTypes.ListType.of(
            UdfTypes.MapType.of(
                UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int")));

    assertThat(UdfTypeUtil.readType(node)).isEqualTo(expected);
  }

  @Test
  void primitiveTypeRejectsUnknownVocabulary() {
    assertThatThrownBy(() -> UdfTypes.PrimitiveType.of("foo"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot parse type string to primitive: foo");

    assertThatThrownBy(() -> UdfTypes.PrimitiveType.of("struct<x:int>"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void readPrimitiveTypeIsCaseInsensitive() {
    JsonNode upper = JsonUtil.mapper().valueToTree("INT");
    JsonNode mixed = JsonUtil.mapper().valueToTree("Decimal(9,2)");
    assertThat(UdfTypeUtil.readType(upper)).isEqualTo(UdfTypes.PrimitiveType.of("int"));
    assertThat(UdfTypeUtil.readType(mixed)).isEqualTo(UdfTypes.PrimitiveType.of("decimal(9,2)"));
  }

  @Test
  void readNestedTypeNameIsCaseInsensitive() {
    String listJson =
        """
        {"type":"LIST","element":"string"}""";
    assertThat(UdfTypeUtil.readType(JsonUtil.parse(listJson, n -> n)))
        .isEqualTo(UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string")));

    String mapJson =
        """
        {"type":"Map","key":"string","value":"int"}""";
    assertThat(UdfTypeUtil.readType(JsonUtil.parse(mapJson, n -> n)))
        .isEqualTo(
            UdfTypes.MapType.of(
                UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int")));

    String structJson =
        """
        {"type":"STRUCT","fields":[{"name":"id","type":"int"}]}""";
    assertThat(UdfTypeUtil.readType(JsonUtil.parse(structJson, n -> n)))
        .isEqualTo(
            UdfTypes.StructType.of(
                UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int"))));
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
  void readListMissingElement() {
    JsonNode node = JsonUtil.parse("{\"type\":\"list\"}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(node))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: element");
  }

  @Test
  void readMapMissingKeyOrValue() {
    JsonNode missingKey = JsonUtil.parse("{\"type\":\"map\",\"value\":\"int\"}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(missingKey))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: key");

    JsonNode missingValue = JsonUtil.parse("{\"type\":\"map\",\"key\":\"string\"}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(missingValue))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: value");
  }

  @Test
  void readStructWithInvalidField() {
    JsonNode missingName =
        JsonUtil.parse("{\"type\":\"struct\",\"fields\":[{\"type\":\"int\"}]}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(missingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    JsonNode missingType =
        JsonUtil.parse("{\"type\":\"struct\",\"fields\":[{\"name\":\"id\"}]}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: type");
  }

  @Test
  void readStructFieldNotObject() {
    JsonNode node = JsonUtil.parse("{\"type\":\"struct\",\"fields\":[\"oops\"]}", n -> n);
    assertThatThrownBy(() -> UdfTypeUtil.readType(node))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse struct field from non-object:");
  }

  @Test
  void writePrimitiveType() {
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", UdfTypes.PrimitiveType.of("int"), gen);
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
    UdfType listType = UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string"));
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
    UdfType mapType =
        UdfTypes.MapType.of(UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int"));
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
        UdfTypes.StructType.of(
            UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")),
            UdfTypes.NestedField.of("name", UdfTypes.PrimitiveType.of("string")));
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
        UdfTypes.StructType.of(
            UdfTypes.NestedField.of("id", UdfTypes.PrimitiveType.of("int")),
            UdfTypes.NestedField.of(
                "tags", UdfTypes.ListType.of(UdfTypes.PrimitiveType.of("string"))),
            UdfTypes.NestedField.of(
                "props",
                UdfTypes.MapType.of(
                    UdfTypes.PrimitiveType.of("string"), UdfTypes.PrimitiveType.of("int"))));

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
