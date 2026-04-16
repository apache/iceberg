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
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestUdfTypeUtil {

  @Test
  public void testReadPrimitiveType() {
    JsonNode node = JsonUtil.mapper().valueToTree("int");
    Object type = UdfTypeUtil.readType(node);
    assertThat(type).isInstanceOf(String.class);
    assertThat(type).isEqualTo("int");
  }

  @Test
  public void testReadDecimalType() {
    JsonNode node = JsonUtil.mapper().valueToTree("decimal(9,2)");
    Object type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo("decimal(9,2)");
  }

  @Test
  public void testReadVariantType() {
    JsonNode node = JsonUtil.mapper().valueToTree("variant");
    Object type = UdfTypeUtil.readType(node);
    assertThat(type).isEqualTo("variant");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReadListType() {
    Map<String, Object> listType = ImmutableMap.of("type", "list", "element", "string");
    JsonNode node = JsonUtil.mapper().valueToTree(listType);
    Object type = UdfTypeUtil.readType(node);
    assertThat(type).isInstanceOf(Map.class);

    Map<String, Object> typeMap = (Map<String, Object>) type;
    assertThat(typeMap).containsEntry("type", "list");
    assertThat(typeMap).containsEntry("element", "string");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReadMapType() {
    Map<String, Object> mapType = ImmutableMap.of("type", "map", "key", "string", "value", "int");
    JsonNode node = JsonUtil.mapper().valueToTree(mapType);
    Object type = UdfTypeUtil.readType(node);

    Map<String, Object> typeMap = (Map<String, Object>) type;
    assertThat(typeMap).containsEntry("type", "map");
    assertThat(typeMap).containsEntry("key", "string");
    assertThat(typeMap).containsEntry("value", "int");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReadStructType() {
    String structJson =
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"}]}";
    JsonNode node = JsonUtil.parse(structJson, n -> n);
    Object type = UdfTypeUtil.readType(node);
    assertThat(type).isInstanceOf(Map.class);

    Map<String, Object> typeMap = (Map<String, Object>) type;
    assertThat(typeMap).containsEntry("type", "struct");
    assertThat(typeMap).containsKey("fields");
  }

  @Test
  public void testReadNullNode() {
    assertThatThrownBy(() -> UdfTypeUtil.readType(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot read type from null node");
  }

  @Test
  public void testReadArrayNode() {
    JsonNode node = JsonUtil.mapper().valueToTree(new int[] {1, 2, 3});
    assertThatThrownBy(() -> UdfTypeUtil.readType(node))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot parse UDF type from node:");
  }

  @Test
  public void testWritePrimitiveType() {
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", "int", gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json).isEqualTo("{\"return-type\":\"int\"}");
  }

  @Test
  public void testWriteNestedType() {
    Map<String, Object> listType = ImmutableMap.of("type", "list", "element", "string");
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              UdfTypeUtil.writeType("return-type", listType, gen);
              gen.writeEndObject();
            },
            false);

    assertThat(json).contains("\"return-type\"");
    assertThat(json).contains("\"type\":\"list\"");
    assertThat(json).contains("\"element\":\"string\"");
  }

  @Test
  public void testWriteNullType() {
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
  public void testWriteUnsupportedType() {
    assertThatThrownBy(
            () ->
                JsonUtil.generate(
                    gen -> {
                      gen.writeStartObject();
                      UdfTypeUtil.writeType("type", 42, gen);
                      gen.writeEndObject();
                    },
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot serialize UDF type:");
  }

  @Test
  public void testWriteTypeValue() {
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartArray();
              UdfTypeUtil.writeTypeValue("int", gen);
              gen.writeEndArray();
            },
            false);

    assertThat(json).isEqualTo("[\"int\"]");
  }

  @Test
  public void testWriteNestedTypeValue() {
    Map<String, Object> listType = ImmutableMap.of("type", "list", "element", "string");
    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartArray();
              UdfTypeUtil.writeTypeValue(listType, gen);
              gen.writeEndArray();
            },
            false);

    assertThat(json).contains("\"type\":\"list\"");
    assertThat(json).contains("\"element\":\"string\"");
  }
}
