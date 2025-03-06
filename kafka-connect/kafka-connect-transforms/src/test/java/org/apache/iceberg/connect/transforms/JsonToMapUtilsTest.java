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
package org.apache.iceberg.connect.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class JsonToMapUtilsTest extends FileLoads {
  private final ObjectMapper mapper = new ObjectMapper();

  private final ObjectNode objNode = loadJson(mapper);

  private ObjectNode loadJson(ObjectMapper objectMapper) {
    try {
      return (ObjectNode) objectMapper.readTree(getFile("jsonmap.json"));
    } catch (Exception e) {
      throw new RuntimeException("failed to load jsonmap.json in test", e);
    }
  }

  @Test
  @DisplayName("addField should add schemas to builder unless schema is null")
  public void addToSchema() {
    SchemaBuilder builder = SchemaBuilder.struct();
    Map.Entry<String, JsonNode> good =
        new AbstractMap.SimpleEntry<String, JsonNode>("good", TextNode.valueOf("text"));
    Map.Entry<String, JsonNode> missing =
        new AbstractMap.SimpleEntry<String, JsonNode>("missing", NullNode.getInstance());
    JsonToMapUtils.addField(good, builder);
    JsonToMapUtils.addField(missing, builder);
    Schema result = builder.build();
    assertThat(result.fields())
        .isEqualTo(Lists.newArrayList(new Field("good", 0, Schema.OPTIONAL_STRING_SCHEMA)));
  }

  @Test
  @DisplayName("extractSimpleValue extracts type from Node based on Schema")
  public void primitiveBasedOnSchemaHappyPath() {
    JsonNode stringNode = objNode.get("string");
    JsonNode booleanNode = objNode.get("boolean");
    JsonNode intNode = objNode.get("int");
    JsonNode longNode = objNode.get("long");
    JsonNode floatIsDoubleNode = objNode.get("float_is_double");
    JsonNode doubleNode = objNode.get("double");
    JsonNode bytesNode = objNode.get("bytes");

    assertThat(JsonToMapUtils.extractValue(stringNode, Schema.Type.STRING, "")).isEqualTo("string");
    assertThat(JsonToMapUtils.extractValue(booleanNode, Schema.Type.BOOLEAN, "")).isEqualTo(true);
    assertThat(JsonToMapUtils.extractValue(intNode, Schema.Type.INT32, "")).isEqualTo(42);
    assertThat(JsonToMapUtils.extractValue(longNode, Schema.Type.INT64, "")).isEqualTo(3147483647L);
    assertThat(JsonToMapUtils.extractValue(floatIsDoubleNode, Schema.Type.FLOAT64, ""))
        .isEqualTo(3.0);
    assertThat(JsonToMapUtils.extractValue(doubleNode, Schema.Type.FLOAT64, "")).isEqualTo(0.3);
    byte[] byteResult = (byte[]) JsonToMapUtils.extractValue(bytesNode, Schema.Type.BYTES, "");
    assertThat(byteResult).isEqualTo(Base64.getDecoder().decode("SGVsbG8="));
  }

  @Test
  @DisplayName("extractValue converts complex nodes to strings if schema is string")
  public void exactStringsFromComplexNodes() {
    JsonNode arrayObjects = objNode.get("array_objects");
    assertThat(arrayObjects).isInstanceOf(ArrayNode.class);

    JsonNode nestedObjNode = objNode.get("nested_obj");
    assertThat(nestedObjNode).isInstanceOf(ObjectNode.class);

    JsonNode arrayDifferentTypes = objNode.get("array_different_types");
    assertThat(arrayDifferentTypes).isInstanceOf(ArrayNode.class);

    JsonNode bigInt = objNode.get("bigInt");
    assertThat(bigInt).isInstanceOf(BigIntegerNode.class);

    assertThat(JsonToMapUtils.extractValue(arrayObjects, Schema.Type.STRING, ""))
        .isEqualTo("[{\"key\":1}]");
    assertThat(JsonToMapUtils.extractValue(arrayDifferentTypes, Schema.Type.STRING, ""))
        .isEqualTo("[\"one\",1]");
    assertThat(JsonToMapUtils.extractValue(bigInt, Schema.Type.STRING, ""))
        .isEqualTo("354736184430273859332531123456");
  }

  @Test
  @DisplayName("extractSimpleValue throws for non-primitive schema types")
  public void primitiveBasedOnSchemaThrows() {
    assertThatThrownBy(
            () -> JsonToMapUtils.extractValue(objNode.get("string"), Schema.Type.STRUCT, ""))
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  @DisplayName("arrayNodeType returns a type if all elements of the array are the same type")
  public void determineArrayNodeType() {
    ArrayNode arrayInt = (ArrayNode) objNode.get("array_int");
    ArrayNode arrayArrayInt = (ArrayNode) objNode.get("array_array_int");
    assertThat(IntNode.class).isEqualTo(JsonToMapUtils.arrayNodeType(arrayInt));
    assertThat(ArrayNode.class).isEqualTo(JsonToMapUtils.arrayNodeType(arrayArrayInt));
  }

  @Test
  @DisplayName("arrayNodeType returns null if elements of the array are different types")
  public void determineArrayNodeTypeNotSameTypes() {
    ArrayNode mixedArray = (ArrayNode) objNode.get("array_different_types");
    assertThat(JsonToMapUtils.arrayNodeType(mixedArray)).isNull();
  }

  @Test
  @DisplayName("schemaFromNode returns null for NullNode and MissingNode types")
  public void schemaFromNodeNullOnNullNodes() {
    JsonNode nullNode = NullNode.getInstance();
    JsonNode missingNode = MissingNode.getInstance();
    assertThat(JsonToMapUtils.schemaFromNode(nullNode)).isNull();
    assertThat(JsonToMapUtils.schemaFromNode(missingNode)).isNull();
  }

  @Test
  @DisplayName("schemaFromNode returns null for empty ObjectNodes")
  public void schemaFromNodeNullEmptyObjectNodes() {
    JsonNode node = objNode.get("empty_obj");
    assertThat(node).isInstanceOf(ObjectNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
  }

  @Test
  @DisplayName(
      "schemaFromNode returns bytes with logical name decimal with scale 0 for BigInteger nodes")
  public void schemaFromNodeStringForBigInteger() {
    JsonNode node = objNode.get("bigInt");
    assertThat(node).isInstanceOf(BigIntegerNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node)).isEqualTo(JsonToMapUtils.decimalSchema(0));
  }

  @Test
  @DisplayName("schemaFromNode returns primitive Schemas for primitive nodes")
  public void schemaFromNodePrimitiveSchemasFromPrimitiveNodes() {
    JsonNode intNode = objNode.get("int");
    JsonNode doubleNode = objNode.get("double");
    assertThat(intNode).isInstanceOf(IntNode.class);
    assertThat(doubleNode).isInstanceOf(DoubleNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(intNode)).isEqualTo(Schema.OPTIONAL_INT32_SCHEMA);
    assertThat(JsonToMapUtils.schemaFromNode(doubleNode)).isEqualTo(Schema.OPTIONAL_FLOAT64_SCHEMA);
  }

  @Test
  @DisplayName("schemaFromNode returns Map<String, String> for ObjectNodes")
  public void schmefromNodeObjectNodesAsMaps() {
    JsonNode node = objNode.get("nested_obj");
    assertThat(node).isInstanceOf(ObjectNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node))
        .isEqualTo(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build());
  }

  @Test
  @DisplayName("schemaFromNode returns Array String schema for ArrayNodes with ObjectNode elements")
  public void schemaFromNodeArrayStringFromArrayObjects() {
    JsonNode arrayObjects = objNode.get("array_objects");
    assertThat(arrayObjects).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(arrayObjects))
        .isEqualTo(JsonToMapUtils.ARRAY_MAP_OPTIONAL_STRING);
  }

  @Test
  @DisplayName("schemaFromNode returns Array String schema for ArrayNodes with inconsistent types")
  public void schemaFromNodeArrayStringFromInconsistentArrayNodes() {
    JsonNode inconsistent = objNode.get("array_different_types");
    assertThat(inconsistent).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(inconsistent))
        .isEqualTo(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build());
  }

  @Test
  @DisplayName(
      "schemaFromNode returns Array[Array[String]] for ArrayNodes of ArrayNodes with inconsistent types")
  public void schemaFromNodeArraysArrays() {
    JsonNode node = objNode.get("array_array_inconsistent");
    assertThat(node).isInstanceOf(ArrayNode.class);

    Schema expected =
        SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
            .optional()
            .build();
    Schema result = JsonToMapUtils.schemaFromNode(node);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  @DisplayName(
      "schemaFromNode returns Array[Array[Map<String, String>]] for ArrayNodes of ArrayNodes of objects")
  public void schemaFromNodeArrayArrayObjects() {
    JsonNode node = objNode.get("array_array_objects");
    assertThat(node).isInstanceOf(ArrayNode.class);
    Schema expected =
        SchemaBuilder.array(JsonToMapUtils.ARRAY_MAP_OPTIONAL_STRING).optional().build();
    Schema result = JsonToMapUtils.schemaFromNode(node);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  @DisplayName("schemaFromNode returns Array[Array[Int]] for ArrayNodes of ArrayNodes of IntNode")
  public void schemaFromNodeArrayArrayOfArrayArrayInt() {
    JsonNode node = objNode.get("array_array_int");
    assertThat(node).isInstanceOf(ArrayNode.class);
    Schema expected =
        SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_INT32_SCHEMA).optional().build())
            .optional()
            .build();
    Schema result = JsonToMapUtils.schemaFromNode(node);
    assertThat(result).isEqualTo(expected);
  }

  @Test
  @DisplayName("schemaFromNode returns null for empty ArrayNodes")
  public void schemaFromNodeNullFromEmptyArray() {
    JsonNode node = objNode.get("empty_arr");
    assertThat(node).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
  }

  @Test
  @DisplayName("schemaFromNode returns null for empty Array of Array nodes")
  public void schemaFromNodeEmptyArrayOfEmptyArrays() {
    JsonNode node = objNode.get("empty_arr_arr");
    assertThat(node).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node)).isNull();
  }

  @Test
  @DisplayName(
      "schemaFromNode returns optional map of optional <string, string> for array of empty object")
  public void schemaFromNodeNullArrayEmptyObject() {
    JsonNode node = objNode.get("array_empty_object");
    assertThat(node).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node))
        .isEqualTo(JsonToMapUtils.ARRAY_MAP_OPTIONAL_STRING);
  }

  @Test
  @DisplayName(
      "schemaFromNode returns Array[Map<String, String>] for array of objects if one object is empty")
  public void schemaFromNodeMixedObjectsOneEmpty() {
    JsonNode node = objNode.get("nested_object_contains_empty");
    assertThat(node).isInstanceOf(ArrayNode.class);
    assertThat(JsonToMapUtils.schemaFromNode(node))
        .isEqualTo(JsonToMapUtils.ARRAY_MAP_OPTIONAL_STRING);
  }

  @Test
  public void addToStruct() {
    SchemaBuilder builder = SchemaBuilder.struct();
    objNode.fields().forEachRemaining(entry -> JsonToMapUtils.addField(entry, builder));
    Schema schema = builder.build();

    Struct result = new Struct(schema);
    JsonToMapUtils.addToStruct(objNode, schema, result);

    assertThat(result.get("string")).isEqualTo("string");
    assertThat(result.get("int")).isEqualTo(42);
    assertThat(result.get("long")).isEqualTo(3147483647L);
    assertThat(result.get("boolean")).isEqualTo(true);
    assertThat(result.get("float_is_double")).isEqualTo(3.0);
    assertThat(result.get("double")).isEqualTo(0.3);
    // we don't actually convert to bytes when parsing the json
    // so just check it is a string
    assertThat(result.get("bytes")).isEqualTo("SGVsbG8=");
    BigDecimal bigIntExpected = new BigDecimal(new BigInteger("354736184430273859332531123456"));
    assertThat(result.get("bigInt")).isEqualTo(bigIntExpected);
    assertThat(result.get("nested_object_contains_empty"))
        .isEqualTo(Lists.newArrayList(Maps.newHashMap(), ImmutableMap.of("one", "1")));
    assertThat(result.get("array_int")).isEqualTo(Lists.newArrayList(1, 1));
    assertThat(result.get("array_array_int"))
        .isEqualTo(Lists.newArrayList(Lists.newArrayList(1, 1), Lists.newArrayList(2, 2)));
    assertThat(result.get("array_objects"))
        .isEqualTo(Lists.newArrayList(ImmutableMap.of("key", "1")));
    assertThat(result.get("array_array_objects"))
        .isEqualTo(
            Lists.newArrayList(
                Lists.newArrayList(ImmutableMap.of("key", "1")),
                Lists.newArrayList(ImmutableMap.of("key", "2", "other", "{\"ugly\":[1,2]}"))));
    assertThat(result.get("array_array_inconsistent"))
        .isEqualTo(Lists.newArrayList(Lists.newArrayList("1"), Lists.newArrayList("2.0")));
    assertThat(result.get("array_different_types")).isEqualTo(Lists.newArrayList("one", "1"));

    Map<String, String> expectedNestedObject = Maps.newHashMap();
    expectedNestedObject.put("key", "{\"nested_key\":1}");
    assertThat(result.get("nested_obj")).isEqualTo(expectedNestedObject);

    assertThat(result.get("empty_string")).isEqualTo("");

    // assert empty fields don't show up on the struct
    assertThatThrownBy(() -> result.get("null")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> result.get("empty_obj")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> result.get("empty_arr")).isInstanceOf(RuntimeException.class);
    assertThatThrownBy(() -> result.get("empty_arr_arr")).isInstanceOf(RuntimeException.class);
  }
}
