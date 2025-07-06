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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BigIntegerNode;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.MissingNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

class JsonToMapUtils {

  private static final String DECIMAL_LOGICAL_NAME = "org.apache.kafka.connect.data.Decimal";
  private static final String SCALE_FIELD = "scale";

  public static final Schema ARRAY_OPTIONAL_STRING =
      SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build();
  public static final Schema ARRAY_MAP_OPTIONAL_STRING =
      SchemaBuilder.array(
              SchemaBuilder.map(Schema.OPTIONAL_STRING_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA)
                  .optional()
                  .build())
          .optional()
          .build();

  public static final Schema ARRAY_ARRAY_OPTIONAL_STRING =
      SchemaBuilder.array(SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
          .optional()
          .build();

  private static SchemaBuilder decimalBuilder(int scale) {
    return SchemaBuilder.bytes()
        .name(DECIMAL_LOGICAL_NAME)
        .parameter(SCALE_FIELD, Integer.toString(scale))
        .version(1);
  }

  public static Schema decimalSchema(int scale) {
    return decimalBuilder(scale).optional().build();
  }

  private JsonToMapUtils() {
    throw new UnsupportedOperationException("This is a utility class and cannot be instantiated");
  }

  private static final Map<Class<? extends JsonNode>, Schema> JSON_NODE_TO_SCHEMA =
      getJsonNodeToSchema();

  private static Map<Class<? extends JsonNode>, Schema> getJsonNodeToSchema() {
    final Map<Class<? extends JsonNode>, Schema> map = Maps.newHashMap();
    map.put(BinaryNode.class, Schema.OPTIONAL_BYTES_SCHEMA);
    map.put(BooleanNode.class, Schema.OPTIONAL_BOOLEAN_SCHEMA);
    map.put(TextNode.class, Schema.OPTIONAL_STRING_SCHEMA);
    map.put(IntNode.class, Schema.OPTIONAL_INT32_SCHEMA);
    map.put(LongNode.class, Schema.OPTIONAL_INT64_SCHEMA);
    map.put(FloatNode.class, Schema.OPTIONAL_FLOAT32_SCHEMA);
    map.put(DoubleNode.class, Schema.OPTIONAL_FLOAT64_SCHEMA);
    map.put(ArrayNode.class, Schema.OPTIONAL_STRING_SCHEMA);
    map.put(
        ObjectNode.class,
        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build());
    map.put(BigIntegerNode.class, decimalSchema(0));
    map.put(DecimalNode.class, Schema.OPTIONAL_STRING_SCHEMA);
    return ImmutableMap.copyOf(map);
  }

  public static void addField(Map.Entry<String, JsonNode> kv, SchemaBuilder builder) {
    String key = kv.getKey();
    Schema schema = schemaFromNode(kv.getValue());
    if (schema != null) {
      builder.field(key, schema);
    }
  }

  public static Schema schemaFromNode(JsonNode node) {
    if (!node.isNull() && !node.isMissingNode()) {
      if (node.isArray()) {
        return arraySchema((ArrayNode) node);
      } else if (node.isObject()) {
        if (node.elements().hasNext()) {
          return JSON_NODE_TO_SCHEMA.get(node.getClass());
        }
      } else {
        return JSON_NODE_TO_SCHEMA.get(node.getClass());
      }
    }
    return null;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static Schema arraySchema(ArrayNode array) {
    final Schema result;

    if (array.isEmpty()) {
      result = null;
    } else {
      final Class<? extends JsonNode> arrayType = arrayNodeType(array);

      if (arrayType == null) {
        // inconsistent types
        result = ARRAY_OPTIONAL_STRING;
      } else {
        if (arrayType == NullNode.class || arrayType == MissingNode.class) {
          // if types of the array are inconsistent, convert to a string
          result = ARRAY_OPTIONAL_STRING;
        } else {
          if (arrayType == ObjectNode.class) {
            result = ARRAY_MAP_OPTIONAL_STRING;
          } else if (arrayType == ArrayNode.class) {
            // nested array case
            // need to protect against arrays of empty arrays, arrays of empty objects, arrays
            // inconsistent types, etc.

            Set<Schema> nestedSchemas = Sets.newHashSet();
            array
                .elements()
                .forEachRemaining(node -> nestedSchemas.add(JsonToMapUtils.schemaFromNode(node)));

            if (nestedSchemas.size() > 1) {
              // inconsistent types for nested arrays
              result = SchemaBuilder.array(ARRAY_OPTIONAL_STRING).optional().build();
            } else {
              // nestedSchemas.size() == 1 in this case (we already checked array is not empty)
              // i.e. consistent types for nested arrays
              Schema nestedArraySchema = nestedSchemas.iterator().next();
              if (nestedArraySchema == null) {
                result = null;
              } else {
                result = SchemaBuilder.array(nestedArraySchema).optional().build();
              }
            }
          } else {
            // we are a consistent primitive
            result = SchemaBuilder.array(JSON_NODE_TO_SCHEMA.get(arrayType)).optional().build();
          }
        }
      }
    }

    return result;
  }

  /* Kafka Connect arrays must all be the same type */
  public static Class<? extends JsonNode> arrayNodeType(ArrayNode array) {
    Set<Class<? extends JsonNode>> elementTypes = Sets.newHashSet();
    array.elements().forEachRemaining(node -> elementTypes.add(node.getClass()));

    Class<? extends JsonNode> result;
    if (elementTypes.isEmpty()) {
      // empty ArrayNode, cannot determine element type
      result = null;
    } else if (elementTypes.size() == 1) {
      // consistent element type
      result = elementTypes.iterator().next();
    } else {
      // inconsistent element types
      result = null;
    }

    return result;
  }

  public static Struct addToStruct(ObjectNode node, Schema schema, Struct struct) {
    schema
        .fields()
        .forEach(
            field -> {
              JsonNode element = node.get(field.name());
              Schema.Type targetType = field.schema().type();
              switch (targetType) {
                case ARRAY:
                  struct.put(
                      field.name(),
                      populateArray(
                          element,
                          field.schema().valueSchema(),
                          field.name(),
                          Lists.newArrayList()));
                  break;
                case MAP:
                  struct.put(field.name(), populateMap(element, Maps.newHashMap()));
                  break;
                default:
                  struct.put(field.name(), extractValue(element, targetType, field.name()));
                  break;
              }
            });
    return struct;
  }

  public static Object extractValue(JsonNode node, Schema.Type type, String fieldName) {
    Object obj;
    switch (type) {
      case STRING:
        obj = nodeToText(node);
        break;
      case BOOLEAN:
        obj = node.booleanValue();
        break;
      case INT32:
        obj = node.intValue();
        break;
      case INT64:
        obj = node.longValue();
        break;
      case FLOAT32:
        obj = node.floatValue();
        break;
      case FLOAT64:
        obj = node.doubleValue();
        break;
      case MAP:
        ObjectNode mapNode = (ObjectNode) node;
        Map<String, String> map = Maps.newHashMap();
        populateMap(mapNode, map);
        obj = map;
        break;
      case BYTES:
        obj = extractBytes(node, fieldName);
        break;
      default:
        throw new JsonToMapException(
            String.format("Unexpected type %s for field %s", type, fieldName));
    }
    return obj;
  }

  private static Object extractBytes(JsonNode node, String fieldName) {
    Object obj;
    try {
      if (node.isBigInteger()) {
        obj = new BigDecimal(node.bigIntegerValue());
      } else if (node.isBigDecimal()) {
        obj = node.decimalValue();
      } else {
        obj = node.binaryValue();
      }
    } catch (Exception e) {
      throw new JsonToMapException(
          String.format("parsing binary value threw exception for %s", fieldName), e);
    }
    return obj;
  }

  private static List<Object> populateArray(
      JsonNode node, Schema schema, String fieldName, List<Object> acc) {
    if (schema.type() == Schema.Type.ARRAY) {
      node.elements()
          .forEachRemaining(
              arrayNode -> {
                List<Object> nestedList = Lists.newArrayList();
                acc.add(populateArray(arrayNode, schema.valueSchema(), fieldName, nestedList));
              });
    } else {
      node.elements()
          .forEachRemaining(
              arrayEntry -> acc.add(extractValue(arrayEntry, schema.type(), fieldName)));
    }
    return acc;
  }

  public static Map<String, String> populateMap(JsonNode node, Map<String, String> map) {
    for (Iterator<Map.Entry<String, JsonNode>> it = node.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> element = it.next();
      map.put(element.getKey(), nodeToText(element.getValue()));
    }
    return map;
  }

  private static String nodeToText(JsonNode node) {
    if (node.isTextual()) {
      return node.textValue();
    } else {
      return node.toString();
    }
  }
}
