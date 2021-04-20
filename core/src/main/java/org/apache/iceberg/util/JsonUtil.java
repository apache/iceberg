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

package org.apache.iceberg.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public class JsonUtil {

  private JsonUtil() {
  }

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  public static JsonFactory factory() {
    return FACTORY;
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  public static int getInt(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing int %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isNumber(),
        "Cannot parse %s from non-numeric value: %s", property, pNode);
    return pNode.asInt();
  }

  public static Integer getIntOrNull(String property, JsonNode node) {
    if (!node.has(property)) {
      return null;
    }
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToInt(),
        "Cannot parse %s from non-string value: %s", property, pNode);
    return pNode.asInt();
  }

  public static long getLong(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing long %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isNumber(),
        "Cannot parse %s from non-numeric value: %s", property, pNode);
    return pNode.asLong();
  }

  public static boolean getBool(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing boolean %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isBoolean(),
        "Cannot parse %s from non-boolean value: %s", property, pNode);
    return pNode.asBoolean();
  }

  public static String getString(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing string %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse %s from non-string value: %s", property, pNode);
    return pNode.asText();
  }

  public static String getStringOrNull(String property, JsonNode node) {
    if (!node.has(property)) {
      return null;
    }
    JsonNode pNode = node.get(property);
    if (pNode != null && pNode.isNull()) {
      return null;
    }
    Preconditions.checkArgument(pNode != null && pNode.isTextual(),
        "Cannot parse %s from non-string value: %s", property, pNode);
    return pNode.asText();
  }

  public static Map<String, String> getStringMap(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing map %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isObject(),
        "Cannot parse %s from non-object value: %s", property, pNode);

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    Iterator<String> fields = pNode.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      builder.put(field, getString(field, pNode));
    }
    return builder.build();
  }

  public static List<String> getStringList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list %s", property);
    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static Set<Integer> getIntegerSetOrNull(String property, JsonNode node) {
    if (!node.has(property)) {
      return null;
    }

    return ImmutableSet.<Integer>builder()
        .addAll(new JsonIntegerArrayIterator(property, node))
        .build();
  }

  abstract static class JsonArrayIterator<T> implements Iterator<T> {

    private final Iterator<JsonNode> elements;

    JsonArrayIterator(String property, JsonNode node) {
      JsonNode pNode = node.get(property);
      Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isArray(),
          "Cannot parse %s from non-array value: %s", property, pNode);
      this.elements = pNode.elements();
    }

    @Override
    public boolean hasNext() {
      return elements.hasNext();
    }

    @Override
    public T next() {
      JsonNode element = elements.next();
      validate(element);
      return convert(element);
    }

    abstract T convert(JsonNode element);

    abstract void validate(JsonNode element);
  }

  static class JsonStringArrayIterator extends JsonArrayIterator<String> {

    JsonStringArrayIterator(String property, JsonNode node) {
      super(property, node);
    }

    @Override
    String convert(JsonNode element) {
      return element.asText();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(element.isTextual(), "Cannot parse string from non-text value: %s", element);
    }
  }

  static class JsonIntegerArrayIterator extends JsonArrayIterator<Integer> {

    JsonIntegerArrayIterator(String property, JsonNode node) {
      super(property, node);
    }

    @Override
    Integer convert(JsonNode element) {
      return element.asInt();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(element.isInt(), "Cannot parse integer from non-int value: %s", element);
    }
  }
}
