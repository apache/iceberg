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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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
        "Cannot parse %s to an integer value: %s", property, pNode);
    return pNode.asInt();
  }

  public static Integer getIntOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToInt(),
        "Cannot parse %s to an integer value: %s", property, pNode);
    return pNode.asInt();
  }

  public static Long getLongOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isIntegralNumber() &&
        pNode.canConvertToLong(), "Cannot parse %s to a long value: %s", property, pNode);
    return pNode.asLong();
  }

  public static long getLong(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing long %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isNumber(),
        "Cannot parse %s to a long value: %s", property, pNode);
    return pNode.asLong();
  }

  public static boolean getBool(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing boolean %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isBoolean(),
        "Cannot parse %s to a boolean value: %s", property, pNode);
    return pNode.asBoolean();
  }

  public static String getString(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing string %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse %s to a string value: %s", property, pNode);
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

  public static String[] getStringArray(JsonNode node) {
    Preconditions.checkArgument(node != null && !node.isNull() && node.isArray(),
        "Cannot parse string array from non-array: %s", node);
    ArrayNode arrayNode = (ArrayNode) node;
    String[] arr = new String[arrayNode.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = arrayNode.get(i).asText();
    }
    return arr;
  }

  public static List<String> getStringList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list %s", property);
    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static Set<String> getStringSet(String property, JsonNode node) {
    Preconditions.checkArgument(node.hasNonNull(property), "Cannot parse missing set %s", property);

    return ImmutableSet.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static List<String> getStringListOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static List<Integer> getIntegerList(String property, JsonNode node) {
    return ImmutableList.<Integer>builder()
        .addAll(new JsonIntegerArrayIterator(property, node))
        .build();
  }

  public static Set<Integer> getIntegerSetOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return getIntegerSet(property, node);
  }

  public static Set<Integer> getIntegerSet(String property, JsonNode node) {
    return ImmutableSet.<Integer>builder()
        .addAll(new JsonIntegerArrayIterator(property, node))
        .build();
  }

  public static Set<Long> getLongSetOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }

    return ImmutableSet.<Long>builder()
        .addAll(new JsonLongArrayIterator(property, node))
        .build();
  }

  public static void writeIntegerFieldIf(boolean condition, String key, Integer value, JsonGenerator generator)
      throws IOException {
    if (condition) {
      generator.writeNumberField(key, value);
    }
  }

  public static void writeLongFieldIf(boolean condition, String key, Long value, JsonGenerator generator)
      throws IOException {
    if (condition) {
      generator.writeNumberField(key, value);
    }
  }

  @FunctionalInterface
  public interface JsonWriter<T> {
    void write(T object, JsonGenerator generator) throws IOException;
  }

  public static <T> void writeObjectList(
      String property,
      Iterable<T> objectList,
      JsonWriter<T> writer,
      JsonGenerator generator)
      throws IOException {
    generator.writeArrayFieldStart(property);
    for (T object : objectList) {
      writer.write(object, generator);
    }
    generator.writeEndArray();
  }

  public static void writeStringList(String property, List<String> stringList, JsonGenerator generator)
      throws IOException {
    generator.writeArrayFieldStart(property);
    for (String s : stringList) {
      generator.writeString(s);
    }
    generator.writeEndArray();
  }

  public static void writeStringMap(String property, Map<String, String> map, JsonGenerator generator)
      throws IOException {
    generator.writeObjectFieldStart(property);
    for (Map.Entry<String, String> entry : map.entrySet()) {
      generator.writeStringField(entry.getKey(), entry.getValue());
    }
    generator.writeEndObject();
  }

  @FunctionalInterface
  public interface JsonReader<T> {
    T read(JsonNode node);
  }

  public static <T> T getObject(String property, JsonNode node, JsonReader<T> reader) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing object %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(pNode.isObject(),
        "Cannot parse %s from non-object value: %s", property, pNode);
    return reader.read(pNode);
  }

  public static <T> List<T> getObjectList(String property, JsonNode node, Function<JsonNode, T> reader) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list %s", property);
    return ImmutableList.<T>builder()
        .addAll(objectArrayIterator(property, node, reader))
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

  static class JsonLongArrayIterator extends JsonArrayIterator<Long> {

    JsonLongArrayIterator(String property, JsonNode node) {
      super(property, node);
    }

    @Override
    Long convert(JsonNode element) {
      return element.asLong();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(
          element.isIntegralNumber() && element.canConvertToLong(),
          "Cannot parse long from  non-long value: %s",
          element);
    }
  }

  static <T> Iterator<T> objectArrayIterator(String property, JsonNode node, Function<JsonNode, T> reader) {
    return new JsonArrayIterator<T>(property, node) {
      protected T convert(JsonNode element) {
        return reader.apply(element);
      }

      protected void validate(JsonNode element) {
        Preconditions.checkArgument(
            element.isObject(),
            "Cannot parse %s from non-object value: %s", property, element);
      }
    };
  }
}
