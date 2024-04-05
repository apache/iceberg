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
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;

public class JsonUtil {

  private JsonUtil() {}

  private static final JsonFactory FACTORY = new JsonFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  public static JsonFactory factory() {
    return FACTORY;
  }

  public static ObjectMapper mapper() {
    return MAPPER;
  }

  @FunctionalInterface
  public interface ToJson {
    void generate(JsonGenerator gen) throws IOException;
  }

  /**
   * Helper for writing JSON with a JsonGenerator.
   *
   * @param toJson a function to produce JSON using a JsonGenerator
   * @param pretty whether to pretty-print JSON for readability
   * @return a JSON string produced from the generator
   */
  public static String generate(ToJson toJson, boolean pretty) {
    try (StringWriter writer = new StringWriter();
        JsonGenerator generator = JsonUtil.factory().createGenerator(writer)) {
      if (pretty) {
        generator.useDefaultPrettyPrinter();
      }
      toJson.generate(generator);
      generator.flush();
      return writer.toString();

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @FunctionalInterface
  public interface FromJson<T> {
    T parse(JsonNode node);
  }

  /**
   * Helper for parsing JSON from a String.
   *
   * @param json a JSON string
   * @param parser a function that converts a JsonNode to a Java object
   * @param <T> type of objects created by the parser
   * @return the parsed Java object
   */
  public static <T> T parse(String json, FromJson<T> parser) {
    try {
      return parser.parse(JsonUtil.mapper().readValue(json, JsonNode.class));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static JsonNode get(String property, JsonNode node) {
    Preconditions.checkArgument(
        node.hasNonNull(property), "Cannot parse missing field: %s", property);
    return node.get(property);
  }

  public static int getInt(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing int: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToInt(),
        "Cannot parse to an integer value: %s: %s",
        property,
        pNode);
    return pNode.asInt();
  }

  public static Integer getIntOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }
    return getInt(property, node);
  }

  public static Long getLongOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }
    return getLong(property, node);
  }

  public static long getLong(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing long: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isIntegralNumber() && pNode.canConvertToLong(),
        "Cannot parse to a long value: %s: %s",
        property,
        pNode);
    return pNode.asLong();
  }

  public static boolean getBool(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing boolean: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isBoolean(),
        "Cannot parse to a boolean value: %s: %s",
        property,
        pNode);
    return pNode.asBoolean();
  }

  public static String getString(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing string: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isTextual(),
        "Cannot parse to a string value: %s: %s",
        property,
        pNode);
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
    return getString(property, node);
  }

  public static ByteBuffer getByteBufferOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode.isTextual(), "Cannot parse byte buffer from non-text value: %s: %s", property, pNode);
    return ByteBuffer.wrap(
        BaseEncoding.base16().decode(pNode.textValue().toUpperCase(Locale.ROOT)));
  }

  public static Map<String, String> getStringMap(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing map: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isObject(),
        "Cannot parse string map from non-object value: %s: %s",
        property,
        pNode);

    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    Iterator<String> fields = pNode.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      builder.put(field, getString(field, pNode));
    }
    return builder.build();
  }

  public static Map<String, String> getStringMapNullableValues(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing map: %s", property);
    JsonNode pNode = node.get(property);
    Preconditions.checkArgument(
        pNode != null && !pNode.isNull() && pNode.isObject(),
        "Cannot parse string map from non-object value: %s: %s",
        property,
        pNode);

    Map<String, String> map = Maps.newHashMap();
    Iterator<String> fields = pNode.fieldNames();
    while (fields.hasNext()) {
      String field = fields.next();
      map.put(field, getStringOrNull(field, pNode));
    }

    return map;
  }

  public static String[] getStringArray(JsonNode node) {
    Preconditions.checkArgument(
        node != null && !node.isNull() && node.isArray(),
        "Cannot parse string array from non-array: %s",
        node);
    ArrayNode arrayNode = (ArrayNode) node;
    String[] arr = new String[arrayNode.size()];
    for (int i = 0; i < arr.length; i++) {
      arr[i] = arrayNode.get(i).asText();
    }
    return arr;
  }

  public static List<String> getStringList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list: %s", property);
    return ImmutableList.<String>builder()
        .addAll(new JsonStringArrayIterator(property, node))
        .build();
  }

  public static Set<String> getStringSet(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing set: %s", property);

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

  public static int[] getIntArrayOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return ArrayUtil.toIntArray(getIntegerList(property, node));
  }

  public static List<Integer> getIntegerList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list: %s", property);
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
    Preconditions.checkArgument(node.has(property), "Cannot parse missing set: %s", property);
    return ImmutableSet.<Integer>builder()
        .addAll(new JsonIntegerArrayIterator(property, node))
        .build();
  }

  public static List<Long> getLongList(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing list: %s", property);
    return ImmutableList.<Long>builder().addAll(new JsonLongArrayIterator(property, node)).build();
  }

  public static List<Long> getLongListOrNull(String property, JsonNode node) {
    if (!node.has(property) || node.get(property).isNull()) {
      return null;
    }

    return ImmutableList.<Long>builder().addAll(new JsonLongArrayIterator(property, node)).build();
  }

  public static Set<Long> getLongSetOrNull(String property, JsonNode node) {
    if (!node.hasNonNull(property)) {
      return null;
    }

    return getLongSet(property, node);
  }

  public static Set<Long> getLongSet(String property, JsonNode node) {
    Preconditions.checkArgument(node.has(property), "Cannot parse missing set: %s", property);
    return ImmutableSet.<Long>builder().addAll(new JsonLongArrayIterator(property, node)).build();
  }

  public static void writeIntegerFieldIf(
      boolean condition, String key, Integer value, JsonGenerator generator) throws IOException {
    if (condition) {
      generator.writeNumberField(key, value);
    }
  }

  public static void writeLongFieldIf(
      boolean condition, String key, Long value, JsonGenerator generator) throws IOException {
    if (condition) {
      generator.writeNumberField(key, value);
    }
  }

  abstract static class JsonArrayIterator<T> implements Iterator<T> {

    private final Iterator<JsonNode> elements;

    JsonArrayIterator(String property, JsonNode node) {
      JsonNode pNode = node.get(property);
      Preconditions.checkArgument(
          pNode != null && !pNode.isNull() && pNode.isArray(),
          "Cannot parse JSON array from non-array value: %s: %s",
          property,
          pNode);
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
    private final String property;

    JsonStringArrayIterator(String property, JsonNode node) {
      super(property, node);
      this.property = property;
    }

    @Override
    String convert(JsonNode element) {
      return element.asText();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(
          element.isTextual(),
          "Cannot parse string from non-text value in %s: %s",
          property,
          element);
    }
  }

  static class JsonIntegerArrayIterator extends JsonArrayIterator<Integer> {
    private final String property;

    JsonIntegerArrayIterator(String property, JsonNode node) {
      super(property, node);
      this.property = property;
    }

    @Override
    Integer convert(JsonNode element) {
      return element.asInt();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(
          element.isInt(), "Cannot parse integer from non-int value in %s: %s", property, element);
    }
  }

  static class JsonLongArrayIterator extends JsonArrayIterator<Long> {
    private final String property;

    JsonLongArrayIterator(String property, JsonNode node) {
      super(property, node);
      this.property = property;
    }

    @Override
    Long convert(JsonNode element) {
      return element.asLong();
    }

    @Override
    void validate(JsonNode element) {
      Preconditions.checkArgument(
          element.isIntegralNumber() && element.canConvertToLong(),
          "Cannot parse long from non-long value in %s: %s",
          property,
          element);
    }
  }

  public static void writeIntegerArray(String property, Iterable<Integer> items, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(property);
    for (Integer item : items) {
      gen.writeNumber(item);
    }
    gen.writeEndArray();
  }

  public static void writeLongArray(String property, Iterable<Long> items, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(property);
    for (Long item : items) {
      gen.writeNumber(item);
    }
    gen.writeEndArray();
  }

  public static void writeStringArray(String property, Iterable<String> items, JsonGenerator gen)
      throws IOException {
    gen.writeArrayFieldStart(property);
    for (String item : items) {
      gen.writeString(item);
    }
    gen.writeEndArray();
  }

  public static void writeStringMap(String property, Map<String, String> map, JsonGenerator gen)
      throws IOException {
    gen.writeObjectFieldStart(property);
    for (Map.Entry<String, String> pair : map.entrySet()) {
      gen.writeStringField(pair.getKey(), pair.getValue());
    }
    gen.writeEndObject();
  }
}
