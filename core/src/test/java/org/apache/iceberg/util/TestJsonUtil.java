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

import com.fasterxml.jackson.core.JsonProcessingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestJsonUtil {

  @Test
  public void get() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.get("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.get("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing field: x");

    Assertions.assertThat(JsonUtil.get("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")).asText())
        .isEqualTo("23");
  }

  @Test
  public void getInt() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing int: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: 23.0");

    Assertions.assertThat(JsonUtil.getInt("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
  }

  @Test
  public void getIntOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
    Assertions.assertThat(JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to an integer value: x: 23.0");
  }

  @Test
  public void getLong() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing long: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: 23.0");

    Assertions.assertThat(JsonUtil.getLong("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
  }

  @Test
  public void getLongOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isEqualTo(23);
    Assertions.assertThat(JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23.0}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a long value: x: 23.0");
  }

  @Test
  public void getString() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: 23");

    Assertions.assertThat(JsonUtil.getString("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isEqualTo("23");
  }

  @Test
  public void getStringOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{}"))).isNull();
    Assertions.assertThat(
            JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isEqualTo("23");
    Assertions.assertThat(
            JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: x: 23");
  }

  @Test
  public void getByteBufferOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getByteBufferOrNull("x", JsonUtil.mapper().readTree("{}")))
        .isNull();
    Assertions.assertThat(
            JsonUtil.getByteBufferOrNull("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isNull();

    byte[] bytes = new byte[] {1, 2, 3, 4};
    String base16Str = BaseEncoding.base16().encode(bytes);
    String json = String.format("{\"x\": \"%s\"}", base16Str);
    ByteBuffer byteBuffer = JsonUtil.getByteBufferOrNull("x", JsonUtil.mapper().readTree(json));
    Assertions.assertThat(byteBuffer.array()).isEqualTo(bytes);

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getByteBufferOrNull("x", JsonUtil.mapper().readTree("{\"x\": 23}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse byte buffer from non-text value: x: 23");
  }

  @Test
  public void getBool() throws JsonProcessingException {
    Assertions.assertThatThrownBy(() -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing boolean: x");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: null");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": \"23\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: \"23\"");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": \"true\"}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a boolean value: x: \"true\"");

    Assertions.assertThat(JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": true}")))
        .isTrue();
    Assertions.assertThat(JsonUtil.getBool("x", JsonUtil.mapper().readTree("{\"x\": false}")))
        .isFalse();
  }

  @Test
  public void getIntArrayOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getIntArrayOrNull("items", JsonUtil.mapper().readTree("{}")))
        .isNull();

    Assertions.assertThat(
            JsonUtil.getIntArrayOrNull("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getIntArrayOrNull(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getIntArrayOrNull(
                "items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .isEqualTo(new int[] {23, 45});
  }

  @Test
  public void getIntegerList() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntegerList("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntegerList("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getIntegerList(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in items: \"23\"");

    List<Integer> items = Arrays.asList(23, 45);
    Assertions.assertThat(
            JsonUtil.getIntegerList("items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .isEqualTo(items);

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              JsonUtil.writeIntegerArray("items", items, gen);
              gen.writeEndObject();
            },
            false);
    Assertions.assertThat(JsonUtil.getIntegerList("items", JsonUtil.mapper().readTree(json)))
        .isEqualTo(items);
  }

  @Test
  public void getIntegerSet() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntegerSet("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing set: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getIntegerSet("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getIntegerSet(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getIntegerSet("items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .containsExactlyElementsOf(Arrays.asList(23, 45));
  }

  @Test
  public void getIntegerSetOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getIntegerSetOrNull("items", JsonUtil.mapper().readTree("{}")))
        .isNull();

    Assertions.assertThat(
            JsonUtil.getIntegerSetOrNull("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getIntegerSetOrNull(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse integer from non-int value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getIntegerSetOrNull(
                "items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .containsExactlyElementsOf(Arrays.asList(23, 45));
  }

  @Test
  public void getLongList() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongList("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongList("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getLongList(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse long from non-long value in items: \"23\"");

    List<Long> items = Arrays.asList(23L, 45L);
    Assertions.assertThat(
            JsonUtil.getLongList("items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .isEqualTo(items);

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              JsonUtil.writeLongArray("items", items, gen);
              gen.writeEndObject();
            },
            false);
    Assertions.assertThat(JsonUtil.getLongList("items", JsonUtil.mapper().readTree(json)))
        .isEqualTo(items);
  }

  @Test
  public void getLongListOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getLongListOrNull("items", JsonUtil.mapper().readTree("{}")))
        .isNull();

    Assertions.assertThat(
            JsonUtil.getLongListOrNull("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getLongListOrNull(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse long from non-long value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getLongListOrNull(
                "items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .containsExactlyElementsOf(Arrays.asList(23L, 45L));
  }

  @Test
  public void getLongSet() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongSet("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing set: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getLongSet("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getLongSet(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse long from non-long value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getLongSet("items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .containsExactlyElementsOf(Arrays.asList(23L, 45L));
  }

  @Test
  public void getLongSetOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getLongSetOrNull("items", JsonUtil.mapper().readTree("{}")))
        .isNull();

    Assertions.assertThat(
            JsonUtil.getLongSetOrNull("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getLongSetOrNull(
                    "items", JsonUtil.mapper().readTree("{\"items\": [13, \"23\"]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse long from non-long value in items: \"23\"");

    Assertions.assertThat(
            JsonUtil.getLongSetOrNull("items", JsonUtil.mapper().readTree("{\"items\": [23, 45]}")))
        .containsExactlyElementsOf(Arrays.asList(23L, 45L));
  }

  @Test
  public void getStringList() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringList("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing list: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringList("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringList(
                    "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", 45]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value in items: 45");

    List<String> items = Arrays.asList("23", "45");
    Assertions.assertThat(
            JsonUtil.getStringList(
                "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", \"45\"]}")))
        .containsExactlyElementsOf(items);

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              JsonUtil.writeStringArray("items", items, gen);
              gen.writeEndObject();
            },
            false);
    Assertions.assertThat(JsonUtil.getStringList("items", JsonUtil.mapper().readTree(json)))
        .isEqualTo(items);
  }

  @Test
  public void getStringListOrNull() throws JsonProcessingException {
    Assertions.assertThat(JsonUtil.getStringListOrNull("items", JsonUtil.mapper().readTree("{}")))
        .isNull();

    Assertions.assertThat(
            JsonUtil.getStringListOrNull("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isNull();

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringListOrNull(
                    "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", 45]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value in items: 45");

    Assertions.assertThat(
            JsonUtil.getStringListOrNull(
                "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", \"45\"]}")))
        .containsExactlyElementsOf(Arrays.asList("23", "45"));
  }

  @Test
  public void getStringSet() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringSet("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing set: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringSet("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse JSON array from non-array value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringSet(
                    "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", 45]}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value in items: 45");

    Assertions.assertThat(
            JsonUtil.getStringSet(
                "items", JsonUtil.mapper().readTree("{\"items\": [\"23\", \"45\"]}")))
        .containsExactlyElementsOf(Arrays.asList("23", "45"));
  }

  @Test
  public void getStringMap() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringMap("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: items");

    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringMap("items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string map from non-object value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringMap(
                    "items", JsonUtil.mapper().readTree("{\"items\": {\"a\":\"23\", \"b\":45}}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: b: 45");

    Map<String, String> items = ImmutableMap.of("a", "23", "b", "45");
    Assertions.assertThat(
            JsonUtil.getStringMap(
                "items", JsonUtil.mapper().readTree("{\"items\": {\"a\":\"23\", \"b\":\"45\"}}")))
        .isEqualTo(items);

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              JsonUtil.writeStringMap("items", items, gen);
              gen.writeEndObject();
            },
            false);
    Assertions.assertThat(JsonUtil.getStringMap("items", JsonUtil.mapper().readTree(json)))
        .isEqualTo(items);
  }

  @Test
  public void getStringMapNullableValues() throws JsonProcessingException {
    Assertions.assertThatThrownBy(
            () -> JsonUtil.getStringMapNullableValues("items", JsonUtil.mapper().readTree("{}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing map: items");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringMapNullableValues(
                    "items", JsonUtil.mapper().readTree("{\"items\": null}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string map from non-object value: items: null");

    Assertions.assertThatThrownBy(
            () ->
                JsonUtil.getStringMapNullableValues(
                    "items", JsonUtil.mapper().readTree("{\"items\": {\"a\":\"23\", \"b\":45}}")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: b: 45");

    Map<String, String> itemsWithNullableValues = Maps.newHashMap();
    itemsWithNullableValues.put("a", null);
    itemsWithNullableValues.put("b", null);
    itemsWithNullableValues.put("c", "23");
    Assertions.assertThat(
            JsonUtil.getStringMapNullableValues(
                "items",
                JsonUtil.mapper()
                    .readTree("{\"items\": {\"a\": null, \"b\": null, \"c\": \"23\"}}")))
        .isEqualTo(itemsWithNullableValues);

    String json =
        JsonUtil.generate(
            gen -> {
              gen.writeStartObject();
              JsonUtil.writeStringMap("items", itemsWithNullableValues, gen);
              gen.writeEndObject();
            },
            false);

    Assertions.assertThat(
            JsonUtil.getStringMapNullableValues("items", JsonUtil.mapper().readTree(json)))
        .isEqualTo(itemsWithNullableValues);
  }
}
