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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestJsonUtil {

  @FunctionalInterface
  public interface JsonStringWriter<T> {
    String write(T object);
  }

  public static <T> JsonStringWriter<T> jsonStringWriter(JsonUtil.JsonWriter<T> writer) {
    return entry -> {
      Writer jsonWriter = new StringWriter();
      try {
        JsonGenerator generator = JsonUtil.factory().createGenerator(jsonWriter);
        writer.write(entry, generator);
        generator.flush();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return jsonWriter.toString();
    };
  }

  public static <T> String toJsonString(T entry, JsonUtil.JsonWriter<T> writer) {
    return jsonStringWriter(writer).write(entry);
  }

  public static JsonNode fromJsonString(String json) {
    try {
      return JsonUtil.mapper().readValue(json, JsonNode.class);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  @FunctionalInterface
  public interface JsonStringReader<T> {
    T read(String json);
  }

  public static <T> JsonStringReader<T> jsonStringReader(JsonUtil.JsonReader<T> reader) {
    return json -> reader.read(fromJsonString(json));
  }

  public static <T> T fromJsonString(String json, JsonUtil.JsonReader<T> reader) {
    return jsonStringReader(reader).read(json);
  }

  public static Collector<CharSequence, ?, String> joiningJsonArray() {
    return Collectors.joining(",", "[", "]");
  }

  public static Collector<CharSequence, ?, String> joiningJsonObject() {
    return Collectors.joining(",", "{", "}");
  }

  public static String arrayString(String... strings) {
    return Stream.of(strings).collect(joiningJsonArray());
  }

  public static String objectString(String... strings) {
    return Stream.of(strings).collect(joiningJsonObject());
  }

  private TestJsonUtil() {}
}
