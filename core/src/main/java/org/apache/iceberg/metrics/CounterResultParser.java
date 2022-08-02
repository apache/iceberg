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
package org.apache.iceberg.metrics;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.metrics.MetricsContext.Unit;
import org.apache.iceberg.metrics.ScanReport.CounterResult;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class CounterResultParser {
  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse counter from '%s': Missing field '%s'";

  static final CounterResult<Integer> NOOP_INT_COUNTER =
      new CounterResult<>("undefined", Unit.UNDEFINED, 0);
  static final CounterResult<Long> NOOP_LONG_COUNTER =
      new CounterResult<>("undefined", Unit.UNDEFINED, 0L);

  private static final String NAME = "name";
  private static final String UNIT = "unit";
  private static final String VALUE = "value";
  private static final String TYPE = "type";

  private CounterResultParser() {}

  static String toJson(CounterResult<?> counterResult) {
    return toJson(counterResult, false);
  }

  static String toJson(CounterResult<?> counterResult, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(counterResult, gen), pretty);
  }

  static void toJson(CounterResult<?> counter, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != counter, "Invalid counter: null");

    gen.writeStartObject();
    gen.writeStringField(NAME, counter.name());
    gen.writeStringField(UNIT, counter.unit().name());
    gen.writeNumberField(VALUE, counter.value().longValue());
    if (counter.value() instanceof Integer) {
      gen.writeStringField(TYPE, Integer.class.getName());
    } else {
      gen.writeStringField(TYPE, Long.class.getName());
    }
    gen.writeEndObject();
  }

  static CounterResult<?> fromJson(String json) {
    return JsonUtil.parse(json, CounterResultParser::fromJson);
  }

  static CounterResult<?> fromJson(JsonNode json) {
    if (null == json) {
      return NOOP_INT_COUNTER;
    }
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);
    String type = JsonUtil.getString(TYPE, json);
    if (Integer.class.getName().equals(type)) {
      return intCounterFromJson(json);
    }
    return longCounterFromJson(json);
  }

  static CounterResult<Integer> intCounterFromJson(String property, JsonNode json) {
    if (null == json) {
      return NOOP_INT_COUNTER;
    }
    return intCounterFromJson(get(property, json));
  }

  static CounterResult<Integer> intCounterFromJson(JsonNode json) {
    if (null == json) {
      return NOOP_INT_COUNTER;
    }
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);
    String type = JsonUtil.getString(TYPE, json);
    Preconditions.checkArgument(
        Integer.class.getName().equals(type), "Cannot parse to int counter: %s", json);

    String name = JsonUtil.getString(NAME, json);
    String unit = JsonUtil.getString(UNIT, json);
    int value = JsonUtil.getInt(VALUE, json);
    return new CounterResult<>(name, Unit.valueOf(unit), value);
  }

  static CounterResult<Long> longCounterFromJson(String property, JsonNode json) {
    if (null == json) {
      return NOOP_LONG_COUNTER;
    }
    return longCounterFromJson(get(property, json));
  }

  static CounterResult<Long> longCounterFromJson(JsonNode json) {
    if (null == json) {
      return NOOP_LONG_COUNTER;
    }
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);
    String type = JsonUtil.getString(TYPE, json);
    Preconditions.checkArgument(
        Long.class.getName().equals(type), "Cannot parse to long counter: %s", json);

    String name = JsonUtil.getString(NAME, json);
    String unit = JsonUtil.getString(UNIT, json);
    long value = JsonUtil.getLong(VALUE, json);
    return new CounterResult<>(name, Unit.valueOf(unit), value);
  }

  private static JsonNode get(String property, JsonNode node) {
    Preconditions.checkArgument(
        node.has(property), "Cannot parse counter from missing object '%s'", property);
    JsonNode counter = node.get(property);
    Preconditions.checkArgument(counter.has(NAME), MISSING_FIELD_ERROR_MSG, property, NAME);
    Preconditions.checkArgument(counter.has(TYPE), MISSING_FIELD_ERROR_MSG, property, TYPE);
    Preconditions.checkArgument(counter.has(UNIT), MISSING_FIELD_ERROR_MSG, property, UNIT);
    Preconditions.checkArgument(counter.has(VALUE), MISSING_FIELD_ERROR_MSG, property, VALUE);
    return counter;
  }
}
