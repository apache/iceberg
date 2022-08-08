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

  private static final String NAME = "name";
  private static final String UNIT = "unit";
  private static final String VALUE = "value";

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
    gen.writeStringField(UNIT, counter.unit().displayName());
    gen.writeNumberField(VALUE, counter.value().longValue());
    gen.writeEndObject();
  }

  static CounterResult<Long> fromJson(String json) {
    return JsonUtil.parse(json, CounterResultParser::fromJson);
  }

  static CounterResult<Long> fromJson(String property, JsonNode json) {
    return fromJson(get(property, json));
  }

  static CounterResult<Long> fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse counter from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);

    String name = JsonUtil.getString(NAME, json);
    String unit = JsonUtil.getString(UNIT, json);
    long value = JsonUtil.getLong(VALUE, json);
    return new CounterResult<>(name, Unit.fromDisplayName(unit), value);
  }

  private static JsonNode get(String property, JsonNode node) {
    Preconditions.checkArgument(
        node.has(property), "Cannot parse counter from missing field: %s", property);

    JsonNode counter = node.get(property);
    Preconditions.checkArgument(counter.has(NAME), MISSING_FIELD_ERROR_MSG, property, NAME);
    Preconditions.checkArgument(counter.has(UNIT), MISSING_FIELD_ERROR_MSG, property, UNIT);
    Preconditions.checkArgument(counter.has(VALUE), MISSING_FIELD_ERROR_MSG, property, VALUE);
    return counter;
  }
}
