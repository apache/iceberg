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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class CounterResultParser {

  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse counter from '%s': Missing field '%s'";

  private static final String UNIT = "unit";
  private static final String VALUE = "value";

  private CounterResultParser() {}

  static String toJson(CounterResult counter) {
    return toJson(counter, false);
  }

  static String toJson(CounterResult counter, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(counter, gen), pretty);
  }

  static void toJson(CounterResult counter, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != counter, "Invalid counter: null");

    gen.writeStartObject();
    gen.writeStringField(UNIT, counter.unit().displayName());
    gen.writeNumberField(VALUE, counter.value());
    gen.writeEndObject();
  }

  static CounterResult fromJson(String json) {
    return JsonUtil.parse(json, CounterResultParser::fromJson);
  }

  static CounterResult fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse counter from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);

    String unit = JsonUtil.getString(UNIT, json);
    long value = JsonUtil.getLong(VALUE, json);
    return CounterResult.of(Unit.fromDisplayName(unit), value);
  }

  /**
   * This is mainly used from {@link ScanMetricsResultParser} where the counter name is already part
   * of the parent {@link JsonNode}, so we omit checking and reading the counter name here.
   *
   * @param counterName The counter name
   * @param json The {@link JsonNode} containing all other timer information
   * @return A {@link CounterResult} instance
   */
  static CounterResult fromJson(String counterName, JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse counter from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);

    if (!json.has(counterName)) {
      return null;
    }

    JsonNode counter = json.get(counterName);
    Preconditions.checkArgument(counter.has(UNIT), MISSING_FIELD_ERROR_MSG, counterName, UNIT);
    Preconditions.checkArgument(counter.has(VALUE), MISSING_FIELD_ERROR_MSG, counterName, VALUE);

    String unit = JsonUtil.getString(UNIT, counter);
    long value = JsonUtil.getLong(VALUE, counter);
    return CounterResult.of(Unit.fromDisplayName(unit), value);
  }
}
