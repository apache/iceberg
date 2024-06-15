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
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.JsonUtil;

public class MultiDimensionCounterResultParser {
  private static final String MISSING_FIELD_ERROR_MSG =
      "Cannot parse counter from '%s': Missing field '%s'";
  private static final String UNIT = "unit";
  private static final String VALUE = "value";
  private static final String COUNTER_ID = "counter-id";
  private static final String COUNTERS = "counters";

  private MultiDimensionCounterResultParser() {}

  static void toJson(String name, MultiDimensionCounterResult multiCounter, JsonGenerator gen)
      throws IOException {
    Preconditions.checkArgument(null != multiCounter, "Invalid counter: null");

    gen.writeFieldName(name);
    gen.writeStartObject();
    gen.writeStringField(UNIT, multiCounter.unit().displayName());
    gen.writeArrayFieldStart(COUNTERS);
    for (Map.Entry<String, Long> counterResult : multiCounter.counterResults().entrySet()) {
      gen.writeStartObject();
      gen.writeStringField(COUNTER_ID, counterResult.getKey());
      gen.writeNumberField(VALUE, counterResult.getValue());
      gen.writeEndObject();
    }
    gen.writeEndArray();
    gen.writeEndObject();
  }

  static MultiDimensionCounterResult fromJson(String multiCounterName, JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse counter from null object");
    Preconditions.checkArgument(json.isObject(), "Cannot parse counter from non-object: %s", json);

    if (!json.has(multiCounterName)) {
      return null;
    }

    JsonNode multiCounter = json.get(multiCounterName);
    Preconditions.checkArgument(
        multiCounter.has(UNIT), MISSING_FIELD_ERROR_MSG, multiCounterName, UNIT);
    Preconditions.checkArgument(
        multiCounter.has(COUNTERS), MISSING_FIELD_ERROR_MSG, multiCounterName, COUNTERS);

    String multiCounterUnit = JsonUtil.getString(UNIT, multiCounter);
    JsonNode counters = multiCounter.get(COUNTERS);
    Preconditions.checkArgument(
        counters.isArray(), "'%s' should be an array in '%s'", COUNTERS, multiCounterName);
    ArrayNode countersArray = (ArrayNode) counters;

    Map<String, Long> counterResults = Maps.newHashMap();
    Iterator<JsonNode> it = countersArray.elements();
    while (it.hasNext()) {
      JsonNode item = it.next();
      Preconditions.checkArgument(
          item.has(COUNTER_ID), MISSING_FIELD_ERROR_MSG, COUNTERS, COUNTER_ID);
      Preconditions.checkArgument(item.has(VALUE), MISSING_FIELD_ERROR_MSG, COUNTERS, VALUE);
      String counterId = JsonUtil.getString(COUNTER_ID, item);
      long value = JsonUtil.getLong(VALUE, item);

      counterResults.put(counterId, value);
    }

    return MultiDimensionCounterResult.of(
        multiCounterName, MetricsContext.Unit.fromDisplayName(multiCounterUnit), counterResults);
  }
}
