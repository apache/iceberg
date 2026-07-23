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
package org.apache.iceberg.rest.labels;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class LabelsParser {
  private static final String OBJECT = "object";
  private static final String FIELDS = "fields";

  private LabelsParser() {}

  /** Returns true when the labels carry neither object-level nor field-level entries. */
  public static boolean isEmpty(Labels labels) {
    return labels.object().isEmpty() && labels.fields().isEmpty();
  }

  public static String toJson(Labels labels) {
    return toJson(labels, false);
  }

  public static String toJson(Labels labels, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(labels, gen), pretty);
  }

  public static void toJson(Labels labels, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != labels, "Invalid labels: null");

    gen.writeStartObject();

    if (!labels.object().isEmpty()) {
      JsonUtil.writeStringMap(OBJECT, labels.object(), gen);
    }

    if (!labels.fields().isEmpty()) {
      gen.writeArrayFieldStart(FIELDS);
      for (FieldLabels fieldLabels : labels.fields()) {
        FieldLabelsParser.toJson(fieldLabels, gen);
      }

      gen.writeEndArray();
    }

    gen.writeEndObject();
  }

  public static Labels fromJson(String json) {
    return JsonUtil.parse(json, LabelsParser::fromJson);
  }

  public static Labels fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse labels from null object");

    ImmutableLabels.Builder builder = ImmutableLabels.builder();

    if (json.hasNonNull(OBJECT)) {
      builder.object(JsonUtil.getStringMap(OBJECT, json));
    }

    if (json.hasNonNull(FIELDS)) {
      builder.fields(JsonUtil.getObjectList(FIELDS, json, FieldLabelsParser::fromJson));
    }

    return builder.build();
  }
}
