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

public class FieldLabelsParser {
  private static final String FIELD_ID = "field-id";
  private static final String LABELS = "labels";

  private FieldLabelsParser() {}

  public static String toJson(FieldLabels fieldLabels) {
    return toJson(fieldLabels, false);
  }

  public static String toJson(FieldLabels fieldLabels, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(fieldLabels, gen), pretty);
  }

  public static void toJson(FieldLabels fieldLabels, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != fieldLabels, "Invalid field labels: null");

    gen.writeStartObject();

    gen.writeNumberField(FIELD_ID, fieldLabels.fieldId());
    JsonUtil.writeStringMap(LABELS, fieldLabels.labels(), gen);

    gen.writeEndObject();
  }

  public static FieldLabels fromJson(String json) {
    return JsonUtil.parse(json, FieldLabelsParser::fromJson);
  }

  public static FieldLabels fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse field labels from null object");

    return ImmutableFieldLabels.builder()
        .fieldId(JsonUtil.getInt(FIELD_ID, json))
        .labels(JsonUtil.getStringMap(LABELS, json))
        .build();
  }
}
