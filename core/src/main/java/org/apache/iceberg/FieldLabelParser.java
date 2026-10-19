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
package org.apache.iceberg;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

public class FieldLabelParser {
  private static final String FIELD_ID = "field-id";
  private static final String LABELS = "labels";

  private FieldLabelParser() {}

  public static String toJson(FieldLabel fieldLabel) {
    return toJson(fieldLabel, false);
  }

  public static String toJson(FieldLabel fieldLabel, boolean pretty) {
    return JsonUtil.generate(gen -> toJson(fieldLabel, gen), pretty);
  }

  public static void toJson(FieldLabel fieldLabel, JsonGenerator gen) throws IOException {
    Preconditions.checkArgument(null != fieldLabel, "Invalid field labels: null");

    gen.writeStartObject();

    gen.writeNumberField(FIELD_ID, fieldLabel.fieldId());
    JsonUtil.writeStringMap(LABELS, fieldLabel.labels(), gen);

    gen.writeEndObject();
  }

  public static FieldLabel fromJson(String json) {
    return JsonUtil.parse(json, FieldLabelParser::fromJson);
  }

  public static FieldLabel fromJson(JsonNode json) {
    Preconditions.checkArgument(null != json, "Cannot parse field labels from null object");

    return ImmutableFieldLabel.builder()
        .fieldId(JsonUtil.getInt(FIELD_ID, json))
        .labels(JsonUtil.getStringMap(LABELS, json))
        .build();
  }
}
