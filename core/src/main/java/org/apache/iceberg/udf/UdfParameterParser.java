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
package org.apache.iceberg.udf;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class UdfParameterParser {
  private static final String NAME = "name";
  private static final String TYPE = "type";
  private static final String DOC = "doc";

  private UdfParameterParser() {}

  static String toJson(UdfParameter parameter) {
    return JsonUtil.generate(gen -> toJson(parameter, gen), false);
  }

  static void toJson(UdfParameter parameter, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(parameter != null, "Invalid UDF parameter: null");
    generator.writeStartObject();
    generator.writeStringField(NAME, parameter.name());
    UdfTypeUtil.writeType(TYPE, parameter.type(), generator);
    if (parameter.doc() != null) {
      generator.writeStringField(DOC, parameter.doc());
    }

    generator.writeEndObject();
  }

  static UdfParameter fromJson(String json) {
    return JsonUtil.parse(json, UdfParameterParser::fromJson);
  }

  static UdfParameter fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse UDF parameter from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse UDF parameter from non-object: %s", node);

    ImmutableUdfParameter.Builder builder =
        ImmutableUdfParameter.builder()
            .name(JsonUtil.getString(NAME, node))
            .type(UdfTypeUtil.readType(node.get(TYPE)));

    if (node.has(DOC)) {
      builder.doc(JsonUtil.getString(DOC, node));
    }

    return builder.build();
  }
}
