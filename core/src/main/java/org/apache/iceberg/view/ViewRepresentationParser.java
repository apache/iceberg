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
package org.apache.iceberg.view;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.JsonUtil;

class ViewRepresentationParser {
  static final String TYPE = "type";

  private ViewRepresentationParser() {}

  static void toJson(ViewRepresentation representation, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(representation != null, "Invalid view representation: null");
    switch (representation.type().toLowerCase(Locale.ENGLISH)) {
      case ViewRepresentation.Type.SQL:
        SQLViewRepresentationParser.toJson((SQLViewRepresentation) representation, generator);
        break;

      default:
        throw new UnsupportedOperationException(
            String.format(
                "Cannot serialize unsupported view representation: %s", representation.type()));
    }
  }

  static String toJson(ViewRepresentation entry) {
    return JsonUtil.generate(gen -> toJson(entry, gen), false);
  }

  static ViewRepresentation fromJson(String json) {
    return JsonUtil.parse(json, ViewRepresentationParser::fromJson);
  }

  static ViewRepresentation fromJson(JsonNode node) {
    Preconditions.checkArgument(node != null, "Cannot parse view representation from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse view representation from non-object: %s", node);
    String type = JsonUtil.getString(TYPE, node).toLowerCase(Locale.ENGLISH);
    switch (type) {
      case ViewRepresentation.Type.SQL:
        return SQLViewRepresentationParser.fromJson(node);

      default:
        return ImmutableUnknownViewRepresentation.builder().type(type).build();
    }
  }
}
