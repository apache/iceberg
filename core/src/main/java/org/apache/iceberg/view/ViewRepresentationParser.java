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

class ViewRepresentationParser {

  enum Field {
    TYPE;

    public String fieldName() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }

  public static void toJson(ViewRepresentation representation, JsonGenerator generator) throws IOException {
    switch (representation.type()) {
      case SQL:
        ViewDefinitionParser.toJson((ViewDefinition) representation, generator);
        break;

      default:
        throw new IllegalArgumentException(String.format("Unknown view representation type '%s' to serialize",
            representation.type()));
    }
  }

  public static ViewRepresentation fromJson(JsonNode node) {
    String typeName = node.get(Field.TYPE.fieldName()).asText();
    ViewRepresentation.Type type = ViewRepresentation.Type.parse(typeName);

    switch (type) {
      case SQL:
        return ViewDefinitionParser.fromJson(node);

      default:
        throw new IllegalStateException(String.format("Unknown view representation type '%s' to deserialize", type));
    }
  }

  private ViewRepresentationParser() {
  }
}

