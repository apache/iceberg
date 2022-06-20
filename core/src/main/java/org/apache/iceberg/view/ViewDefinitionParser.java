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
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.util.JsonUtil;

class ViewDefinitionParser {
  private enum Field {
    SQL("sql"),
    DIALECT("dialect"),
    SCHEMA("schema"),
    DEFAULT_CATALOG("default-catalog"),
    DEFAULT_NAMESPACE("default-namespace"),
    FIELD_ALIASES("field-aliases"),
    FIELD_COMMENTS("field-comments");

    private final String name;

    Field(String name) {
      this.name = name;
    }
  }

  static void toJson(ViewDefinition view, JsonGenerator generator) throws IOException {
    generator.writeStartObject();
    generator.writeStringField(
        ViewRepresentationParser.Field.TYPE.fieldName(), view.type().typeName());
    generator.writeStringField(ViewDefinitionParser.Field.SQL.name, view.sql());
    generator.writeStringField(ViewDefinitionParser.Field.DIALECT.name, view.dialect());
    generator.writeFieldName(ViewDefinitionParser.Field.SCHEMA.name);
    SchemaParser.toJson(view.schema(), generator);
    generator.writeStringField(
        ViewDefinitionParser.Field.DEFAULT_CATALOG.name, view.defaultCatalog());
    JsonUtil.writeStringList(
        ViewDefinitionParser.Field.DEFAULT_NAMESPACE.name, view.defaultNamespace(), generator);
    JsonUtil.writeStringList(
        ViewDefinitionParser.Field.FIELD_ALIASES.name, view.fieldAliases(), generator);
    JsonUtil.writeStringList(
        ViewDefinitionParser.Field.FIELD_COMMENTS.name, view.fieldComments(), generator);
    generator.writeEndObject();
  }

  static ViewDefinition fromJson(JsonNode node) {
    return BaseViewDefinition.builder()
        .sql(JsonUtil.getString(ViewDefinitionParser.Field.SQL.name, node))
        .dialect(JsonUtil.getString(ViewDefinitionParser.Field.DIALECT.name, node))
        .schema(
            JsonUtil.getObject(
                ViewDefinitionParser.Field.SCHEMA.name, node, SchemaParser::fromJson))
        .defaultCatalog(JsonUtil.getString(ViewDefinitionParser.Field.DEFAULT_CATALOG.name, node))
        .defaultNamespace(
            JsonUtil.getStringList(ViewDefinitionParser.Field.DEFAULT_NAMESPACE.name, node))
        .fieldAliases(JsonUtil.getStringList(ViewDefinitionParser.Field.FIELD_ALIASES.name, node))
        .fieldComments(JsonUtil.getStringList(ViewDefinitionParser.Field.FIELD_COMMENTS.name, node))
        .build();
  }

  private ViewDefinitionParser() {}
}
