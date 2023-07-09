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
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.util.JsonUtil;

class SQLViewRepresentationParser {
  private static final String SQL = "sql";
  private static final String DIALECT = "dialect";
  private static final String SCHEMA_ID = "schema-id";
  private static final String DEFAULT_CATALOG = "default-catalog";
  private static final String DEFAULT_NAMESPACE = "default-namespace";
  private static final String FIELD_ALIASES = "field-aliases";
  private static final String FIELD_COMMENTS = "field-comments";

  private SQLViewRepresentationParser() {}

  static String toJson(SQLViewRepresentation sqlViewRepresentation) {
    return JsonUtil.generate(gen -> toJson(sqlViewRepresentation, gen), false);
  }

  static void toJson(SQLViewRepresentation view, JsonGenerator generator) throws IOException {
    Preconditions.checkArgument(view != null, "Invalid SQL view representation: null");
    generator.writeStartObject();
    generator.writeStringField(ViewRepresentationParser.TYPE, view.type());
    generator.writeStringField(SQL, view.sql());
    generator.writeStringField(DIALECT, view.dialect());

    if (view.defaultCatalog() != null) {
      generator.writeStringField(DEFAULT_CATALOG, view.defaultCatalog());
    }

    if (view.defaultNamespace() != null) {
      JsonUtil.writeStringArray(
          DEFAULT_NAMESPACE, Arrays.asList(view.defaultNamespace().levels()), generator);
    }

    if (!view.fieldAliases().isEmpty()) {
      JsonUtil.writeStringArray(FIELD_ALIASES, view.fieldAliases(), generator);
    }

    if (!view.fieldComments().isEmpty()) {
      JsonUtil.writeStringArray(FIELD_COMMENTS, view.fieldComments(), generator);
    }

    generator.writeEndObject();
  }

  static SQLViewRepresentation fromJson(String json) {
    return JsonUtil.parse(json, SQLViewRepresentationParser::fromJson);
  }

  static SQLViewRepresentation fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null, "Cannot parse SQL view representation from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse SQL view representation from non-object: %s", node);
    ImmutableSQLViewRepresentation.Builder builder =
        ImmutableSQLViewRepresentation.builder()
            .sql(JsonUtil.getString(SQL, node))
            .dialect(JsonUtil.getString(DIALECT, node));
    String defaultCatalog = JsonUtil.getStringOrNull(DEFAULT_CATALOG, node);
    if (defaultCatalog != null) {
      builder.defaultCatalog(defaultCatalog);
    }

    Integer schemaId = JsonUtil.getIntOrNull(SCHEMA_ID, node);

    List<String> namespace = JsonUtil.getStringListOrNull(DEFAULT_NAMESPACE, node);
    if (namespace != null && !namespace.isEmpty()) {
      builder.defaultNamespace(Namespace.of(Iterables.toArray(namespace, String.class)));
    }

    List<String> fieldAliases = JsonUtil.getStringListOrNull(FIELD_ALIASES, node);
    if (fieldAliases != null) {
      builder.fieldAliases(fieldAliases);
    }

    List<String> fieldComments = JsonUtil.getStringListOrNull(FIELD_COMMENTS, node);
    if (fieldComments != null) {
      builder.fieldComments(fieldComments);
    }

    return builder.build();
  }
}
