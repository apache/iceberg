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

class SQLUdfRepresentationParser {
  private static final String SQL = "sql";
  private static final String DIALECT = "dialect";

  private SQLUdfRepresentationParser() {}

  static String toJson(SQLUdfRepresentation representation) {
    return JsonUtil.generate(gen -> toJson(representation, gen), false);
  }

  static void toJson(SQLUdfRepresentation representation, JsonGenerator generator)
      throws IOException {
    Preconditions.checkArgument(representation != null, "Invalid SQL UDF representation: null");
    generator.writeStartObject();
    generator.writeStringField(UdfRepresentationParser.TYPE, representation.type());
    generator.writeStringField(SQL, representation.sql());
    generator.writeStringField(DIALECT, representation.dialect());
    generator.writeEndObject();
  }

  static SQLUdfRepresentation fromJson(String json) {
    return JsonUtil.parse(json, SQLUdfRepresentationParser::fromJson);
  }

  static SQLUdfRepresentation fromJson(JsonNode node) {
    Preconditions.checkArgument(
        node != null, "Cannot parse SQL UDF representation from null object");
    Preconditions.checkArgument(
        node.isObject(), "Cannot parse SQL UDF representation from non-object: %s", node);
    return ImmutableSQLUdfRepresentation.builder()
        .sql(JsonUtil.getString(SQL, node))
        .dialect(JsonUtil.getString(DIALECT, node))
        .build();
  }
}
