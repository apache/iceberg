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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

class TestSQLUdfRepresentationParser {

  @Test
  void parseSqlUdfRepresentation() {
    String json = "{\"type\":\"sql\", \"sql\": \"x + 1\", \"dialect\": \"spark\"}";
    SQLUdfRepresentation representation =
        ImmutableSQLUdfRepresentation.builder().sql("x + 1").dialect("spark").build();

    assertThat(SQLUdfRepresentationParser.fromJson(json))
        .as("Should be able to parse valid SQL UDF representation")
        .isEqualTo(representation);
  }

  @Test
  void parseMissingRequiredFields() {
    String missingDialect = "{\"type\":\"sql\", \"sql\": \"x + 1\"}";
    assertThatThrownBy(() -> UdfRepresentationParser.fromJson(missingDialect))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: dialect");

    String missingSql = "{\"type\":\"sql\", \"dialect\": \"spark\"}";
    assertThatThrownBy(() -> UdfRepresentationParser.fromJson(missingSql))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: sql");

    String missingType = "{\"sql\":\"x + 1\",\"dialect\":\"spark\"}";
    assertThatThrownBy(() -> UdfRepresentationParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");
  }

  @Test
  void roundTripSerialization() {
    String expectedJson = "{\"type\":\"sql\",\"sql\":\"x + 1\",\"dialect\":\"spark\"}";
    SQLUdfRepresentation representation =
        ImmutableSQLUdfRepresentation.builder().sql("x + 1").dialect("spark").build();

    assertThat(UdfRepresentationParser.toJson(representation))
        .as("Should be able to serialize valid SQL UDF representation")
        .isEqualTo(expectedJson);

    assertThat(UdfRepresentationParser.fromJson(UdfRepresentationParser.toJson(representation)))
        .isEqualTo(representation);
  }

  @Test
  void roundTripWithTrinoDialect() {
    SQLUdfRepresentation representation =
        ImmutableSQLUdfRepresentation.builder().sql("x + 1.0").dialect("trino").build();

    String serialized = UdfRepresentationParser.toJson(representation);
    UdfRepresentation deserialized = UdfRepresentationParser.fromJson(serialized);

    assertThat(deserialized).isInstanceOf(SQLUdfRepresentation.class);
    SQLUdfRepresentation sqlRepr = (SQLUdfRepresentation) deserialized;
    assertThat(sqlRepr.sql()).isEqualTo("x + 1.0");
    assertThat(sqlRepr.dialect()).isEqualTo("trino");
    assertThat(sqlRepr.type()).isEqualTo("sql");
  }

  @Test
  void nullSqlUdfRepresentation() {
    assertThatThrownBy(() -> SQLUdfRepresentationParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid SQL UDF representation: null");

    assertThatThrownBy(() -> SQLUdfRepresentationParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse SQL UDF representation from null object");
  }
}
