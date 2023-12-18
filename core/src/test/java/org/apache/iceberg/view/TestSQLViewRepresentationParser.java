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

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSQLViewRepresentationParser {
  @Test
  public void testParseSqlViewRepresentation() {
    String requiredFields =
        "{\"type\":\"sql\", \"sql\": \"select * from foo\", \"dialect\": \"spark-sql\"}";
    SQLViewRepresentation viewRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from foo")
            .dialect("spark-sql")
            .build();

    Assertions.assertThat(SQLViewRepresentationParser.fromJson(requiredFields))
        .as("Should be able to parse valid SQL view representation")
        .isEqualTo(viewRepresentation);

    String requiredAndOptionalFields =
        "{\"type\":\"sql\", \"sql\": \"select * from foo\", \"dialect\": \"spark-sql\"}";

    SQLViewRepresentation viewWithOptionalFields =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from foo")
            .dialect("spark-sql")
            .build();
    Assertions.assertThat(SQLViewRepresentationParser.fromJson(requiredAndOptionalFields))
        .as("Should be able to parse valid SQL view representation")
        .isEqualTo(viewWithOptionalFields);
  }

  @Test
  public void testParseSqlViewRepresentationMissingRequiredFields() {
    String missingDialect = "{\"type\":\"sql\", \"sql\": \"select * from foo\"}";
    Assertions.assertThatThrownBy(() -> ViewRepresentationParser.fromJson(missingDialect))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: dialect");

    String missingType = "{\"sql\":\"select * from foo\",\"dialect\":\"spark-sql\"}";
    Assertions.assertThatThrownBy(() -> ViewRepresentationParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: type");
  }

  @Test
  public void testViewRepresentationSerialization() {
    String json = "{\"type\":\"sql\",\"sql\":\"select * from foo\",\"dialect\":\"spark-sql\"}";
    SQLViewRepresentation viewRepresentation =
        ImmutableSQLViewRepresentation.builder()
            .sql("select * from foo")
            .dialect("spark-sql")
            .build();
    Assertions.assertThat(ViewRepresentationParser.toJson(viewRepresentation))
        .as("Should be able to serialize valid SQL view representation")
        .isEqualTo(json);
    Assertions.assertThat(
            ViewRepresentationParser.fromJson(ViewRepresentationParser.toJson(viewRepresentation)))
        .isEqualTo(viewRepresentation);
  }

  @Test
  public void testNullSqlViewRepresentation() {
    Assertions.assertThatThrownBy(() -> SQLViewRepresentationParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid SQL view representation: null");

    Assertions.assertThatThrownBy(() -> SQLViewRepresentationParser.fromJson((JsonNode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse SQL view representation from null object");
  }
}
