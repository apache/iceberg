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
package org.apache.iceberg.catalog;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableIdentifierParser {

  @Test
  public void testTableIdentifierToJson() {
    String json = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("accounting", "tax"), "paid");
    Assertions.assertThat(TableIdentifierParser.toJson(identifier))
        .as("Should be able to serialize a table identifier with both namespace and name")
        .isEqualTo(json);

    TableIdentifier identifierWithEmptyNamespace = TableIdentifier.of(Namespace.empty(), "paid");
    String jsonWithEmptyNamespace = "{\"namespace\":[],\"name\":\"paid\"}";
    Assertions.assertThat(TableIdentifierParser.toJson(identifierWithEmptyNamespace))
        .as("Should be able to serialize a table identifier that uses the empty namespace")
        .isEqualTo(jsonWithEmptyNamespace);
  }

  @Test
  public void testTableIdentifierFromJson() {
    String json = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("accounting", "tax"), "paid");
    Assertions.assertThat(TableIdentifierParser.fromJson(json))
        .as("Should be able to deserialize a valid table identifier")
        .isEqualTo(identifier);

    TableIdentifier identifierWithEmptyNamespace = TableIdentifier.of(Namespace.empty(), "paid");
    String jsonWithEmptyNamespace = "{\"namespace\":[],\"name\":\"paid\"}";
    Assertions.assertThat(TableIdentifierParser.fromJson(jsonWithEmptyNamespace))
        .as("Should be able to deserialize a valid multi-level table identifier")
        .isEqualTo(identifierWithEmptyNamespace);

    String identifierMissingNamespace = "{\"name\":\"paid\"}";
    Assertions.assertThat(TableIdentifierParser.fromJson(identifierMissingNamespace))
        .as(
            "Should implicitly convert a missing namespace into the the empty namespace when parsing")
        .isEqualTo(identifierWithEmptyNamespace);
  }

  @Test
  public void testFailParsingWhenNullOrEmptyJson() {
    String nullJson = null;
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(nullJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse table identifier from invalid JSON: null");

    String emptyString = "";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(emptyString))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse table identifier from invalid JSON: ''");

    String emptyJson = "{}";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(emptyJson))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");

    String emptyJsonArray = "[]";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(emptyJsonArray))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing or non-object table identifier: []");
  }

  @Test
  public void testFailParsingWhenMissingRequiredFields() {
    String identifierMissingName = "{\"namespace\":[\"accounting\",\"tax\"]}";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(identifierMissingName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse missing string: name");
  }

  @Test
  public void testFailWhenFieldsHaveInvalidValues() {
    String invalidNamespace = "{\"namespace\":\"accounting.tax\",\"name\":\"paid\"}";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(invalidNamespace))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse from non-array value: namespace: \"accounting.tax\"");

    String invalidName = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":1234}";
    Assertions.assertThatThrownBy(() -> TableIdentifierParser.fromJson(invalidName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse to a string value: name: 1234");
  }
}
