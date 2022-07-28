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

import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

public class TestTableIdentifierParser {

  @Test
  public void testTableIdentifierToJson() {
    String json = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("accounting", "tax"), "paid");
    Assert.assertEquals(
        "Should be able to serialize a table identifier with both namespace and name",
        json,
        TableIdentifierParser.toJson(identifier));

    TableIdentifier identifierWithEmptyNamespace = TableIdentifier.of(Namespace.empty(), "paid");
    String jsonWithEmptyNamespace = "{\"namespace\":[],\"name\":\"paid\"}";
    Assert.assertEquals(
        "Should be able to serialize a table identifier that uses the empty namespace",
        jsonWithEmptyNamespace,
        TableIdentifierParser.toJson(identifierWithEmptyNamespace));
  }

  @Test
  public void testTableIdentifierFromJson() {
    String json = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":\"paid\"}";
    TableIdentifier identifier = TableIdentifier.of(Namespace.of("accounting", "tax"), "paid");
    Assert.assertEquals(
        "Should be able to deserialize a valid table identifier",
        identifier,
        TableIdentifierParser.fromJson(json));

    TableIdentifier identifierWithEmptyNamespace = TableIdentifier.of(Namespace.empty(), "paid");
    String jsonWithEmptyNamespace = "{\"namespace\":[],\"name\":\"paid\"}";
    Assert.assertEquals(
        "Should be able to deserialize a valid multi-level table identifier",
        identifierWithEmptyNamespace,
        TableIdentifierParser.fromJson(jsonWithEmptyNamespace));

    String identifierMissingNamespace = "{\"name\":\"paid\"}";
    Assert.assertEquals(
        "Should implicitly convert a missing namespace into the the empty namespace when parsing",
        identifierWithEmptyNamespace,
        TableIdentifierParser.fromJson(identifierMissingNamespace));
  }

  @Test
  public void testFailParsingWhenNullOrEmptyJson() {
    String nullJson = null;
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize null JSON string",
        IllegalArgumentException.class,
        "Cannot parse table identifier from invalid JSON: null",
        () -> TableIdentifierParser.fromJson(nullJson));

    String emptyString = "";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize an empty string",
        IllegalArgumentException.class,
        "Cannot parse table identifier from invalid JSON: ''",
        () -> TableIdentifierParser.fromJson(emptyString));

    String emptyJson = "{}";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize an empty JSON string",
        IllegalArgumentException.class,
        "Cannot parse missing string name",
        () -> TableIdentifierParser.fromJson(emptyJson));

    String emptyJsonArray = "[]";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize an empty JSON array",
        IllegalArgumentException.class,
        "Cannot parse missing or non-object table identifier: []",
        () -> TableIdentifierParser.fromJson(emptyJsonArray));
  }

  @Test
  public void testFailParsingWhenMissingRequiredFields() {
    String identifierMissingName = "{\"namespace\":[\"accounting\",\"tax\"]}";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize table with missing name",
        IllegalArgumentException.class,
        "Cannot parse missing string name",
        () -> TableIdentifierParser.fromJson(identifierMissingName));
  }

  @Test
  public void testFailWhenFieldsHaveInvalidValues() {
    String invalidNamespace = "{\"namespace\":\"accounting.tax\",\"name\":\"paid\"}";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize table with invalid namespace",
        IllegalArgumentException.class,
        "Cannot parse namespace from non-array value: \"accounting.tax\"",
        () -> TableIdentifierParser.fromJson(invalidNamespace));

    String invalidName = "{\"namespace\":[\"accounting\",\"tax\"],\"name\":1234}";
    AssertHelpers.assertThrows(
        "TableIdentifierParser should fail to deserialize table with invalid name",
        IllegalArgumentException.class,
        "Cannot parse name to a string value: 1234",
        () -> TableIdentifierParser.fromJson(invalidName));
  }
}
