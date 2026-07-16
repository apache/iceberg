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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestCatalogObjectIdentifierParser {

  @Test
  public void toJson() {
    assertThat(
            CatalogObjectIdentifierParser.toJson(
                CatalogObjectIdentifier.of("accounting", "tax", "paid")))
        .isEqualTo("[\"accounting\",\"tax\",\"paid\"]");

    assertThat(CatalogObjectIdentifierParser.toJson(CatalogObjectIdentifier.of("accounting")))
        .isEqualTo("[\"accounting\"]");
  }

  @Test
  public void toJsonRejectsNull() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.toJson(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid catalog object identifier: null");
  }

  @Test
  public void fromJson() {
    assertThat(CatalogObjectIdentifierParser.fromJson("[\"accounting\",\"tax\",\"paid\"]"))
        .isEqualTo(CatalogObjectIdentifier.of("accounting", "tax", "paid"));
  }

  @Test
  public void roundTrip() {
    CatalogObjectIdentifier identifier = CatalogObjectIdentifier.of("accounting", "tax", "paid");
    assertThat(
            CatalogObjectIdentifierParser.fromJson(
                CatalogObjectIdentifierParser.toJson(identifier)))
        .isEqualTo(identifier);
  }

  @Test
  public void fromJsonRejectsInvalidInput() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson((String) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse catalog object identifier from invalid JSON: null");

    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse catalog object identifier from invalid JSON: ''");

    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("{}"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string array from non-array: {}");

    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("\"accounting.tax\""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string array from non-array: \"accounting.tax\"");
  }

  @Test
  public void fromJsonAcceptsEmptyArray() {
    CatalogObjectIdentifier identifier = CatalogObjectIdentifierParser.fromJson("[]");
    assertThat(identifier.levels()).isEmpty();
    assertThat(identifier.length()).isZero();
  }

  @Test
  public void fromJsonRejectsNonStringElement() {
    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("[null]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value: null");

    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("[1]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value: 1");

    assertThatThrownBy(() -> CatalogObjectIdentifierParser.fromJson("[\"a\", null, \"b\"]"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse string from non-text value: null");
  }
}
