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

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCatalogObjectUuidParser {
  @Test
  void testToJson() {
    CatalogObjectUuid catalogObjectUuid = new CatalogObjectUuid("uuid", CatalogObjectType.NAMESPACE);
    String catalogObjectUuidJson = "{\"uuid\":\"uuid\",\"type\":\"namespace\"}";
    assertThat(CatalogObjectUuidParser.toJson(catalogObjectUuid)).isEqualTo(catalogObjectUuidJson);
  }

  @Test
  void testToJsonPretty() {
    CatalogObjectUuid catalogObjectUuid = new CatalogObjectUuid("uuid", CatalogObjectType.NAMESPACE);
    String catalogObjectUuidJson = "{\n" +
        "  \"uuid\" : \"uuid\",\n" +
        "  \"type\" : \"namespace\"\n" +
        "}";
    assertThat(CatalogObjectUuidParser.toJsonPretty(catalogObjectUuid)).isEqualTo(catalogObjectUuidJson);
  }

  @Test
  void testToJsonWithNullOperation() {
    assertThatThrownBy(() -> CatalogObjectUuidParser.toJson(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid catalog object uuid: null");
  }

  @Test
  void testFromJson() {
    CatalogObjectUuid catalogObjectUuid = new CatalogObjectUuid("uuid", CatalogObjectType.NAMESPACE);
    String catalogObjectUuidJson = "{\"uuid\":\"uuid\",\"type\":\"namespace\"}";
    assertThat(CatalogObjectUuidParser.fromJson(catalogObjectUuidJson)).isEqualTo(catalogObjectUuid);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson((JsonNode) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot parse catalog object uuid from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingUuid = "{\"type\":\"namespace\"}";
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson(missingUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String missingType = "{\"uuid\":\"uuid\"}";
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson(missingType))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidUuid = "{\"uuid\":123,\"type\":\"namespace\"}";
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson(invalidUuid))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidType = "{\"uuid\":\"uuid\",\"type\":123}";
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson(invalidType))
        .isInstanceOf(IllegalArgumentException.class);

    String invalidCatalogObjectType = "{\"uuid\":\"uuid\",\"type\":\"unknown\"}";
    assertThatThrownBy(() -> CatalogObjectUuidParser.fromJson(invalidCatalogObjectType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid CatalogObjectType: unknown");
  }
}
