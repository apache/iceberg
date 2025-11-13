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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

public class TestCatalogObjectUuidParser {
  @Test
  void testToJson() {
    CatalogObjectUuid catalogObjectUuid =
        new CatalogObjectUuid("uuid", CatalogObjectType.TABLE.type());
    String catalogObjectUuidJson = "{\"uuid\":\"uuid\",\"type\":\"table\"}";
    assertThat(CatalogObjectUuidParser.toJson(catalogObjectUuid)).isEqualTo(catalogObjectUuidJson);
  }

  @Test
  void testToJsonPretty() {
    CatalogObjectUuid catalogObjectUuid =
        new CatalogObjectUuid("uuid", CatalogObjectType.TABLE.type());
    String catalogObjectUuidJson =
        "{\n" + "  \"uuid\" : \"uuid\",\n" + "  \"type\" : \"table\"\n" + "}";
    assertThat(CatalogObjectUuidParser.toJsonPretty(catalogObjectUuid))
        .isEqualTo(catalogObjectUuidJson);
  }

  @Test
  void testToJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogObjectUuidParser.toJson(null))
        .withMessage("Invalid catalog object uuid: null");
  }

  @Test
  void testFromJson() {
    CatalogObjectUuid catalogObjectUuid =
        new CatalogObjectUuid("uuid", CatalogObjectType.TABLE.type());
    String catalogObjectUuidJson = "{\"uuid\":\"uuid\",\"type\":\"table\"}";
    assertThat(CatalogObjectUuidParser.fromJson(catalogObjectUuidJson))
        .isEqualTo(catalogObjectUuid);
  }

  @Test
  void testFromJsonWithNullInput() {
    assertThatNullPointerException()
        .isThrownBy(() -> CatalogObjectUuidParser.fromJson((JsonNode) null))
        .withMessage("Cannot parse catalog object uuid from null object");
  }

  @Test
  void testFromJsonWithMissingProperties() {
    String missingUuid = "{\"type\":\"table\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogObjectUuidParser.fromJson(missingUuid));

    String missingType = "{\"uuid\":\"uuid\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogObjectUuidParser.fromJson(missingType));
  }

  @Test
  void testFromJsonWithInvalidProperties() {
    String invalidUuid = "{\"uuid\":123,\"type\":\"namespace\"}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogObjectUuidParser.fromJson(invalidUuid));

    String invalidType = "{\"uuid\":\"uuid\",\"type\":123}";
    assertThatIllegalArgumentException()
        .isThrownBy(() -> CatalogObjectUuidParser.fromJson(invalidType));

  }
}
