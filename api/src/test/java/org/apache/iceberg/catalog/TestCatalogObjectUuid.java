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

public class TestCatalogObjectUuid {

  @Test
  void testInvalidUuid() {
    assertThatThrownBy(() -> new CatalogObjectUuid(null, CatalogObjectType.TABLE))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid UUID: null");

    assertThatThrownBy(() -> new CatalogObjectUuid("", CatalogObjectType.TABLE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid UUID: empty");
  }

  @Test
  void testInvalidObjectType() {
    assertThatThrownBy(() -> new CatalogObjectUuid("valid-uuid", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid objectType: null");
  }

  @Test
  void testValidUuidAndObjectType() {
    String validUuid = "123e4567-e89b-12d3-a456-426614174000";
    CatalogObjectType type = CatalogObjectType.TABLE;

    CatalogObjectUuid catalogObjectUuid = new CatalogObjectUuid(validUuid, type);

    assertThat(catalogObjectUuid.uuid()).isEqualTo(validUuid);
    assertThat(catalogObjectUuid.type()).isEqualTo(type);
  }
}
