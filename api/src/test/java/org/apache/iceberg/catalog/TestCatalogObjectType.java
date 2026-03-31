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

class TestCatalogObjectType {

  @Test
  void tableValue() {
    assertThat(CatalogObjectType.TABLE.value()).isEqualTo("table");
    assertThat(CatalogObjectType.TABLE.toString()).isEqualTo("table");
  }

  @Test
  void viewValue() {
    assertThat(CatalogObjectType.VIEW.value()).isEqualTo("view");
    assertThat(CatalogObjectType.VIEW.toString()).isEqualTo("view");
  }

  @Test
  void fromStringTable() {
    assertThat(CatalogObjectType.fromString("table")).isEqualTo(CatalogObjectType.TABLE);
    assertThat(CatalogObjectType.fromString("TABLE")).isEqualTo(CatalogObjectType.TABLE);
    assertThat(CatalogObjectType.fromString("Table")).isEqualTo(CatalogObjectType.TABLE);
  }

  @Test
  void fromStringView() {
    assertThat(CatalogObjectType.fromString("view")).isEqualTo(CatalogObjectType.VIEW);
    assertThat(CatalogObjectType.fromString("VIEW")).isEqualTo(CatalogObjectType.VIEW);
    assertThat(CatalogObjectType.fromString("View")).isEqualTo(CatalogObjectType.VIEW);
  }

  @Test
  void fromStringNull() {
    assertThatThrownBy(() -> CatalogObjectType.fromString(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null");
  }

  @Test
  void fromStringInvalid() {
    assertThatThrownBy(() -> CatalogObjectType.fromString("invalid"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("invalid");
  }
}
