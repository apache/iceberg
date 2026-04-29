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

public class TestCatalogObjectType {

  @Test
  public void testType() {
    assertThat(CatalogObjectType.TABLE.type()).isEqualTo("table");
    assertThat(CatalogObjectType.VIEW.type()).isEqualTo("view");
    assertThat(CatalogObjectType.NAMESPACE.type()).isEqualTo("namespace");
  }

  @Test
  public void testFromName() {
    assertThat(CatalogObjectType.fromName("table")).isEqualTo(CatalogObjectType.TABLE);
    assertThat(CatalogObjectType.fromName("view")).isEqualTo(CatalogObjectType.VIEW);
    assertThat(CatalogObjectType.fromName("namespace")).isEqualTo(CatalogObjectType.NAMESPACE);
    assertThat(CatalogObjectType.fromName("TABLE")).isEqualTo(CatalogObjectType.TABLE);
  }

  @Test
  public void testFromNameInvalid() {
    assertThatThrownBy(() -> CatalogObjectType.fromName(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Catalog object type is null");

    assertThatThrownBy(() -> CatalogObjectType.fromName("unknown"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid catalog object type: unknown");
  }
}
