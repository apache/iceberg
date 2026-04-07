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

public class TestCatalogObjectIdentifier {

  @Test
  public void testWithNullAndEmpty() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create CatalogObjectIdentifier from null array");

    assertThat(CatalogObjectIdentifier.of()).isEqualTo(CatalogObjectIdentifier.empty());
  }

  @Test
  public void testCatalogObjectIdentifier() {
    String[] levels = {"a", "b", "c", "d"};
    CatalogObjectIdentifier catalogObjectIdentifier = CatalogObjectIdentifier.of(levels);
    assertThat(catalogObjectIdentifier).isNotNull();
    assertThat(catalogObjectIdentifier.levels()).hasSize(4);
    assertThat(catalogObjectIdentifier).hasToString("a.b.c.d");
    for (int i = 0; i < levels.length; i++) {
      assertThat(catalogObjectIdentifier.level(i)).isEqualTo(levels[i]);
    }
  }

  @Test
  public void testWithNullInLevel() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of("a", null, "b"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create a CatalogObjectIdentifier with a null level");
  }

  @Test
  public void testDisallowsCatalogObjectIdentifierWithNullByte() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of("ac", "\u0000c", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a CatalogObjectIdentifier with the null-byte character");

    assertThatThrownBy(() -> CatalogObjectIdentifier.of("ac", "c\0", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a CatalogObjectIdentifier with the null-byte character");
  }
}
