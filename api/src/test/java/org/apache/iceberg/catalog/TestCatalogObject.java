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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCatalogObject {

  @Test
  public void testWithNullAndEmpty() {
    assertThatThrownBy(() -> CatalogObject.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create CatalogObject from null array");

    assertThat(CatalogObject.of()).isEqualTo(CatalogObject.empty());
  }

  @Test
  public void testCatalogObject() {
    String[] levels = {"a", "b", "c", "d"};
    CatalogObject catalogObject = CatalogObject.of(levels);
    assertThat(catalogObject).isNotNull();
    assertThat(catalogObject.levels()).hasSize(4);
    assertThat(catalogObject).hasToString("a.b.c.d");
    for (int i = 0; i < levels.length; i++) {
      assertThat(catalogObject.level(i)).isEqualTo(levels[i]);
    }
  }

  @Test
  public void testWithNullInLevel() {
    assertThatThrownBy(() -> CatalogObject.of("a", null, "b"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create a CatalogObject with a null level");
  }

  @Test
  public void testDisallowsCatalogObjectWithNullByte() {
    assertThatThrownBy(() -> CatalogObject.of("ac", "\u0000c", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a CatalogObject with the null-byte character");

    assertThatThrownBy(() -> CatalogObject.of("ac", "c\0", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a CatalogObject with the null-byte character");
  }
}
