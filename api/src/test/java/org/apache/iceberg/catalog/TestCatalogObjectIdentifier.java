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
  public void withNullArray() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create catalog object identifier from null array");
  }

  @Test
  public void withEmptyLevels() {
    CatalogObjectIdentifier identifier = CatalogObjectIdentifier.of();
    assertThat(identifier.levels()).isEmpty();
    assertThat(identifier.length()).isZero();
    assertThat(identifier).hasToString("");
  }

  @Test
  public void levelAccessors() {
    String[] levels = {"a", "b", "c", "d"};
    CatalogObjectIdentifier identifier = CatalogObjectIdentifier.of(levels);
    assertThat(identifier).isNotNull();
    assertThat(identifier.levels()).hasSize(4);
    assertThat(identifier.length()).isEqualTo(4);
    assertThat(identifier).hasToString("a.b.c.d");
    for (int i = 0; i < levels.length; i++) {
      assertThat(identifier.level(i)).isEqualTo(levels[i]);
    }
  }

  @Test
  public void levelOutOfBounds() {
    CatalogObjectIdentifier identifier = CatalogObjectIdentifier.of("a", "b");
    assertThatThrownBy(() -> identifier.level(2))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("Index 2 out of bounds");
    assertThatThrownBy(() -> identifier.level(-1))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessageContaining("Index -1 out of bounds");
  }

  @Test
  public void equalsAndHashCode() {
    CatalogObjectIdentifier first = CatalogObjectIdentifier.of("accounting", "tax", "paid");
    CatalogObjectIdentifier second = CatalogObjectIdentifier.of("accounting", "tax", "paid");
    CatalogObjectIdentifier different = CatalogObjectIdentifier.of("accounting", "tax");

    assertThat(first).isEqualTo(second).hasSameHashCodeAs(second).isNotEqualTo(different);
  }

  @Test
  public void notEqualToNullOrOtherType() {
    CatalogObjectIdentifier identifier = CatalogObjectIdentifier.of("accounting", "tax", "paid");
    assertThat(identifier).isNotEqualTo(null);
    assertThat(identifier).isNotEqualTo("accounting.tax.paid");
    assertThat(identifier).isNotEqualTo(Namespace.of("accounting", "tax", "paid"));
  }

  @Test
  public void withNullLevel() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of("a", null, "b"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create a catalog object identifier with a null level");
  }

  @Test
  public void disallowsNullByteInLevel() {
    assertThatThrownBy(() -> CatalogObjectIdentifier.of("ac", "\u0000c", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a catalog object identifier with the null-byte character");

    assertThatThrownBy(() -> CatalogObjectIdentifier.of("ac", "c\0", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a catalog object identifier with the null-byte character");
  }
}
