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

public class TestNamespace {

  @Test
  public void testWithNullAndEmpty() {
    assertThatThrownBy(() -> Namespace.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create Namespace from null array");

    assertThat(Namespace.of()).isEqualTo(Namespace.empty());
  }

  @Test
  public void testNamespace() {
    String[] levels = {"a", "b", "c", "d"};
    Namespace namespace = Namespace.of(levels);
    assertThat(namespace).isNotNull();
    assertThat(namespace.levels()).hasSize(4);
    assertThat(namespace).hasToString("a.b.c.d");
    for (int i = 0; i < levels.length; i++) {
      assertThat(namespace.level(i)).isEqualTo(levels[i]);
    }
  }

  @Test
  public void testWithNullInLevel() {
    assertThatThrownBy(() -> Namespace.of("a", null, "b"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Cannot create a namespace with a null level");
  }

  @Test
  public void testDisallowsNamespaceWithNullByte() {
    assertThatThrownBy(() -> Namespace.of("ac", "\u0000c", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a namespace with the null-byte character");

    assertThatThrownBy(() -> Namespace.of("ac", "c\0", "b"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create a namespace with the null-byte character");
  }
}
