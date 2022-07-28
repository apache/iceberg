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

import org.apache.iceberg.AssertHelpers;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestNamespace {

  @Test
  public void testWithNullAndEmpty() {
    Assertions.assertThatThrownBy(() -> Namespace.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create Namespace from null array");

    Assertions.assertThat(Namespace.of()).isEqualTo(Namespace.empty());
  }

  @Test
  public void testNamespace() {
    String[] levels = {"a", "b", "c", "d"};
    Namespace namespace = Namespace.of(levels);
    Assertions.assertThat(namespace).isNotNull();
    Assertions.assertThat(namespace.levels()).isNotNull().hasSize(4);
    Assertions.assertThat(namespace.toString()).isEqualTo("a.b.c.d");
    for (int i = 0; i < levels.length; i++) {
      Assertions.assertThat(namespace.level(i)).isEqualTo(levels[i]);
    }
  }

  @Test
  public void testWithNullInLevel() {
    AssertHelpers.assertThrows(
        "An individual level of a namespace cannot be null",
        NullPointerException.class,
        "Cannot create a namespace with a null level",
        () -> Namespace.of("a", null, "b"));
  }

  @Test
  public void testDisallowsNamespaceWithNullByte() {
    AssertHelpers.assertThrows(
        "An individual level of a namespace cannot contain the standard null-byte character",
        IllegalArgumentException.class,
        "Cannot create a namespace with the null-byte character",
        () -> Namespace.of("ac", "\u0000c", "b"));

    AssertHelpers.assertThrows(
        "An individual level of a namespace cannot contain the original ASCII null-byte character",
        IllegalArgumentException.class,
        "Cannot create a namespace with the null-byte character",
        () -> Namespace.of("ac", "c\0", "b"));
  }
}
