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

public class TestTableIdentifier {

  @Test
  public void testTableIdentifierParsing() {
    TableIdentifier oneLevelIdentifier = TableIdentifier.parse("tbl");
    assertThat(oneLevelIdentifier.hasNamespace()).isFalse();
    assertThat(oneLevelIdentifier.name()).isEqualTo("tbl");

    TableIdentifier twoLevelIdentifier = TableIdentifier.parse("userdb.tbl");
    assertThat(twoLevelIdentifier.namespace().levels()).hasSize(1);
    assertThat(twoLevelIdentifier.namespace().levels()[0]).isEqualTo("userdb");
    assertThat(twoLevelIdentifier.name()).isEqualTo("tbl");

    TableIdentifier threeLevelIdentifier = TableIdentifier.parse("catalog.userdb.tbl");
    assertThat(threeLevelIdentifier.namespace().levels()).hasSize(2);
    assertThat(threeLevelIdentifier.namespace().levels()[0]).isEqualTo("catalog");
    assertThat(threeLevelIdentifier.namespace().levels()[1]).isEqualTo("userdb");
    assertThat(threeLevelIdentifier.name()).isEqualTo("tbl");
  }

  @Test
  public void testToLowerCase() {
    assertThat(TableIdentifier.of("tbl")).isEqualTo(TableIdentifier.of("Tbl").toLowerCase());
    assertThat(TableIdentifier.of("db", "tbl"))
        .isEqualTo(TableIdentifier.of("dB", "TBL").toLowerCase());
    assertThat(TableIdentifier.of("catalog", "db", "tbl"))
        .isEqualTo(TableIdentifier.of("Catalog", "dB", "TBL").toLowerCase());
  }

  @Test
  public void testInvalidTableName() {
    assertThatThrownBy(() -> TableIdentifier.of(Namespace.empty(), ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: null or empty");

    assertThatThrownBy(() -> TableIdentifier.of(Namespace.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: null or empty");
  }

  @Test
  public void testNulls() {
    assertThatThrownBy(() -> TableIdentifier.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create table identifier from null array");

    assertThatThrownBy(() -> TableIdentifier.parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse table identifier: null");

    assertThatThrownBy(() -> TableIdentifier.of(null, "name"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid Namespace: null");
  }
}
