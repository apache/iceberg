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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableIdentifier {

  @Test
  public void testTableIdentifierParsing() {
    TableIdentifier oneLevelIdentifier = TableIdentifier.parse("tbl");
    Assertions.assertThat(oneLevelIdentifier.hasNamespace()).isFalse();
    Assertions.assertThat(oneLevelIdentifier.name()).isEqualTo("tbl");

    TableIdentifier twoLevelIdentifier = TableIdentifier.parse("userdb.tbl");
    Assertions.assertThat(twoLevelIdentifier.namespace().levels().length).isEqualTo(1);
    Assertions.assertThat(twoLevelIdentifier.namespace().levels()[0]).isEqualTo("userdb");
    Assertions.assertThat(twoLevelIdentifier.name()).isEqualTo("tbl");

    TableIdentifier threeLevelIdentifier = TableIdentifier.parse("catalog.userdb.tbl");
    Assertions.assertThat(threeLevelIdentifier.namespace().levels().length).isEqualTo(2);
    Assertions.assertThat(threeLevelIdentifier.namespace().levels()[0]).isEqualTo("catalog");
    Assertions.assertThat(threeLevelIdentifier.namespace().levels()[1]).isEqualTo("userdb");
    Assertions.assertThat(threeLevelIdentifier.name()).isEqualTo("tbl");
  }

  @Test
  public void testToLowerCase() {
    Assertions.assertThat(TableIdentifier.of("Tbl").toLowerCase())
        .isEqualTo(TableIdentifier.of("tbl"));
    Assertions.assertThat(TableIdentifier.of("dB", "TBL").toLowerCase())
        .isEqualTo(TableIdentifier.of("db", "tbl"));
    Assertions.assertThat(TableIdentifier.of("Catalog", "dB", "TBL").toLowerCase())
        .isEqualTo(TableIdentifier.of("catalog", "db", "tbl"));
  }

  @Test
  public void testInvalidTableName() {
    Assertions.assertThatThrownBy(() -> TableIdentifier.of(Namespace.empty(), ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: null or empty");

    Assertions.assertThatThrownBy(() -> TableIdentifier.of(Namespace.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid table name: null or empty");
  }

  @Test
  public void testNulls() {
    Assertions.assertThatThrownBy(() -> TableIdentifier.of((String[]) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot create table identifier from null array");

    Assertions.assertThatThrownBy(() -> TableIdentifier.parse(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse table identifier: null");

    Assertions.assertThatThrownBy(() -> TableIdentifier.of(null, "name"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid Namespace: null");
  }
}
