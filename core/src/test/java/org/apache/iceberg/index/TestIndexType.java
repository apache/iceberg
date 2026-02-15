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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TestIndexType {

  @Test
  public void testFromStringBtree() {
    assertThat(IndexType.fromString("btree")).isEqualTo(IndexType.BTREE);
    assertThat(IndexType.fromString("BTREE")).isEqualTo(IndexType.BTREE);
    assertThat(IndexType.fromString("Btree")).isEqualTo(IndexType.BTREE);
  }

  @Test
  public void testFromStringTerm() {
    assertThat(IndexType.fromString("term")).isEqualTo(IndexType.TERM);
    assertThat(IndexType.fromString("TERM")).isEqualTo(IndexType.TERM);
    assertThat(IndexType.fromString("Term")).isEqualTo(IndexType.TERM);
  }

  @Test
  public void testFromStringIvf() {
    assertThat(IndexType.fromString("ivf")).isEqualTo(IndexType.IVF);
    assertThat(IndexType.fromString("IVF")).isEqualTo(IndexType.IVF);
    assertThat(IndexType.fromString("Ivf")).isEqualTo(IndexType.IVF);
  }

  @Test
  public void testFromStringUnknownType() {
    assertThatThrownBy(() -> IndexType.fromString("unknown"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown index type: unknown");

    assertThatThrownBy(() -> IndexType.fromString(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unknown index type:");

    assertThatThrownBy(() -> IndexType.fromString("hash"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown index type: hash");
  }

  @Test
  public void testTypeName() {
    assertThat(IndexType.BTREE.typeName()).isEqualTo("btree");
    assertThat(IndexType.TERM.typeName()).isEqualTo("term");
    assertThat(IndexType.IVF.typeName()).isEqualTo("ivf");
  }

  @Test
  public void testToString() {
    assertThat(IndexType.BTREE.toString()).isEqualTo("btree");
    assertThat(IndexType.TERM.toString()).isEqualTo("term");
    assertThat(IndexType.IVF.toString()).isEqualTo("ivf");
  }

  @Test
  public void testAllTypesHaveConsistentNameAndToString() {
    for (IndexType type : IndexType.values()) {
      assertThat(type.typeName()).isEqualTo(type.toString());
      assertThat(IndexType.fromString(type.typeName())).isEqualTo(type);
    }
  }
}
