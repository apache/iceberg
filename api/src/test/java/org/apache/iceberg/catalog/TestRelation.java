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
import static org.mockito.Mockito.mock;

import org.apache.iceberg.Table;
import org.apache.iceberg.view.View;
import org.junit.jupiter.api.Test;

class TestRelation {

  @Test
  void forTable() {
    Table table = mock(Table.class);
    Relation relation = Relation.forTable(table);

    assertThat(relation.objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(relation.table()).isSameAs(table);
    assertThat(relation.view()).isNull();
  }

  @Test
  void forView() {
    View view = mock(View.class);
    Relation relation = Relation.forView(view);

    assertThat(relation.objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(relation.view()).isSameAs(view);
    assertThat(relation.table()).isNull();
  }

  @Test
  void forTableNullThrows() {
    assertThatThrownBy(() -> Relation.forTable(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null");
  }

  @Test
  void forViewNullThrows() {
    assertThatThrownBy(() -> Relation.forView(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null");
  }
}
