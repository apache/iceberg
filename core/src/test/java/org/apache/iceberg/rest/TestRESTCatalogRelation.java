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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.Relation;
import org.apache.iceberg.catalog.SupportsRelations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestRESTCatalogRelation extends TestBaseWithRESTServer {

  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "x", Types.LongType.get()));
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of(NS, "tbl");
  private static final TableIdentifier VIEW_IDENT = TableIdentifier.of(NS, "vw");
  private static final TableIdentifier MISSING_IDENT = TableIdentifier.of(NS, "missing");

  @Override
  protected String catalogName() {
    return "test";
  }

  @BeforeEach
  void createFixtures() {
    backendCatalog.createNamespace(NS);
    backendCatalog.createTable(TABLE_IDENT, SCHEMA);
    backendCatalog
        .buildView(VIEW_IDENT)
        .withSchema(SCHEMA)
        .withDefaultNamespace(NS)
        .withQuery("spark", "SELECT 1")
        .create();
  }

  @Test
  void loadRelationTable() {
    SupportsRelations relations = (SupportsRelations) restCatalog;
    Relation relation = relations.loadRelation(TABLE_IDENT);

    assertThat(relation.objectType()).isEqualTo(CatalogObjectType.TABLE);
    assertThat(relation.table()).isNotNull();
    assertThat(relation.table().schema().columns()).hasSize(1);
    assertThat(relation.view()).isNull();
  }

  @Test
  void loadRelationView() {
    SupportsRelations relations = (SupportsRelations) restCatalog;
    Relation relation = relations.loadRelation(VIEW_IDENT);

    assertThat(relation.objectType()).isEqualTo(CatalogObjectType.VIEW);
    assertThat(relation.view()).isNotNull();
    assertThat(relation.view().schema().columns()).hasSize(1);
    assertThat(relation.table()).isNull();
  }

  @Test
  void loadRelationNotFound() {
    SupportsRelations relations = (SupportsRelations) restCatalog;
    assertThatThrownBy(() -> relations.loadRelation(MISSING_IDENT))
        .isInstanceOf(NotFoundException.class)
        .hasMessageContaining("missing");
  }

  @Test
  void loadRelationsMultiple() {
    SupportsRelations relations = (SupportsRelations) restCatalog;
    Set<TableIdentifier> identifiers = ImmutableSet.of(TABLE_IDENT, VIEW_IDENT, MISSING_IDENT);

    List<Relation> results = relations.loadRelations(identifiers);

    assertThat(results).hasSize(2);

    Relation tableRelation =
        results.stream()
            .filter(r -> r.objectType() == CatalogObjectType.TABLE)
            .findFirst()
            .orElseThrow();
    assertThat(tableRelation.table()).isNotNull();
    assertThat(tableRelation.table().schema().columns()).hasSize(1);

    Relation viewRelation =
        results.stream()
            .filter(r -> r.objectType() == CatalogObjectType.VIEW)
            .findFirst()
            .orElseThrow();
    assertThat(viewRelation.view()).isNotNull();
    assertThat(viewRelation.view().schema().columns()).hasSize(1);
  }

  @Test
  void loadRelationsEmpty() {
    SupportsRelations relations = (SupportsRelations) restCatalog;
    Set<TableIdentifier> identifiers = ImmutableSet.of(MISSING_IDENT);

    List<Relation> results = relations.loadRelations(identifiers);
    assertThat(results).isEmpty();
  }
}
