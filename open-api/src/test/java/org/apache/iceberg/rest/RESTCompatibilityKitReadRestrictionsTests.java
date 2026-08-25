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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SupportsReadRestrictions;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.restrictions.ReadRestrictions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class RESTCompatibilityKitReadRestrictionsTests {

  private static final Namespace RESTRICTIONS_NS = Namespace.of("restrictions_ns");
  private static final TableIdentifier TABLE = TableIdentifier.of(RESTRICTIONS_NS, "pii_table");

  private static final int PII_FIELD_ID = 2;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(PII_FIELD_ID, "pii", Types.StringType.get()),
          Types.NestedField.optional(3, "country", Types.StringType.get()));

  @RegisterExtension
  static RESTServerExtension server =
      new RESTServerExtension(
          ImmutableMap.of(
              RESTCatalogServer.REST_PORT,
              RESTServerExtension.FREE_PORT,
              RESTServerCatalogAdapter.READ_RESTRICTIONS_NAMESPACE,
              "restrictions_ns",
              RESTServerCatalogAdapter.READ_RESTRICTIONS_FIELD_ID,
              String.valueOf(PII_FIELD_ID)));

  private static RESTCatalog restCatalog;

  @BeforeAll
  static void beforeAll() {
    restCatalog = RCKUtils.initCatalogClient(server.config());

    assumeThat(
            PropertyUtil.propertyAsBoolean(
                restCatalog.properties(),
                RESTCompatibilityKitSuite.RCK_SUPPORTS_READ_RESTRICTIONS,
                true))
        .as("Read restrictions must be supported")
        .isTrue();

    if (!restCatalog.namespaceExists(RESTRICTIONS_NS)) {
      restCatalog.createNamespace(RESTRICTIONS_NS);
    }

    restCatalog.buildTable(TABLE, SCHEMA).create();
  }

  @AfterAll
  static void afterAll() throws IOException {
    if (restCatalog != null) {
      try {
        restCatalog.dropTable(TABLE, true);
        restCatalog.dropNamespace(RESTRICTIONS_NS);
      } finally {
        restCatalog.close();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMaskToFixedValueRestriction() {
    Table table = restCatalog.loadTable(TABLE);

    assertThat(table).isInstanceOf(SupportsReadRestrictions.class);

    ReadRestrictions restrictions = TableUtil.readRestrictions(table).orElseThrow();
    assertThat(restrictions.isEmpty()).isFalse();
    assertThat(restrictions.rowFilter()).isNull();
    assertThat(restrictions.maskedFieldIds()).containsExactly(PII_FIELD_ID);
    assertThat(restrictions.columnProjections()).hasSize(1);
    assertThat(restrictions.columnProjections().get(0).name()).isEqualTo("mask-to-fixed-value");
    assertThat(restrictions.columnProjections().get(0).fieldId()).isEqualTo(PII_FIELD_ID);

    SerializableFunction<Object, Object> fn =
        (SerializableFunction<Object, Object>)
            restrictions.columnProjections().get(0).bind(Types.StringType.get());
    assertThat(fn.apply("sensitive-data")).isEqualTo("XXXXXXXX");
  }

  @Test
  public void testNoRestrictionsOutsideConfiguredNamespace() {
    Namespace unrestricted = Namespace.of("unrestricted_ns");
    TableIdentifier unrestrictedTable = TableIdentifier.of(unrestricted, "plain_table");

    try {
      if (!restCatalog.namespaceExists(unrestricted)) {
        restCatalog.createNamespace(unrestricted);
      }
      restCatalog.buildTable(unrestrictedTable, SCHEMA).create();

      Table table = restCatalog.loadTable(unrestrictedTable);
      assertThat(TableUtil.readRestrictions(table))
          .satisfiesAnyOf(
              r -> assertThat(r).isEmpty(), r -> assertThat(r.get().isEmpty()).isTrue());
    } finally {
      restCatalog.dropTable(unrestrictedTable, true);
      restCatalog.dropNamespace(unrestricted);
    }
  }
}
