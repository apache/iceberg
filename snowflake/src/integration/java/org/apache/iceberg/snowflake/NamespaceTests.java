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
package org.apache.iceberg.snowflake;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class NamespaceTests extends SnowTestBase {

  @Test
  public void testListNamespacesAtRootLevel() throws SQLException, InterruptedException {
    List<Namespace> namespaces = snowflakeCatalog.listNamespaces(Namespace.empty());
    int dbCount =
        clientPool.run(
            conn -> {
              int databaseCount = 0;
              ResultSet rs = conn.createStatement().executeQuery("show databases");
              while (rs.next()) {
                databaseCount++;
              }
              return databaseCount;
            });
    Assertions.assertThat(namespaces.stream().count()).isEqualTo(dbCount);
  }

  @Test
  public void testListNamespacesAtDatabaseLevel() throws SQLException, InterruptedException {
    String schema1 = "Schema_1";
    String schema2 = "Schema_2";
    String dbName = getDatabase().toUpperCase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema1);
      createOrReplaceSchema(schema2);
      List<Namespace> namespaces = snowflakeCatalog.listNamespaces(Namespace.of(dbName));
      Assertions.assertThat(namespaces)
          .containsExactlyInAnyOrderElementsOf(
              Arrays.asList(
                  Namespace.of(dbName, schema1.toUpperCase()),
                  Namespace.of(dbName, schema2.toUpperCase()),
                  Namespace.of(dbName, "PUBLIC"),
                  Namespace.of(dbName, "INFORMATION_SCHEMA")));
    } finally {
      dropSchemaIfExists(schema1);
      dropSchemaIfExists(schema2);
    }
  }

  @Test
  public void testListNamespacesAtSchemaLevelIsNotAllowed()
      throws SQLException, InterruptedException {
    String schema = "Schema_1";
    String dbName = getDatabase().toUpperCase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThatThrownBy(
              () -> snowflakeCatalog.listNamespaces(Namespace.of(dbName, schema)))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("listNamespaces must be at either ROOT or DATABASE level");
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testLoadNonExistingRootLevelNamespace() {
    String nonExistingDb = "IDontExist";
    Assertions.assertThatThrownBy(
            () -> snowflakeCatalog.loadNamespaceMetadata(Namespace.of(nonExistingDb)))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContainingAll("snowflake identifier", "doesn't exist");
  }

  @Test
  public void testLoadNonExistingDBLevelNamespace() {
    String nonExistingSchema = "IDontExist";
    String db = getDatabase();
    Assertions.assertThatThrownBy(
            () -> snowflakeCatalog.loadNamespaceMetadata(Namespace.of(db, nonExistingSchema)))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContainingAll("snowflake identifier", "doesn't exist");
  }

  @Test
  public void testLoadNamespaceThatExceedMaxSupportedHierarchy() {
    String nonExistingSchema = "IDontExist";
    String db = getDatabase();
    Assertions.assertThatThrownBy(
            () ->
                snowflakeCatalog.loadNamespaceMetadata(
                    Namespace.of(db, nonExistingSchema, "UnSupportedLevel")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Snowflake max namespace level is");
  }

  @ParameterizedTest
  @ValueSource(strings = {"Schema", "schema", "_schema", "Schema123", "schema$", "_Schema$_123"})
  public void testLoadNamespaceWithValidUnquotedIdentifier(String schema)
      throws SQLException, InterruptedException {
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThat(snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema)))
          .isEmpty();
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testLoadNamespaceWithUnquotedIdentifierIsCaseInsensitive()
      throws SQLException, InterruptedException {
    String schema = "Schema123";
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThat(snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema)))
          .isEmpty();
      Assertions.assertThat(
              snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema.toUpperCase())))
          .isEmpty();
      Assertions.assertThat(
              snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, "schEmA123")))
          .isEmpty();
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testLoadNamespaceWithQuotedIdentifierIsCaseSensitive()
      throws SQLException, InterruptedException {
    String schema = "\"Schema123\"";
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThatThrownBy(
              () ->
                  snowflakeCatalog.loadNamespaceMetadata(
                      Namespace.of(dbName, schema.toUpperCase())))
          .isInstanceOf(NoSuchNamespaceException.class)
          .hasMessageContainingAll("snowflake identifier", "doesn't exist");
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "\"Schema123\"",
        "\"MySchema\"",
        "\"my.schema\"",
        "\"my schema\"",
        "\"My 'Schema'\"",
        "\"3rd_schema\"",
        "\"$Ischema\"",
        "\"H@!!.0-w*r()^'\"",
        "\"идентификатор\""
      })
  public void testLoadNamespaceWithQuotedIdentifier(String schema)
      throws SQLException, InterruptedException {
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThat(snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema)))
          .isEmpty();
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testDropNamespaceNotSupported() throws SQLException, InterruptedException {
    String schema = "Schema123";
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThatThrownBy(
              () -> snowflakeCatalog.dropNamespace(Namespace.of(dbName, schema)))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("SnowflakeCatalog does not currently support dropNamespace");
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testSetPropertiesNotSupported() throws SQLException, InterruptedException {
    String schema = "Schema123";
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThatThrownBy(
              () -> snowflakeCatalog.setProperties(Namespace.of(dbName, schema), Maps.newHashMap()))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("SnowflakeCatalog does not currently support setProperties");
    } finally {
      dropSchemaIfExists(schema);
    }
  }

  @Test
  public void testRemovePropertiesNotSupported() throws SQLException, InterruptedException {
    String schema = "Schema123";
    String dbName = getDatabase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema);
      Assertions.assertThatThrownBy(
              () ->
                  snowflakeCatalog.removeProperties(
                      Namespace.of(dbName, schema), Sets.newHashSet()))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("SnowflakeCatalog does not currently support removeProperties");
    } finally {
      dropSchemaIfExists(schema);
    }
  }
}
