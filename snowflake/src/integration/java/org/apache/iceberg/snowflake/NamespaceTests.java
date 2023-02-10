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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class NamespaceTests extends SnowTestBase {

  @Test
  public void listNamespacesAtRootLevel() throws SQLException, InterruptedException {
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
  public void listNamespacesAtDatabaseLevel() throws SQLException, InterruptedException {
    String schema1 = "Schema_1";
    String schema2 = "Schema_2";
    String dbName = TestConfigurations.getInstance().getDatabase().toUpperCase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema1);
      createOrReplaceSchema(schema2);
      List<Namespace> namespaces = snowflakeCatalog.listNamespaces(Namespace.of(dbName));
      Assertions.assertThat(namespaces)
          .containsExactlyInAnyOrderElementsOf(
              new ArrayList<Namespace>(
                  Arrays.asList(
                      Namespace.of(dbName, schema1.toUpperCase()),
                      Namespace.of(dbName, schema2.toUpperCase()),
                      Namespace.of(dbName, "PUBLIC"),
                      Namespace.of(dbName, "INFORMATION_SCHEMA"))));
    } finally {
      dropSchemaIfExists(schema1);
      dropSchemaIfExists(schema2);
    }
  }

  @Test
  public void listNamespacesAtSchemaLevelIsNotAllowed() throws SQLException, InterruptedException {
    String schema1 = "Schema_1";
    String dbName = TestConfigurations.getInstance().getDatabase().toUpperCase();
    try {
      clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
      createOrReplaceSchema(schema1);
      Assertions.assertThatException()
          .isThrownBy(
              () -> {
                snowflakeCatalog.listNamespaces(Namespace.of(dbName, schema1));
              })
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNonExistingRootLevelNamespace() throws SQLException, InterruptedException {
    String nonExistingDb = "IDontExist";
    Assertions.assertThatThrownBy(
            () -> {
              snowflakeCatalog.loadNamespaceMetadata(Namespace.of(nonExistingDb));
            })
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  public void loadNonExistingDBLevelNamespace() throws SQLException, InterruptedException {
    String nonExistingSchema = "IDontExist";
    String db = TestConfigurations.getInstance().getDatabase();
    Assertions.assertThatThrownBy(
            () -> {
              snowflakeCatalog.loadNamespaceMetadata(Namespace.of(db, nonExistingSchema));
            })
        .isInstanceOf(NoSuchNamespaceException.class);
  }

  @Test
  public void loadNamespaceThatExceedMaxSupportedHierarchy()
      throws SQLException, InterruptedException {
    String nonExistingSchema = "IDontExist";
    String db = TestConfigurations.getInstance().getDatabase();
    Assertions.assertThatThrownBy(
            () -> {
              snowflakeCatalog.loadNamespaceMetadata(
                  Namespace.of(db, nonExistingSchema, "UnSupportedLevel"));
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void loadNamespaceWithUnquotedIdentifier() throws SQLException, InterruptedException {
    String schema1 = "Schema123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
              });
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNamespaceWithUnquotedIdentifierIsCaseInsensitive()
      throws SQLException, InterruptedException {
    String schema1 = "Schema123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1.toUpperCase()));
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, "schEmA123"));
              });
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNamespaceWithUnquotedIdentifierWithLeadingUnderscoreAndDollarCharacters()
      throws SQLException, InterruptedException {
    String schema1 = "_Schema$_123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
              });
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNamespaceWithQuotedIdentifierIsCaseSensitive()
      throws SQLException, InterruptedException {
    String schema1 = "\"Schema123\"";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1.toUpperCase()));
              })
          .isInstanceOf(NoSuchNamespaceException.class);
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNamespaceWithQuotedIdentifier() throws SQLException, InterruptedException {
    String schema1 = "\"Schema123\"";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
              });
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void loadNamespaceWithQuotedIdentifierWithSpecialCharacters()
      throws SQLException, InterruptedException {
    String schema1 = "\"H@!!.0-w*r()^'\"";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatNoException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
              });
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void dropNamespaceNotSupported() throws SQLException, InterruptedException {
    String schema1 = "Schema123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.dropNamespace(Namespace.of(dbName, schema1));
              })
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void setPropertiesNotSupported() throws SQLException, InterruptedException {
    String schema1 = "Schema123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.setProperties(
                    Namespace.of(dbName, schema1), new HashMap<String, String>());
              })
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      dropSchemaIfExists(schema1);
    }
  }

  @Test
  public void removePropertiesNotSupported() throws SQLException, InterruptedException {
    String schema1 = "Schema123";
    String dbName = TestConfigurations.getInstance().getDatabase();
    try {
      Assertions.assertThatException()
          .isThrownBy(
              () -> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.removeProperties(
                    Namespace.of(dbName, schema1), new HashSet<String>());
              })
          .isInstanceOf(UnsupportedOperationException.class);
    } finally {
      dropSchemaIfExists(schema1);
    }
  }
}
