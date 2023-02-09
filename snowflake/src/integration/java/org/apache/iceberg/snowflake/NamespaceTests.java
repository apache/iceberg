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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.naming.Name;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

public class NamespaceTests extends SnowTestBase {

  @BeforeClass
  static public void beforeAll() {
    SnowTestBase.beforeAll();
    try {
      clientPool.run(
          conn -> {
            return conn.createStatement()
                .execute(
                    "CREATE OR REPLACE DATABASE " + TestConfigurations.getInstance().getDatabase());
          });
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

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
      Assertions.assertThat( namespaces)
          .containsExactlyInAnyOrderElementsOf(
              new ArrayList<Namespace>(
                  Arrays.asList(Namespace.of(dbName, schema1.toUpperCase()),
                          Namespace.of(dbName, schema2.toUpperCase()),
                          Namespace.of(dbName, "PUBLIC"),
                          Namespace.of(dbName, "INFORMATION_SCHEMA"))));
    } finally {
      dropSchemaIfExists(schema1);
      dropSchemaIfExists(schema2);
    }
  }

    @Test
    public void loadNonExistingRootLevelNamespace() throws SQLException, InterruptedException {
        String nonExistingDb = "IDontExist";
        Assertions.assertThatThrownBy(
                () -> {
                    snowflakeCatalog.loadNamespaceMetadata(Namespace.of(nonExistingDb));
                }).isInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    public void loadNonExistingDBLevelNamespace() throws SQLException, InterruptedException {
        String nonExistingSchema = "IDontExist";
        String db = TestConfigurations.getInstance().getDatabase();
        Assertions.assertThatThrownBy(
                () -> {
                    snowflakeCatalog.loadNamespaceMetadata(Namespace.of(db, nonExistingSchema));
                }).isInstanceOf(NoSuchNamespaceException.class);
    }

    @Test
    public void loadNamespaceThatExceedMaxSupportedHierarchy() throws SQLException, InterruptedException {
        String nonExistingSchema = "IDontExist";
        String db = TestConfigurations.getInstance().getDatabase();
        Assertions.assertThatThrownBy(
                () -> {
                    snowflakeCatalog.loadNamespaceMetadata(Namespace.of(db, nonExistingSchema,"UnSupportedLevel"
                            ));
                }).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void loadNamespaceWithUnquotedIdentifier() throws SQLException, InterruptedException {
        String schema1 = "Schema123";
        String dbName = TestConfigurations.getInstance().getDatabase();
        try {
            Assertions.assertThatNoException().isThrownBy( ()-> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
            });
        } finally {
            dropSchemaIfExists(schema1);
        }
    }

    @Test
    public void loadNamespaceWithUnquotedIdentifierIsCaseInsensitive() throws SQLException, InterruptedException {
        String schema1 = "Schema123";
        String dbName = TestConfigurations.getInstance().getDatabase();
        try {
            Assertions.assertThatNoException().isThrownBy( ()-> {
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
    public void loadNamespaceWithUnquotedIdentifierWithLeadingUnderscoreAndDollarCharacters() throws SQLException, InterruptedException {
        String schema1 = "_Schema$_123";
        String dbName = TestConfigurations.getInstance().getDatabase();
        try {
            Assertions.assertThatNoException().isThrownBy( ()-> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
            });
        } finally {
            dropSchemaIfExists(schema1);
        }
    }

    @Test
    public void loadNamespaceWithQuotedIdentifier() throws SQLException, InterruptedException {
        String schema1 = "\"Schema123\"";
        String dbName = TestConfigurations.getInstance().getDatabase();
        try {
            Assertions.assertThatNoException().isThrownBy( ()-> {
                clientPool.run(conn -> conn.createStatement().execute("use " + dbName));
                createOrReplaceSchema(schema1);
                snowflakeCatalog.loadNamespaceMetadata(Namespace.of(dbName, schema1));
            });
        } finally {
            dropSchemaIfExists(schema1);
        }
    }
  @AfterAll
  public void afterAll() {
    try {
      clientPool.run(
          conn -> {
            return conn.createStatement()
                .execute(
                    "DROP DATABASE IF EXISTS " + TestConfigurations.getInstance().getDatabase());
          });
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void createOrReplaceSchema(String schemaName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
            PreparedStatement statement = conn.prepareStatement("CREATE OR REPLACE SCHEMA IDENTIFIER(?)");
            statement.setString(1, schemaName);
          return statement.execute();
        });
  }

  private void dropSchemaIfExists(String schemaName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
            PreparedStatement statement = conn.prepareStatement("DROP SCHEMA IF EXISTS IDENTIFIER(?)");
            statement.setString(1, schemaName);
            return statement.execute();
        });
  }
}
