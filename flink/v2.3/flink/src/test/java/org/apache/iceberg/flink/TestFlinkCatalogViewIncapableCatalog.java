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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A catalog can implement {@link ViewCatalog} but still reject view operations at runtime: a JDBC
 * catalog with the V0 schema throws {@link UnsupportedOperationException} from every view method.
 * Table operations through such a catalog must keep working as if views were not supported.
 */
class TestFlinkCatalogViewIncapableCatalog extends TestBase {

  private static final String CATALOG_NAME = "jdbc_v0";
  private static final String DATABASE = "db_v0";
  private static final String TABLE_NAME = "t";

  @BeforeEach
  void createCatalogAndTable() {
    sql(
        "CREATE CATALOG %s WITH ("
            + "'type'='iceberg', "
            + "'catalog-impl'='org.apache.iceberg.jdbc.JdbcCatalog', "
            + "'uri'='jdbc:sqlite:%s/catalog.db', "
            + "'warehouse'='file://%s/warehouse')",
        CATALOG_NAME, temporaryDirectory, temporaryDirectory);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE);
    sql("USE %s", DATABASE);
    sql("CREATE TABLE %s (id BIGINT)", TABLE_NAME);
    sql("INSERT INTO %s VALUES (1)", TABLE_NAME);
  }

  @AfterEach
  void cleanCatalog() {
    dropCatalog(CATALOG_NAME, true);
  }

  @Test
  void listViews() {
    assertThat(sql("SHOW VIEWS")).isEmpty();
  }

  @Test
  void listTables() {
    assertThat(sql("SHOW TABLES")).containsExactly(Row.of(TABLE_NAME));
  }

  @Test
  void tableExists() {
    Catalog flinkCatalog = getTableEnv().getCatalog(CATALOG_NAME).get();
    assertThat(flinkCatalog.tableExists(new ObjectPath(DATABASE, TABLE_NAME))).isTrue();
    assertThat(flinkCatalog.tableExists(new ObjectPath(DATABASE, "missing"))).isFalse();
  }

  @Test
  void dropTable() throws Exception {
    Catalog flinkCatalog = getTableEnv().getCatalog(CATALOG_NAME).get();
    flinkCatalog.dropTable(new ObjectPath(DATABASE, "missing"), true);
    assertThatThrownBy(() -> flinkCatalog.dropTable(new ObjectPath(DATABASE, "missing"), false))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("missing");

    sql("DROP TABLE %s", TABLE_NAME);
    assertThat(sql("SHOW TABLES")).isEmpty();
  }

  @Test
  void renameTable() throws Exception {
    Catalog flinkCatalog = getTableEnv().getCatalog(CATALOG_NAME).get();
    flinkCatalog.renameTable(new ObjectPath(DATABASE, "missing"), "renamed", true);
    assertThatThrownBy(
            () -> flinkCatalog.renameTable(new ObjectPath(DATABASE, "missing"), "renamed", false))
        .isInstanceOf(TableNotExistException.class)
        .hasMessageContaining("missing");

    sql("ALTER TABLE %s RENAME TO renamed", TABLE_NAME);
    assertThat(sql("SHOW TABLES")).containsExactly(Row.of("renamed"));
  }

  @Test
  void getTable() {
    assertSameElements(Lists.newArrayList(Row.of(1L)), sql("SELECT * FROM %s", TABLE_NAME));
    assertThatThrownBy(() -> sql("SELECT * FROM missing"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Object 'missing' not found");
  }
}
