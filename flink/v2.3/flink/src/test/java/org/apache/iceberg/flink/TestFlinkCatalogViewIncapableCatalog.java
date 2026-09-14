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

import java.nio.file.Path;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.types.Row;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A catalog can implement {@link org.apache.iceberg.catalog.ViewCatalog} but still reject view
 * operations at runtime — a JDBC catalog with a V0 schema throws {@link
 * UnsupportedOperationException} from every view method. Table operations through such a catalog
 * must keep working as if views were not supported.
 */
public class TestFlinkCatalogViewIncapableCatalog extends TestBase {

  private static final String CATALOG_NAME = "jdbc_v0";

  @TempDir private Path warehouse;

  @AfterEach
  public void cleanCatalog() {
    sql("USE CATALOG default_catalog");
    dropCatalog(CATALOG_NAME, true);
  }

  @Test
  public void testTableOperationsWithViewIncapableCatalog() {
    sql(
        "CREATE CATALOG %s WITH ("
            + "'type'='iceberg', "
            + "'catalog-impl'='org.apache.iceberg.jdbc.JdbcCatalog', "
            + "'uri'='jdbc:sqlite:%s/catalog.db', "
            + "'warehouse'='file://%s/warehouse')",
        CATALOG_NAME, warehouse, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE db_v0");
    sql("USE db_v0");
    sql("CREATE TABLE t (id BIGINT)");
    sql("INSERT INTO t VALUES (1)");

    // none of these may surface the catalog's UnsupportedOperationException
    assertThat(sql("SHOW TABLES")).containsExactly(Row.of("t"));
    assertThat(sql("SHOW VIEWS")).isEmpty();
    assertSameElements(Lists.newArrayList(Row.of(1L)), sql("SELECT * FROM t"));
    assertThat(
            getTableEnv()
                .getCatalog(CATALOG_NAME)
                .get()
                .tableExists(new ObjectPath("db_v0", "missing")))
        .isFalse();
    assertThatThrownBy(() -> sql("SELECT * FROM missing"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Object 'missing' not found");
  }
}
