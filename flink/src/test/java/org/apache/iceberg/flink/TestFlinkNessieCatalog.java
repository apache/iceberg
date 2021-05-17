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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestFlinkNessieCatalog extends FlinkTestBase {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testCreateNessieCatalog() throws IOException {
    String port = System.getProperty("quarkus.http.test-port", "19120");
    String uri = String.format("http://localhost:%s/api/v1", port);
    Map<String, String> props = Maps.newHashMap();
    props.put("type", "iceberg");
    props.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.nessie.NessieCatalog");
    props.put(CatalogProperties.URI, uri);
    props.put("ref", "main");
    props.put("auth_type", "NONE");
    File warehouseDir = tempFolder.newFolder();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + warehouseDir.getAbsolutePath());

    checkSQLQuery(props, warehouseDir);
  }

  private void checkSQLQuery(Map<String, String> catalogProperties, File warehouseDir) throws IOException {
    sql("CREATE CATALOG test_catalog WITH %s", FlinkCatalogTestBase.toWithClause(catalogProperties));
    sql("USE CATALOG test_catalog");
    sql("CREATE DATABASE test_db");
    sql("USE test_db");
    sql("CREATE TABLE test_table(c1 INT, c2 STRING)");
    sql("INSERT INTO test_table SELECT 1, 'a'");

    Path databasePath = warehouseDir.toPath().resolve("test_db.db");
    Assert.assertTrue("Database path should exist", Files.exists(databasePath));

    Path tablePath = databasePath.resolve("test_table");
    Assert.assertTrue("Table path should exist", Files.exists(tablePath));

    Path dataPath = tablePath.resolve("data");
    Assert.assertTrue("Table data path should exist", Files.exists(dataPath));
    Assert.assertEquals("Should have a .crc file and a .parquet file", 2, Files.list(dataPath).count());

    sql("DROP TABLE test_table");
    sql("DROP DATABASE test_db");
    sql("DROP CATALOG test_catalog");
  }
}
