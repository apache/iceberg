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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestFlinkHiveCatalog extends TestBase {

  @Test
  public void testCreateCatalogWithWarehouseLocation() throws IOException {
    Map<String, String> props = Maps.newHashMap();
    props.put("type", "iceberg");
    props.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
    props.put(CatalogProperties.URI, CatalogTestBase.getURI(hiveConf));

    File warehouseDir = Files.createTempDirectory(temporaryDirectory, "junit").toFile();
    props.put(CatalogProperties.WAREHOUSE_LOCATION, "file://" + warehouseDir.getAbsolutePath());

    checkSQLQuery(props, warehouseDir);
  }

  @Test
  public void testCreateCatalogWithHiveConfDir() throws IOException {
    // Dump the hive conf into a local file.
    File hiveConfDir = Files.createTempDirectory(temporaryDirectory, "junit").toFile();
    File hiveSiteXML = new File(hiveConfDir, "hive-site.xml");
    File warehouseDir = Files.createTempDirectory(temporaryDirectory, "junit").toFile();
    try (FileOutputStream fos = new FileOutputStream(hiveSiteXML)) {
      Configuration newConf = new Configuration(hiveConf);
      // Set another new directory which is different with the hive metastore's warehouse path.
      newConf.set(
          HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file://" + warehouseDir.getAbsolutePath());
      newConf.writeXml(fos);
    }
    assertThat(hiveSiteXML.toPath()).exists();

    // Construct the catalog attributions.
    Map<String, String> props = Maps.newHashMap();
    props.put("type", "iceberg");
    props.put(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE, "hive");
    props.put(CatalogProperties.URI, CatalogTestBase.getURI(hiveConf));
    // Set the 'hive-conf-dir' instead of 'warehouse'
    props.put(FlinkCatalogFactory.HIVE_CONF_DIR, hiveConfDir.getAbsolutePath());

    checkSQLQuery(props, warehouseDir);
  }

  private void checkSQLQuery(Map<String, String> catalogProperties, File warehouseDir)
      throws IOException {
    sql("CREATE CATALOG test_catalog WITH %s", CatalogTestBase.toWithClause(catalogProperties));
    sql("USE CATALOG test_catalog");
    sql("CREATE DATABASE test_db");
    sql("USE test_db");
    sql("CREATE TABLE test_table(c1 INT, c2 STRING)");
    sql("INSERT INTO test_table SELECT 1, 'a'");

    Path databasePath = warehouseDir.toPath().resolve("test_db.db");
    assertThat(databasePath).exists();

    Path tablePath = databasePath.resolve("test_table");
    assertThat(tablePath).exists();

    Path dataPath = tablePath.resolve("data");
    assertThat(dataPath).exists();
    assertThat(Files.list(dataPath).count())
        .as("Should have a .crc file and a .parquet file")
        .isEqualTo(2);

    sql("DROP TABLE test_table");
    sql("DROP DATABASE test_db");
    dropCatalog("test_catalog", false);
  }
}
