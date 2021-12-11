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

package org.apache.iceberg.spark.extensions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.TableProperties.GC_ENABLED;

public class TestDeleteReachableFilesProcedure extends SparkExtensionsTestBase {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  public TestDeleteReachableFilesProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testDeleteReachableFilesInEmptyTable() throws IOException {
    if (!createTable()) {
      return;
    }
    Table table = catalog.loadTable(tableIdent);
    String metadataFileLocation = metadataFileLocation(table);

    catalog.dropTable(tableIdent, false);

    List<Object[]> output = sql(
        "CALL %s.system.delete_reachable_files('%s')",
        catalogName, metadataFileLocation);
    assertEquals("Should delete only other files (metadata json and version_hint file)",
        ImmutableList.of(row(0L, 0L, 0L, 2L)), output);
  }

  @Test
  public void testDeleteReachableFilesInDataFolder() throws IOException {
    if (!createTable()) {
      return;
    }

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = catalog.loadTable(tableIdent);
    String metadataFileLocation = metadataFileLocation(table);

    catalog.dropTable(tableIdent, false);

    List<Object[]> output = sql(
        "CALL %s.system.delete_reachable_files('%s')",
        catalogName, metadataFileLocation);
    assertEquals("Should delete expected number of files",
        ImmutableList.of(row(2L, 2L, 2L, 4L)), output);
  }

  @Test
  public void testDeleteReachableFilesGCDisabled() throws IOException {
    if (!createTable()) {
      return;
    }
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    Table table = catalog.loadTable(tableIdent);
    String metadataFileLocation = metadataFileLocation(table);

    catalog.dropTable(tableIdent, false);
    AssertHelpers.assertThrows("Should reject call",
        ValidationException.class, "Cannot remove files: GC is disabled",
        () -> sql("CALL %s.system.delete_reachable_files('%s')", catalogName, metadataFileLocation));
  }

  private String metadataFileLocation(Table tbl) {
    return ((HasTableOperations) tbl).operations().current().metadataFileLocation();
  }

  private boolean createTable() throws IOException {
    if (catalogName.equals("testhadoop") || catalogName.equals("testhive")) {
      // This procedure cannot work for hadoop catalog,
      // as after drop table metadata file will be deleted (even with purge=false)
      // For hive catalog, drop table with purge = true is not cleaning the table in metastore.
      // Hence, table creation failing for second test case
      return false;
    }
    // give a fresh location to Hive tables as Spark will not clean up the table location
    // correctly while dropping tables through spark_catalog
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg LOCATION '%s'",
            tableName, temp.newFolder());
    return true;
  }
}
