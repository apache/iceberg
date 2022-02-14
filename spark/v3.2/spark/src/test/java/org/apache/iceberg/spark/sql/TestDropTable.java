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

package org.apache.iceberg.spark.sql;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDropTable extends SparkCatalogTestBase {

  public TestDropTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTable() {
    sql("CREATE TABLE %s (id INT, name STRING) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'test')", tableName);
  }

  @After
  public void removeTable() throws IOException {
    sql("DROP TABLE IF EXISTS %s", tableName);

    File baseLocation = new File(getTableBaseLocation());
    if (baseLocation.exists()) {
      FileUtils.deleteDirectory(baseLocation);
    }
  }

  @Test
  public void testDropTable() {
    dropTableInternal();
  }

  @Test
  public void testDropTableGCDisabled() {
    sql("ALTER TABLE %s SET TBLPROPERTIES (gc.enabled = false)", tableName);
    dropTableInternal();
  }

  private void dropTableInternal() {
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "test")), sql("SELECT * FROM %s", tableName));

    List<String> previousDataFiles = getDataOrMetadataFiles(true);
    Assert.assertTrue("The number of data files should be larger than zero", previousDataFiles.size() > 0);

    sql("DROP TABLE %s", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));

    if (catalogName.equals("testhadoop")) {
      // HadoopCatalog drop table without purge will delete the base table location.
      Assert.assertEquals("The number of data files should be zero", 0, getDataOrMetadataFiles(true).size());
      Assert.assertEquals("The number of metadata files should be zero", 0, getDataOrMetadataFiles(false).size());
    } else {
      Assert.assertEquals("The data files should not change", previousDataFiles, getDataOrMetadataFiles(true));
    }
  }

  @Test
  public void testPurgeTable() {
    assertEquals("Should have expected rows",
        ImmutableList.of(row(1, "test")),
        sql("SELECT * FROM %s", tableName));

    List<String> previousDataFiles = getDataOrMetadataFiles(true);
    Assert.assertTrue("The number of data files should be larger than zero", previousDataFiles.size() > 0);

    sql("DROP TABLE %s PURGE", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));
    Assert.assertEquals("The number of data files should be zero", 0, getDataOrMetadataFiles(true).size());
    Assert.assertEquals("The number of metadata files should be zero", 0, getDataOrMetadataFiles(false).size());
  }

  @Test
  public void testPurgeTableGCDisabled() {
    sql("ALTER TABLE %s SET TBLPROPERTIES (gc.enabled = false)", tableName);
    List<String> previousDataFiles = getDataOrMetadataFiles(true);
    List<String> previousMetadataFiles = getDataOrMetadataFiles(false);
    Assert.assertTrue("The number of data files should be larger than zero", previousDataFiles.size() > 0);
    Assert.assertTrue("The number of metadata files should be larger than zero", previousMetadataFiles.size() > 0);

    AssertHelpers.assertThrows("Purge table is not allowed when GC is disabled", ValidationException.class,
        "Cannot purge table: GC is disabled (deleting files may corrupt other tables",
        () -> sql("DROP TABLE %s PURGE", tableName));

    Assert.assertTrue("Table should not been dropped", validationCatalog.tableExists(tableIdent));
    Assert.assertEquals("The data files should not change", previousDataFiles, getDataOrMetadataFiles(true));
    Assert.assertEquals("The metadata files should not change", previousMetadataFiles, getDataOrMetadataFiles(false));
  }

  private List<String> getDataOrMetadataFiles(boolean dataFiles) {
    String baseLocation = getTableBaseLocation();

    File dir;
    if (dataFiles) {
      dir = new File(baseLocation, "data");
    } else {
      dir = new File(baseLocation, "metadata");
    }

    if (dir.exists()) {
      return Arrays.stream(dir.listFiles()).map(File::getAbsolutePath).collect(Collectors.toList());
    } else {
      return Lists.newArrayList();
    }
  }

  private String getTableBaseLocation() {
    String baseLocation;
    if (catalogName.equals("testhadoop")) {
      baseLocation = Paths.get(warehouse.getAbsolutePath(), "default", "table").toAbsolutePath().toString();
    } else {
      String databasePath = metastore.getDefaultDatabasePath();
      baseLocation = Paths.get(databasePath, "table").toAbsolutePath().toString();
    }

    return baseLocation;
  }
}
