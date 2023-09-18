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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
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
  }

  @Test
  public void testDropTable() throws IOException {
    dropTableInternal();
  }

  @Test
  public void testDropTableGCDisabled() throws IOException {
    sql("ALTER TABLE %s SET TBLPROPERTIES (gc.enabled = false)", tableName);
    dropTableInternal();
  }

  private void dropTableInternal() throws IOException {
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "test")),
        sql("SELECT * FROM %s", tableName));

    List<String> manifestAndFiles = manifestsAndFiles();
    Assert.assertEquals(
        "There should be 2 files for manifests and files", 2, manifestAndFiles.size());
    Assert.assertTrue("All files should be existed", checkFilesExist(manifestAndFiles, true));

    sql("DROP TABLE %s", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));

    if (catalogName.equals("testhadoop")) {
      // HadoopCatalog drop table without purge will delete the base table location.
      Assert.assertTrue("All files should be deleted", checkFilesExist(manifestAndFiles, false));
    } else {
      Assert.assertTrue("All files should not be deleted", checkFilesExist(manifestAndFiles, true));
    }
  }

  // TODO: enable once SPARK-43203 is fixed
  @Ignore
  public void testPurgeTable() throws IOException {
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "test")),
        sql("SELECT * FROM %s", tableName));

    List<String> manifestAndFiles = manifestsAndFiles();
    Assert.assertEquals(
        "There should be 2 files for manifests and files", 2, manifestAndFiles.size());
    Assert.assertTrue("All files should exist", checkFilesExist(manifestAndFiles, true));

    sql("DROP TABLE %s PURGE", tableName);
    Assert.assertFalse("Table should not exist", validationCatalog.tableExists(tableIdent));
    Assert.assertTrue("All files should be deleted", checkFilesExist(manifestAndFiles, false));
  }

  // TODO: enable once SPARK-43203 is fixed
  @Ignore
  public void testPurgeTableGCDisabled() throws IOException {
    sql("ALTER TABLE %s SET TBLPROPERTIES (gc.enabled = false)", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "test")),
        sql("SELECT * FROM %s", tableName));

    List<String> manifestAndFiles = manifestsAndFiles();
    Assert.assertEquals(
        "There totally should have 2 files for manifests and files", 2, manifestAndFiles.size());
    Assert.assertTrue("All files should be existed", checkFilesExist(manifestAndFiles, true));

    Assertions.assertThatThrownBy(() -> sql("DROP TABLE %s PURGE", tableName))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Cannot purge table: GC is disabled (deleting files may corrupt other tables");

    Assert.assertTrue("Table should not been dropped", validationCatalog.tableExists(tableIdent));
    Assert.assertTrue("All files should not be deleted", checkFilesExist(manifestAndFiles, true));
  }

  private List<String> manifestsAndFiles() {
    List<Object[]> files = sql("SELECT file_path FROM %s.%s", tableName, MetadataTableType.FILES);
    List<Object[]> manifests =
        sql("SELECT path FROM %s.%s", tableName, MetadataTableType.MANIFESTS);
    return Streams.concat(files.stream(), manifests.stream())
        .map(row -> (String) row[0])
        .collect(Collectors.toList());
  }

  private boolean checkFilesExist(List<String> files, boolean shouldExist) throws IOException {
    boolean mask = !shouldExist;
    if (files.isEmpty()) {
      return mask;
    }

    FileSystem fs = new Path(files.get(0)).getFileSystem(hiveConf);
    return files.stream()
        .allMatch(
            file -> {
              try {
                return fs.exists(new Path(file)) ^ mask;
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            });
  }
}
