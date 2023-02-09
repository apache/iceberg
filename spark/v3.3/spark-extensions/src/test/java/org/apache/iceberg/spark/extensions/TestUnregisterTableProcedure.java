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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestUnregisterTableProcedure extends SparkExtensionsTestBase {

  public TestUnregisterTableProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void dropTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testUnregisterTable() throws NoSuchTableException, IOException {
    if (catalogName.equals(SparkCatalogConfig.HADOOP.catalogName())) {
      AssertHelpers.assertThrows(
          "Should fail for hadoop catalog",
          UnsupportedOperationException.class,
          "Unregister table is not supported for Hadoop catalog",
          () -> sql("CALL %s.system.unregister_table('%s')", catalogName, tableName));
      return;
    }

    sql("CREATE TABLE %s (id int, data string) using ICEBERG", tableName);
    spark
        .range(0, 10)
        .withColumn("data", functions.col("id").cast(DataTypes.StringType))
        .writeTo(tableName)
        .append();

    Table table = catalog.loadTable(tableIdent);

    // delete the files from storage
    FileUtils.forceDelete(new File(table.location().replaceFirst("file:", "")));

    List<Object[]> result = sql("CALL %s.system.unregister_table('%s')", catalogName, tableName);
    Assert.assertTrue("Should be success", (boolean) result.get(0)[0]);

    AssertHelpers.assertThrows(
        "Entry should not be present in the catalog",
        org.apache.iceberg.exceptions.NoSuchTableException.class,
        "Table does not exist: default.table",
        () -> catalog.loadTable(tableIdent));
  }

  @Test
  public void testFailure() {
    Assume.assumeTrue(!catalogName.equals(SparkCatalogConfig.HADOOP.catalogName()));

    List<Object[]> result = sql("CALL %s.system.unregister_table('foo.bar')", catalogName);
    Assert.assertFalse("Should be a failure as table does not exist", (boolean) result.get(0)[0]);
  }

  @Test
  public void testInvalidInput() {
    AssertHelpers.assertThrows(
        "Should fail because of invalid input",
        AnalysisException.class,
        "Missing required parameters: [table]",
        () -> sql("CALL %s.system.unregister_table()", catalogName));

    AssertHelpers.assertThrows(
        "Should fail because of invalid input",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier for argument table",
        () -> sql("CALL %s.system.unregister_table('')", catalogName));
  }
}
