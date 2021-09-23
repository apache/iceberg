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
import java.nio.file.Files;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestDropTable extends SparkCatalogTestBase {
  private final boolean shouldPurgeDataAndMetadata;

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][]{
        {"testhive", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"
        ) },
        {"testhadoop", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hadoop"
        ) },
        {"testhive_purge", SparkCatalog.class.getName(),
          ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            SparkCatalog.PURGE_DATA_AND_METADATA, "false"
        ) }
    };
  }

  public TestDropTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    if (config.containsKey(SparkCatalog.PURGE_DATA_AND_METADATA)) {
      this.shouldPurgeDataAndMetadata = Boolean.parseBoolean(config.get(SparkCatalog.PURGE_DATA_AND_METADATA));
    } else {
      this.shouldPurgeDataAndMetadata = SparkCatalog.PURGE_DATA_AND_METADATA_DEFAULT;
    }
  }

  @Test
  public void testDropTable() throws Exception {
    Assert.assertFalse("Table should not already exist", validationCatalog.tableExists(tableIdent));

    sql("CREATE TABLE %s USING iceberg as select 1 as value", tableName);

    Table table = getIcebergTableFromSparkCatalog();
    Assert.assertNotNull("Should load the new table", table);
    String location = table.location().replaceFirst("file:", "");
    File metadata = new File(location, "metadata");
    File data = new File(location, "data");

    sql("drop table %s", tableName);
    if (shouldPurgeDataAndMetadata) {
      Assert.assertTrue("Data folder should be dropped", notExistsOrEmpty(data));
      Assert.assertTrue("Metadata folder should be dropped", notExistsOrEmpty(metadata));

    } else {
      Assert.assertTrue("Data folder should exists", data.exists());
      Assert.assertTrue("Metadata folder should exists", metadata.exists());

      Assert.assertFalse("Data folder shouldn't be empty", isEmpty(data));
      Assert.assertFalse("Metadata folder shouldn't be empty", isEmpty(data));
    }
  }

  private boolean notExistsOrEmpty(File dir) throws IOException {
    return !dir.exists() || countFiles(dir) == 0;
  }

  private boolean isEmpty(File dir) throws IOException {
    return countFiles(dir) == 0;
  }

  private long countFiles(File dir) throws IOException {
    return Files.list(dir.toPath()).count();
  }

  @SuppressWarnings("ThrowSpecificity")
  private Table getIcebergTableFromSparkCatalog() throws Exception {
    Identifier identifier = Identifier.of(tableIdent.namespace().levels(), tableIdent.name());
    TableCatalog catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
    SparkTable sparkTable =  (SparkTable) catalog.loadTable(identifier);
    return sparkTable.table();
  }

}
