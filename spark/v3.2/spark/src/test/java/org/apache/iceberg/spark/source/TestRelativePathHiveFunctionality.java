/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRelativePathHiveFunctionality extends SparkTestBase {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);
  private static File warehouse = null;

  private Table table = null;

  private static final String TABLE_NAME = "test_table";
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "category", Types.StringType.get()),
      Types.NestedField.optional(3, "data", Types.StringType.get())
  );

  @BeforeClass
  public static void setupSpark() {
    ImmutableMap<String, String> hadoop_catalog = ImmutableMap.of(
        "type", "hadoop",
        "default-namespace", "default",
        "format-version", "2"
    );
    spark.conf().set("spark.sql.catalog.hadoop_catalog", SparkCatalog.class.getName());
    hadoop_catalog.forEach((key, value) -> spark.conf().set("spark.sql.catalog.spark_catalog." + key, value));

    ImmutableMap<String, String> hive_catalog = ImmutableMap.of(
        "type", "hive",
        "default-namespace", "default",
        "format-version", "2"
    );
    spark.conf().set("spark.sql.catalog.hive_catalog", SparkCatalog.class.getName());
    hive_catalog.forEach((key, value) -> spark.conf().set("spark.sql.catalog.hive_catalog." + key, value));
  }

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @BeforeClass
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
  }

  @Before
  public void setupTable() throws IOException {
    createAndInitTable();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void dropTable() {
    TestTables.clearTables();
  }


  public TestRelativePathHiveFunctionality() {

  }


  @Test
  public void testAbsoluteAndRelativePathSupport()  {




    String metadataFilePath = "blob://path/to/target-table/metadata/00005-uuid.metadata.json";
    HiveCatalog catalog = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "hive_prod",
        ImmutableMap.of(), hiveConf);
    //Table targetTable = catalog.registerTable(TableIdentifier.of("db2", "target_table"), metadataFilePath);
  }
  
  private void createAndInitTable() throws IOException {
    File location = temp.newFolder();
    this.table = TestTables.create(temp.newFolder(), TABLE_NAME, SCHEMA, PartitionSpec.unpartitioned(),
        location.getParentFile(), true);
  }
}
