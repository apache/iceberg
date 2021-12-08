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

package org.apache.iceberg.spark.source;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRelativePathHiveFunctionality extends SparkTestBase {
  private static File warehouse = null;
  private static final String HADOOP_CATALOG = "hadoop";
  private static final String HIVE_CATALOG = "hive_catalog";
  private static SparkSession spark = null;
  private static SparkConf conf = null;

  private Table table = null;

  private static final String TABLE_NAME = "default.test_table";
  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.LongType.get()),
      Types.NestedField.optional(3, "data", Types.StringType.get())
  );


  @BeforeClass
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @After
  public void dropTable() {
    TestTables.clearTables();
  }


  @Test
  public void testAbsoluteAndRelativePathSupport() throws NoSuchTableException {
    conf = new SparkConf();

    // Create a hadoop catalog and a hive catalog.
    ImmutableMap<String, String> hadoopConfig = ImmutableMap.of(
        "type", "hadoop",
        "default-namespace", "default",
        "warehouse", "file:" + warehouse
    );
    conf.set("spark.sql.catalog.hadoop", SparkCatalog.class.getName());
    hadoopConfig.forEach((key, value) -> conf.set("spark.sql.catalog." + HADOOP_CATALOG + "." + key, value));

    ImmutableMap<String, String> hiveConfig = ImmutableMap.of(
        "type", "hive",
        "default-namespace", "default"
    );
    conf.set("spark.sql.catalog.hive_catalog", SparkCatalog.class.getName());
    hiveConfig.forEach((key, value) -> conf.set("spark.sql.catalog." + HIVE_CATALOG + "." + key, value));

    spark = SparkSession.builder().config(conf).master("local[2]").getOrCreate();

    Catalog hadoopCatalog = new HadoopCatalog(spark.sessionState().newHadoopConf(),
        "file:" + warehouse);

    // Table hadoopTable = hadoopCatalog.buildTable(TableIdentifier.parse(TABLE_NAME), SCHEMA)
    //     .withLocationPrefix("file:" + warehouse.getParent())
    //     .withPartitionSpec(PartitionSpec.unpartitioned())
    //     .withProperties(ImmutableMap.of(
    //         TableProperties.WRITE_METADATA_USE_RELATIVE_PATH, "true",
    //         TableProperties.FORMAT_VERSION, "2"))
    //     .create();
    //
    // java.util.List<SimpleRecord> expected = Lists.newArrayList(
    //     new SimpleRecord(1, "a"),
    //     new SimpleRecord(2, "b"),
    //     new SimpleRecord(3, "c")
    // );
    //
    // Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    // df.writeTo(HADOOP_CATALOG + "." + TABLE_NAME).append();


    String metadataFilePath = warehouse + "/default/test_table/metadata/v1.metadata.json";
    HiveCatalog catalog = (HiveCatalog) CatalogUtil.loadCatalog(HiveCatalog.class.getName(), HIVE_CATALOG,
        ImmutableMap.of(), hiveConf);
    sql("CREATE table " + HIVE_CATALOG + "." + TABLE_NAME);
   // Table targetTable = catalog.registerTable(TableIdentifier.of("default", "target_table"), metadataFilePath);

    Assert.assertEquals("Table should now have 3 rows",
        3L, scalarSql("SELECT COUNT(*) FROM %s", HIVE_CATALOG + "." + TABLE_NAME));
  }

  private void createAndInitTable() throws IOException {
    File location = temp.newFolder();
    this.table = TestTables.create(temp.newFolder(), TABLE_NAME, SCHEMA, PartitionSpec.unpartitioned(),
        location.getParentFile(), true);
  }
}
