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

import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSourceHiveTables {

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  private static SparkSession spark;
  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static HiveConf hiveConf;
  private static HiveCatalog catalog;

  @BeforeClass
  public static void startMetastoreAndSpark() throws Exception {
    TestIcebergSourceHiveTables.metastore = new TestHiveMetastore();
    metastore.start();
    TestIcebergSourceHiveTables.hiveConf = metastore.hiveConf();
    String dbPath = metastore.getDatabasePath("db");
    Database db = new Database("db", "desc", dbPath, new HashMap<>());
    TestIcebergSourceHiveTables.clients = new HiveClientPool(1, hiveConf);
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });

    TestIcebergSourceHiveTables.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .getOrCreate();

    TestIcebergSourceHiveTables.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    catalog.close();
    TestIcebergSourceHiveTables.catalog = null;
    clients.close();
    TestIcebergSourceHiveTables.clients = null;
    metastore.stop();
    TestIcebergSourceHiveTables.metastore = null;
    spark.stop();
    TestIcebergSourceHiveTables.spark = null;
  }

  @Test
  public void testHiveTablesSupport() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    try {
      catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

      List<SimpleRecord> expectedRecords = Lists.newArrayList(
          new SimpleRecord(1, "1"),
          new SimpleRecord(2, "2"),
          new SimpleRecord(3, "3"));

      Dataset<Row> inputDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode(SaveMode.Append)
          .save(tableIdentifier.toString());

      Dataset<Row> resultDf = spark.read()
          .format("iceberg")
          .load(tableIdentifier.toString());
      List<SimpleRecord> actualRecords = resultDf.orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Records should match", expectedRecords, actualRecords);

      // test metadata table name resolution
      for (String metadataName : new String[] {"history", "snapshots", "files", "entries", "manifests"}) {
        String metadataTableName = tableIdentifier.toString() + "." + metadataName;
        Dataset<Row> df = spark.read().format("iceberg").load(metadataTableName);
        Assert.assertNotNull("Should resolve metadata table: " + metadataName, df);
      }

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }
}
