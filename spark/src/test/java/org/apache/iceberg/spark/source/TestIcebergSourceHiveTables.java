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

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

public class TestIcebergSourceHiveTables extends TestIcebergSourceTablesBase {

  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static HiveConf hiveConf;
  private static HiveCatalog catalog;
  private static TableIdentifier currentIdentifier;

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

  @After
  public void dropTable() throws IOException {
    Table table = catalog.loadTable(currentIdentifier);
    Path tablePath = new Path(table.location());
    FileSystem fs = tablePath.getFileSystem(hiveConf);
    fs.delete(tablePath, true);
    catalog.dropTable(currentIdentifier, false);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    TestIcebergSourceHiveTables.currentIdentifier = ident;
    return TestIcebergSourceHiveTables.catalog.createTable(ident, schema, spec);
  }

  @Override
  public Table loadTable(TableIdentifier ident, String entriesSuffix) {
    TableIdentifier identifier = TableIdentifier.of(ident.namespace().level(0), ident.name(), entriesSuffix);
    return TestIcebergSourceHiveTables.catalog.loadTable(identifier);
  }

  @Override
  public String loadLocation(TableIdentifier ident, String entriesSuffix) {
    return String.format("%s.%s", loadLocation(ident), entriesSuffix);
  }

  @Override
  public String loadLocation(TableIdentifier ident) {
    return ident.toString();
  }
}
