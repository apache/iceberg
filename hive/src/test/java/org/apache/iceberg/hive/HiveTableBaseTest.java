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

package org.apache.iceberg.hive;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;


public class HiveTableBaseTest {

  static final String DB_NAME = "hivedb";
  static final String TABLE_NAME =  "tbl";
  static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);

  static final Schema schema = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get())).fields());

  static final Schema altered = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.LongType.get())).fields());

  private static final PartitionSpec partitionSpec = builderFor(schema).identity("id").build();

  private static HiveConf hiveConf;
  private static TestHiveMetastore metastore;

  protected static HiveMetaStoreClient metastoreClient;
  protected static HiveCatalog catalog;

  @BeforeClass
  public static void startMetastore() throws Exception {
    HiveTableBaseTest.metastore = new TestHiveMetastore();
    metastore.start();
    HiveTableBaseTest.hiveConf = metastore.hiveConf();
    HiveTableBaseTest.metastoreClient = new HiveMetaStoreClient(hiveConf);
    String dbPath = metastore.getDatabasePath(DB_NAME);
    Database db = new Database(DB_NAME, "description", dbPath, new HashMap<>());
    metastoreClient.createDatabase(db);
    HiveTableBaseTest.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastore() {
    HiveTableBaseTest.catalog.close();

    metastoreClient.close();
    HiveTableBaseTest.metastoreClient = null;

    metastore.stop();
    HiveTableBaseTest.metastore = null;
  }

  private Path tableLocation;

  @Before
  public void createTestTable() {
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema, partitionSpec).location());
  }

  @After
  public void dropTestTable() throws Exception {
    // drop the table data
    tableLocation.getFileSystem(hiveConf).delete(tableLocation, true);
    catalog.dropTable(TABLE_IDENTIFIER);
  }

  private static String getTableBasePath(String tableName) {
    String databasePath = metastore.getDatabasePath(DB_NAME);
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  protected static String getTableLocation(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString()).toString();
  }

  private static String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private static List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());
  }

  protected static List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
  }

  protected static List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private static List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName)
        .stream()
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }

}
