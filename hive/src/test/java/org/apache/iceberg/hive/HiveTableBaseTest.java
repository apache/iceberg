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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
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
  private static File hiveLocalDir;

  private static ExecutorService executorService;
  private static TServer server;

  protected static HiveMetaStoreClient metastoreClient;
  protected static HiveCatalog catalog;

  @BeforeClass
  public static void startMetastore() throws Exception {
    HiveTableBaseTest.executorService = Executors.newSingleThreadExecutor();
    HiveTableBaseTest.hiveLocalDir = createTempDirectory("hive", asFileAttribute(fromString("rwxrwxrwx"))).toFile();
    File derbyLogFile = new File(hiveLocalDir, "derby.log");
    System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
    setupDB("jdbc:derby:" + getDerbyPath() + ";create=true");

    HiveTableBaseTest.server = thriftServer();
    executorService.submit(() -> server.serve());

    HiveTableBaseTest.metastoreClient = new HiveMetaStoreClient(hiveConf);
    metastoreClient.createDatabase(new Database(DB_NAME, "description", getDBPath(), new HashMap<>()));

    HiveTableBaseTest.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastore() {
    HiveTableBaseTest.catalog.close();

    metastoreClient.close();
    HiveTableBaseTest.metastoreClient = null;

    if (server != null) {
      server.stop();
    }

    executorService.shutdown();

    if (hiveLocalDir != null) {
      hiveLocalDir.delete();
    }
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

  private static HiveConf hiveConf(Configuration conf, int port) {
    final HiveConf newHiveConf = new HiveConf(conf, HiveTableBaseTest.class);
    newHiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
    newHiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + hiveLocalDir.getAbsolutePath());
    return newHiveConf;
  }

  private static String getDerbyPath() {
    final File metastoreDb = new File(hiveLocalDir, "metastore_db");
    return metastoreDb.getPath();
  }

  private static TServer thriftServer() throws Exception {
    TServerSocket socket = new TServerSocket(0);
    HiveTableBaseTest.hiveConf = hiveConf(new Configuration(), socket.getServerSocket().getLocalPort());
    HiveConf serverConf = new HiveConf(hiveConf);
    serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:" + getDerbyPath() + ";create=true");
    HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", serverConf);
    IHMSHandler handler = RetryingHMSHandler.getProxy(serverConf, baseHandler, false);

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
        .processor(new TSetIpAddressProcessor<>(handler))
        .transportFactory(new TTransportFactory())
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(3)
        .maxWorkerThreads(5);

    return new TThreadPoolServer(args);
  }

  private static void setupDB(String dbURL) throws SQLException, IOException {
    Connection connection = DriverManager.getConnection(dbURL);
    ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);

    URL hiveSqlScript = HiveTableBaseTest.class.getClassLoader().getResource("hive-schema-3.1.0.derby.sql");
    try (Reader reader = new BufferedReader(new FileReader(new File(hiveSqlScript.getFile())))) {
      scriptRunner.runScript(reader);
    }
  }

  private static String getDBPath() {
    return Paths.get(hiveLocalDir.getAbsolutePath(), DB_NAME + ".db").toAbsolutePath().toString();
  }

  private static String getTableBasePath(String tableName) {
    return Paths.get(getDBPath(), tableName).toAbsolutePath().toString();
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
    return filterByExtension(tableName, getFileExtension(hiveConf));
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
