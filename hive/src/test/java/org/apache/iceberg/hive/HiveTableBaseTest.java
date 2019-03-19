/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.iceberg.hive;

import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.types.Types;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.junit.After;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
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

import static com.netflix.iceberg.PartitionSpec.builderFor;
import static com.netflix.iceberg.TableMetadataParser.getFileExtension;
import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.CONNECT_URL_KEY;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.THRIFT_URIS;
import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.WAREHOUSE;


class HiveTableBaseTest {

  static final String DB_NAME = "hivedb";
  static final String TABLE_NAME =  "tbl";

  static final Schema schema = new Schema(Types.StructType.of(
          required(1, "id", Types.LongType.get())).fields());

  static final Schema altered = new Schema(Types.StructType.of(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.LongType.get())).fields());

  private static final PartitionSpec partitionSpec = builderFor(schema).identity("id").build();

  Configuration hiveConf;
  HiveMetaStoreClient metastoreClient;
  private File hiveLocalDir;

  private ExecutorService executorService;
  private TServer server;

  @Before
  public void setup() throws IOException,
          TException,
          InvocationTargetException,
          NoSuchMethodException,
          IllegalAccessException,
          NoSuchFieldException, SQLException {
    this.executorService = Executors.newSingleThreadExecutor();
    hiveLocalDir = createTempDirectory("hive", asFileAttribute(fromString("rwxrwxrwx"))).toFile();
    File derbyLogFile = new File(hiveLocalDir, "derby.log");
    System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());
    setupDB("jdbc:derby:" + getDerbyPath() + ";create=true");

    this.server = thriftServer();
    executorService.submit(() -> server.serve());

    this.metastoreClient = new HiveMetaStoreClient(this.hiveConf);
    createIfNotExistsCatalog("hive");
    this.metastoreClient.createDatabase(new Database(DB_NAME, "description", getDBPath(), new HashMap<>()));
    new HiveTables(this.hiveConf).create(schema, partitionSpec, DB_NAME, TABLE_NAME);
  }

  @After
  public void cleanup() {
    if (server != null) {
      server.stop();
    }

    executorService.shutdown();

    if (hiveLocalDir != null) {
      hiveLocalDir.delete();
    }
  }

  private HiveConf hiveConf(Configuration conf, int port) {
    final HiveConf hiveConf = new HiveConf(conf, this.getClass());
    hiveConf.set(THRIFT_URIS.getVarname(), "thrift://localhost:" + port);
    hiveConf.set(WAREHOUSE.getVarname(), "file:" + hiveLocalDir.getAbsolutePath());
    hiveConf.set(WAREHOUSE.getHiveName(), "file:" + hiveLocalDir.getAbsolutePath());
    hiveConf.set(CONNECT_URL_KEY.getVarname(), "jdbc:derby:" + getDerbyPath() + ";create=true");
    return hiveConf;
  }

  private String getDerbyPath() {
    final File metastore_db = new File(hiveLocalDir, "metastore_db");
    return metastore_db.getPath();
  }

  private TServer thriftServer() throws IOException,
          TTransportException,
          MetaException,
          InvocationTargetException,
          NoSuchMethodException,
          IllegalAccessException,
          NoSuchFieldException {
    final TServerSocketKeepAlive socket = new TServerSocketKeepAlive(new TServerSocket(0));
    this.hiveConf = hiveConf(new Configuration(), socket.getServerSocket().getLocalPort());
    HiveMetaStore.HMSHandler baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", hiveConf);
    IHMSHandler handler = RetryingHMSHandler.getProxy(hiveConf, baseHandler, true);
    final TTransportFactory transportFactory = new TTransportFactory();
    final TSetIpAddressProcessor<IHMSHandler> processor = new TSetIpAddressProcessor<>(handler);

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
            .processor(processor)
            .transportFactory(transportFactory)
            .protocolFactory(new TBinaryProtocol.Factory())
            .minWorkerThreads(3)
            .maxWorkerThreads(5);

    return new TThreadPoolServer(args);
  }

  private void setupDB(String dbURL) throws SQLException, IOException {
    Connection connection = DriverManager.getConnection(dbURL);
    ScriptRunner scriptRunner = new ScriptRunner(connection, true, true);

    URL hiveSqlScript = getClass().getClassLoader().getResource("hive-schema-3.1.0.derby.sql");
    Reader reader = new BufferedReader(new FileReader(new File(hiveSqlScript.getFile())));
    scriptRunner.runScript(reader);
  }

  private void createIfNotExistsCatalog(String catalogName) throws TException {
    try {
      metastoreClient.getCatalog(catalogName);
    } catch(NoSuchObjectException e) {
      String catalogPath = Paths.get(hiveLocalDir.getAbsolutePath(), catalogName + ".catalog").toString();
      metastoreClient.createCatalog(new Catalog(catalogName, catalogPath));
    }
  }

  private String getDBPath() {
   return Paths.get(hiveLocalDir.getAbsolutePath(), DB_NAME + ".db").toAbsolutePath().toString();
  }

  String getTableBasePath(String tableName) {
    return Paths.get(getDBPath(), tableName).toAbsolutePath().toString();
  }

  String getTableLocation(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString()).toString();
  }

  String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
            .map(File::getAbsolutePath)
            .collect(Collectors.toList());
  }

  List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(hiveConf));
  }

  List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName)
            .stream()
            .filter(f -> f.endsWith(extension))
            .collect(Collectors.toList());
  }

}
