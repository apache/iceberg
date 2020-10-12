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

import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.error.NessieConflictException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.junit.After;
import org.junit.Before;

public abstract class TestIcebergSourceNessieTables extends TestIcebergSourceTablesBase {

  private static TableIdentifier currentIdentifier;

  private NessieClient client;
  private String branch;

  private Configuration getConfig() throws IOException {
    String defaultFs = temp.newFolder().toURI().toString();
    String fsImpl = org.apache.hadoop.fs.LocalFileSystem.class.getName();
    String path = "http://localhost:19121/api/v1";
    branch = "test-" + TestIcebergSourceNessieTables.class.getName();

    Configuration conf =  spark.sessionState().newHadoopConf();
    conf.set("fs.defaultFS", defaultFs);
    conf.set("fs.file.impl", fsImpl);
    conf.set("nessie.ref", branch);
    conf.set("nessie.url", path);
    setHadoopConfig(path, branch);
    this.client = new NessieClient(NessieClient.AuthType.NONE, path, null, null);

    try {
      this.client.getTreeApi().createEmptyBranch(branch);
    } catch (NessieConflictException e) {
      this.client.getTreeApi().deleteBranch(branch, this.client.getTreeApi().getReferenceByName(branch).getHash());
      this.client.getTreeApi().createEmptyBranch(branch);
    }
    return conf;
  }

  @Before
  public void start() throws IOException {
    SparkTestBase.nessie = new NessieCatalog("nessie", getConfig());

  }

  @After
  public void dropTable() throws IOException {
    Table table = nessie.loadTable(currentIdentifier);
    Path tablePath = new Path(table.location());
    FileSystem fs = tablePath.getFileSystem(getConfig());
    fs.delete(tablePath, true);
    nessie.refresh();
    nessie.dropTable(currentIdentifier, false);

    this.client.getTreeApi().deleteBranch(branch, this.client.getTreeApi().getReferenceByName(branch).getHash());
    this.client.close();
    unsetHadoopConfig();
  }

  @SuppressWarnings("RegexpSingleLine")
  private void setHadoopConfig(String path, String newBranch) {
    spark.sparkContext().hadoopConfiguration().set("nessie.url", path);
    spark.sparkContext().hadoopConfiguration().set("nessie.ref", newBranch);
  }

  @SuppressWarnings("RegexpSingleLine")
  private void unsetHadoopConfig() {
    spark.sparkContext().hadoopConfiguration().unset("nessie.url");
    spark.sparkContext().hadoopConfiguration().unset("nessie.ref");
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    TestIcebergSourceNessieTables.currentIdentifier = ident;
    return TestIcebergSourceNessieTables.nessie.createTable(ident, schema, spec);
  }

  @Override
  public Table loadTable(TableIdentifier ident, String entriesSuffix) {
    TableIdentifier identifier = TableIdentifier.of(ident.namespace().level(0), ident.name(), entriesSuffix);
    return TestIcebergSourceNessieTables.nessie.loadTable(identifier);
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
