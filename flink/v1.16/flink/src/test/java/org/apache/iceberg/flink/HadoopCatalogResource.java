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
package org.apache.iceberg.flink;

import java.io.File;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public class HadoopCatalogResource extends ExternalResource {
  protected final TemporaryFolder temporaryFolder;
  protected final String database;
  protected final String tableName;

  protected HadoopCatalog catalog;
  protected String warehouse;
  protected String location;
  protected TableLoader tableLoader;

  public HadoopCatalogResource(TemporaryFolder temporaryFolder, String database, String tableName) {
    this.temporaryFolder = temporaryFolder;
    this.database = database;
    this.tableName = tableName;
  }

  @Override
  protected void before() throws Throwable {
    File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    // before variables
    this.warehouse = "file:" + warehouseFile;
    Configuration hadoopConf = new Configuration();
    this.catalog = new HadoopCatalog(hadoopConf, warehouse);
    this.location = String.format("%s/%s/%s", warehouse, database, tableName);
    this.tableLoader = TableLoader.fromHadoopTable(location);
  }

  @Override
  protected void after() {}

  public TableLoader tableLoader() {
    return tableLoader;
  }

  public HadoopCatalog catalog() {
    return catalog;
  }

  public String warehouse() {
    return warehouse;
  }
}
