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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.rules.TemporaryFolder;

public class TestHiveIcebergStorageHandlerWithHiveCatalogAndLinkedinMetadata
    extends TestHiveIcebergStorageHandlerWithHiveCatalog {

  private HiveCatalog hiveCatalog;
  private TemporaryFolder temporaryFolder;

  @Override
  public TestTables testTables(Configuration conf, TemporaryFolder temp) {
    hiveCatalog = HiveCatalogs.loadCatalog(conf);
    temporaryFolder = temp;
    return super.testTables(conf, temp);
  }

  @Override
  protected Table createIcebergTable(String tableName, Schema schema, List<Record> records) throws IOException {
    // This code is derived from TestTables. There was no easy way to alter table location without changing
    // bunch of interfaces. With this code the same outcome is achieved.
    TableIdentifier tableIdentifier = TableIdentifier.parse("default." + tableName);
    Table table = hiveCatalog.buildTable(tableIdentifier, schema)
        .withPartitionSpec(PartitionSpec.unpartitioned())
        .withLocation(getLocationWithoutURI(tableIdentifier))
        .withProperties(
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name(),
                TableProperties.ENGINE_HIVE_ENABLED, "true"))
        .create();

    if (!records.isEmpty()) {
      GenericAppenderHelper appender = new GenericAppenderHelper(table, fileFormat, temporaryFolder);
      table
          .newAppend()
          .appendFile(appender.writeFile(null, records))
          .commit();
    }
    return table;
  }

  private String getLocationWithoutURI(TableIdentifier tableIdentifier) {
    try {
      String location =  DynMethods.builder("defaultWarehouseLocation")
          .hiddenImpl(HiveCatalog.class, TableIdentifier.class)
          .build()
          .invoke(hiveCatalog, tableIdentifier);
      return new URI(location).getPath();
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
