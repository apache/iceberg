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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.view.ViewCatalogTests;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

public class TestHiveViewCatalog extends ViewCatalogTests<HiveCatalog> {

  private HiveCatalog catalog;

  @RegisterExtension
  private static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      HiveMetastoreExtension.builder().build();

  @BeforeEach
  public void before() throws TException {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());
  }

  @AfterEach
  public void cleanup() throws Exception {
    HIVE_METASTORE_EXTENSION.metastore().reset();
  }

  @Override
  protected HiveCatalog catalog() {
    return catalog;
  }

  @Override
  protected Catalog tableCatalog() {
    return catalog;
  }

  @Override
  protected boolean requiresNamespaceCreate() {
    return true;
  }

  @Test
  public void testListView() throws TException {
    String dbName = "hivedb";
    Namespace ns = Namespace.of(dbName);
    TableIdentifier identifier = TableIdentifier.of(ns, "tbl");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(identifier.namespace());
    }

    assertThat(catalog.viewExists(identifier)).isFalse();
    assertThat(catalog.listViews(ns)).isEmpty();

    String hiveTableName = "test_hive_view";
    // create a hive table
    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        createHiveView(hiveTableName, dbName, tempDir.toUri().toString());
    HIVE_METASTORE_EXTENSION.metastoreClient().createTable(hiveTable);

    catalog.setListAllTables(true);
    List<TableIdentifier> tableIdents1 = catalog.listTables(ns);
    assertThat(tableIdents1).as("should have one table with type VIRTUAL_VIEW.").hasSize(1);

    List<TableIdentifier> tableIdents2 = catalog.listViews(ns);
    assertThat(tableIdents2).as("should have zero iceberg view.").hasSize(0);

    catalog
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(ns)
        .withQuery("hive", "select * from ns.tbl")
        .create();
    assertThat(catalog.viewExists(identifier)).isTrue();

    List<TableIdentifier> tableIdents3 = catalog.listViews(ns);
    assertThat(tableIdents3).as("should have one iceberg view .").hasSize(1);
  }

  private org.apache.hadoop.hive.metastore.api.Table createHiveView(
      String hiveViewName, String dbName, String location) {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(
        serdeConstants.SERIALIZATION_CLASS, "org.apache.hadoop.hive.serde2.thrift.test.IntString");
    parameters.put(
        serdeConstants.SERIALIZATION_FORMAT, "org.apache.thrift.protocol.TBinaryProtocol");

    SerDeInfo serDeInfo =
        new SerDeInfo(null, "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", parameters);

    // StorageDescriptor has an empty list of fields - SerDe will report them.
    StorageDescriptor sd =
        new StorageDescriptor(
            Lists.newArrayList(),
            location,
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.mapred.TextOutputFormat",
            false,
            -1,
            serDeInfo,
            Lists.newArrayList(),
            Lists.newArrayList(),
            Maps.newHashMap());

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        new org.apache.hadoop.hive.metastore.api.Table(
            hiveViewName,
            dbName,
            "test_owner",
            0,
            0,
            0,
            sd,
            Lists.newArrayList(),
            Maps.newHashMap(),
            "viewOriginalText",
            "viewExpandedText",
            TableType.VIRTUAL_VIEW.name());
    return hiveTable;
  }
}
