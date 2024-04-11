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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchIcebergViewException;
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
  public void testHiveViewAndIcebergViewWithSameName() throws TException, IOException {
    String dbName = "hivedb";
    Namespace ns = Namespace.of(dbName);
    String viewName = "test_hive_view";
    TableIdentifier identifier = TableIdentifier.of(ns, viewName);

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(identifier.namespace());
    }

    assertThat(catalog.listViews(ns)).isEmpty();
    // create a hive table
    Table hiveTable =
        createHiveView(
            viewName, dbName, Files.createTempDirectory("hive-view-tests-name").toString());
    HIVE_METASTORE_EXTENSION.metastoreClient().createTable(hiveTable);

    catalog.setListAllTables(true);
    assertThat(catalog.listTables(ns)).containsExactly(identifier).hasSize(1);

    assertThat(catalog.viewExists(identifier)).isFalse();

    assertThatThrownBy(
            () ->
                catalog
                    .buildView(identifier)
                    .withSchema(SCHEMA)
                    .withDefaultNamespace(ns)
                    .withQuery("hive", "select * from hivedb.tbl")
                    .create())
        .isInstanceOf(NoSuchIcebergViewException.class)
        .hasMessageStartingWith("Not an iceberg view: hive.hivedb.test_hive_view");
  }

  @Test
  public void testListViewWithHiveView() throws TException, IOException {
    String dbName = "hivedb";
    Namespace ns = Namespace.of(dbName);
    TableIdentifier identifier = TableIdentifier.of(ns, "test_iceberg_view");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(identifier.namespace());
    }

    assertThat(catalog.viewExists(identifier)).isFalse();
    assertThat(catalog.listViews(ns)).isEmpty();

    String hiveViewName = "test_hive_view";
    // create a hive table
    Table hiveTable =
        createHiveView(
            hiveViewName, dbName, Files.createTempDirectory("hive-view-tests-list").toString());
    HIVE_METASTORE_EXTENSION.metastoreClient().createTable(hiveTable);

    catalog.setListAllTables(true);

    assertThat(catalog.listTables(ns))
        .containsExactly(TableIdentifier.of(ns, hiveViewName))
        .hasSize(1);

    assertThat(catalog.listViews(ns)).hasSize(0);

    catalog
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(ns)
        .withQuery("hive", "select * from hivedb.tbl")
        .create();
    assertThat(catalog.viewExists(identifier)).isTrue();

    assertThat(catalog.listViews(ns)).containsExactly(identifier).hasSize(1);
  }

  @Test
  public void testViewWithHiveParameters() throws TException, IOException {
    String dbName = "hivedb";
    Namespace ns = Namespace.of(dbName);
    TableIdentifier identifier = TableIdentifier.of(ns, "test_iceberg_view");

    if (requiresNamespaceCreate()) {
      catalog.createNamespace(identifier.namespace());
    }

    assertThat(catalog.viewExists(identifier)).isFalse();
    String tableQuery = "select * from hivedb.tbl";

    catalog
        .buildView(identifier)
        .withSchema(SCHEMA)
        .withDefaultNamespace(ns)
        .withQuery("hive", tableQuery)
        .create();
    assertThat(catalog.viewExists(identifier)).isTrue();

    Table hiveTable =
        HIVE_METASTORE_EXTENSION.metastoreClient().getTable(dbName, identifier.name());
    assertThat(hiveTable.getViewOriginalText()).isEqualTo(tableQuery);
    assertThat(hiveTable.getViewExpandedText()).isEqualTo(tableQuery);
  }

  private Table createHiveView(String hiveViewName, String dbName, String location) {
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

    Table hiveTable =
        new Table(
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
