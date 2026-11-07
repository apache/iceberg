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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynFields;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestFlinkDynamicTableFactory {
  private static final ResolvedSchema SCHEMA =
      ResolvedSchema.of(Column.physical("id", DataTypes.BIGINT()));
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("db", "tbl");

  @TempDir private Path temp;
  private Map<String, String> properties;

  @BeforeEach
  void before() {
    TrackingCatalog.INSTANCES.clear();
    properties = Maps.newHashMap();
    properties.put(CatalogProperties.CATALOG_IMPL, TrackingCatalog.class.getName());
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, temp.toUri().toString());
    properties.put(CatalogProperties.CACHE_ENABLED, "false");
    properties.put(FlinkCreateTableOptions.CATALOG_NAME.key(), "catalog");
  }

  @AfterEach
  void after() throws IOException {
    for (TrackingCatalog catalog : TrackingCatalog.INSTANCES) {
      catalog.failClose = false;
      if (catalog.closeCount == 0) {
        catalog.close();
      }
    }

    TrackingCatalog.INSTANCES.clear();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void closesTemporaryCatalog(boolean source) throws IOException {
    Object connector = createConnector(new FlinkDynamicTableFactory(), source);
    assertThat(TrackingCatalog.INSTANCES).hasSize(1);
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);

    try (TableLoader loader = tableLoader(connector, source)) {
      loader.open();
      assertThat(loader.loadTable().schema().findType("id")).isEqualTo(Types.LongType.get());
      assertThat(TrackingCatalog.INSTANCES).hasSize(2);
      assertThat(TrackingCatalog.INSTANCES.get(1).closeCount).isZero();
    }
  }

  @Test
  void closesTemporaryCatalogForDynamicSink() throws IOException {
    properties.put(FlinkCreateTableOptions.USE_DYNAMIC_ICEBERG_SINK.key(), "true");
    properties.put(FlinkCreateTableOptions.DYNAMIC_RECORD_GENERATOR_IMPL.key(), "unused");
    Object sink = createConnector(new FlinkDynamicTableFactory(), false);
    assertThat(TrackingCatalog.INSTANCES).hasSize(1);
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
    CatalogLoader loader =
        DynFields.builder()
            .hiddenImpl(IcebergTableSink.class, "catalogLoader")
            .<CatalogLoader>build(sink)
            .get();
    try (TrackingCatalog catalog = (TrackingCatalog) loader.loadCatalog()) {
      assertThat(catalog.closeCount).isZero();
    }
  }

  @Test
  void closesTemporaryCatalogForExistingTable() throws IOException {
    try (HadoopCatalog catalog =
        new HadoopCatalog(FlinkCatalogFactory.clusterHadoopConf(), temp.toUri().toString())) {
      catalog.createTable(IDENTIFIER, FlinkSchemaUtil.convert(SCHEMA));
    }

    createConnector(new FlinkDynamicTableFactory(), false);
    assertThat(TrackingCatalog.INSTANCES).hasSize(1);
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(strings = {"namespace", "table"})
  void closesTemporaryCatalogOnFailure(String operation) {
    properties.put("fail-operation", operation);
    assertThatThrownBy(() -> createConnector(new FlinkDynamicTableFactory(), false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Catalog failure: " + operation);
    assertThat(TrackingCatalog.INSTANCES).hasSize(1);
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
  }

  @Test
  void preservesFailureWhenCloseFails() {
    properties.put("fail-operation", "table");
    properties.put("fail-close", "true");
    assertThatThrownBy(() -> createConnector(new FlinkDynamicTableFactory(), false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Catalog failure: table")
        .satisfies(
            failure -> {
              assertThat(failure.getSuppressed()).hasSize(1);
              assertThat(failure.getSuppressed()[0])
                  .isInstanceOf(CatalogException.class)
                  .hasCauseInstanceOf(IOException.class)
                  .hasRootCauseMessage("Cannot close catalog");
            });
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
  }

  @Test
  void propagatesCloseFailure() {
    properties.put("fail-close", "true");
    assertThatThrownBy(() -> createConnector(new FlinkDynamicTableFactory(), false))
        .isInstanceOf(CatalogException.class)
        .hasMessage("java.io.IOException: Cannot close catalog")
        .hasCauseInstanceOf(IOException.class)
        .hasRootCauseMessage("Cannot close catalog");
    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void leavesCallerCatalogOpen(boolean source) {
    FlinkCatalog catalog =
        (FlinkCatalog)
            new FlinkCatalogFactory()
                .createCatalog("catalog", properties, FlinkCatalogFactory.clusterHadoopConf());
    try {
      createConnector(new FlinkDynamicTableFactory(catalog), source);
      assertThat(TrackingCatalog.INSTANCES).hasSize(1);
      assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isZero();
    } finally {
      catalog.close();
    }

    assertThat(TrackingCatalog.INSTANCES.get(0).closeCount).isEqualTo(1);
  }

  private Object createConnector(FlinkDynamicTableFactory factory, boolean source) {
    CatalogTable table =
        CatalogTable.newBuilder()
            .schema(Schema.newBuilder().fromResolvedSchema(SCHEMA).build())
            .options(properties)
            .build();
    DynamicTableFactory.Context context = mock(DynamicTableFactory.Context.class);
    when(context.getObjectIdentifier()).thenReturn(ObjectIdentifier.of("catalog", "db", "tbl"));
    when(context.getCatalogTable()).thenReturn(new ResolvedCatalogTable(table, SCHEMA));
    when(context.getConfiguration()).thenReturn(new Configuration());
    return source
        ? factory.createDynamicTableSource(context)
        : factory.createDynamicTableSink(context);
  }

  private TableLoader tableLoader(Object connector, boolean source) {
    return DynFields.builder()
        .hiddenImpl(connector.getClass(), source ? "loader" : "tableLoader")
        .<TableLoader>build(connector)
        .get();
  }

  public static class TrackingCatalog extends HadoopCatalog {
    private static final List<TrackingCatalog> INSTANCES = Lists.newArrayList();
    private int closeCount;
    private String failOperation;
    private boolean failClose;

    @Override
    public void initialize(String name, Map<String, String> props) {
      super.initialize(name, props);
      this.failOperation = props.get("fail-operation");
      this.failClose = Boolean.parseBoolean(props.get("fail-close"));
      INSTANCES.add(this);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
      if ("namespace".equals(failOperation)) {
        throw new IllegalStateException("Catalog failure: namespace");
      }

      super.createNamespace(namespace, metadata);
    }

    @Override
    public Table loadTable(TableIdentifier identifier) {
      if ("table".equals(failOperation)) {
        throw new IllegalStateException("Catalog failure: table");
      }

      assertThat(closeCount).as("Catalog must be open when loading a table").isZero();
      return super.loadTable(identifier);
    }

    @Override
    public void close() throws IOException {
      closeCount += 1;
      super.close();
      if (failClose) {
        throw new IOException("Cannot close catalog");
      }
    }
  }
}
