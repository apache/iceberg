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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.lineage.DatasetConfigFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.ExecutionConfigOptions.UidGeneration;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class TestIcebergLineageConfig {
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "lineage");
  private static final String SINK_UID = "lineage-sink";
  private static final String NATIVE_PREFIX = "projects/1234/catalogs/native_catalog";

  @RegisterExtension
  private static final HadoopCatalogExtension CATALOG_EXTENSION =
      new HadoopCatalogExtension("db", "lineage");

  private Table table;

  @BeforeEach
  void createTable() {
    this.table = CATALOG_EXTENSION.catalog().createTable(TABLE_IDENTIFIER, SimpleDataUtil.SCHEMA);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(booleans = false)
  void sourcePreservesCallbackWhenLineageIsDisabled(Boolean emitLineage) {
    Configuration config = lineageConfig(emitLineage);
    ScanTableSource.ScanRuntimeProvider provider =
        tableSource(config).getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class));

    assertThat(provider).isInstanceOf(DataStreamScanProvider.class);
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataStream<RowData> stream =
        ((DataStreamScanProvider) provider).produceDataStream(name -> Optional.empty(), env);

    assertThat(stream.getTransformation()).isInstanceOf(SourceTransformation.class);
    assertThat(((SourceTransformation<?, ?, ?>) stream.getTransformation()).getSource())
        .isInstanceOf(IcebergSource.class);
    assertThat(stream.getTransformation().getName()).isEqualTo("IcebergSource-" + table.name());
    assertThat(stream.getTransformation().getUid()).isNull();
    assertThat(stream.getParallelism()).isEqualTo(1);
  }

  @ParameterizedTest
  @EnumSource(UidGeneration.class)
  void sourceUsesDeclarativeProviderWhenLineageIsEnabled(UidGeneration uidGeneration) {
    Configuration config = lineageConfig(true);
    config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, uidGeneration);
    ScanTableSource.ScanRuntimeProvider provider =
        tableSource(config).getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class));

    assertThat(provider).isInstanceOf(SourceProvider.class);
    assertThat(((SourceProvider) provider).createSource()).isInstanceOf(IcebergSource.class);
  }

  @Test
  void legacySourcePreservesCallbackWhenLineageIsDisabled() {
    Configuration config = new Configuration();
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, false);

    assertThat(tableSource(config).getScanRuntimeProvider(mock(ScanTableSource.ScanContext.class)))
        .isInstanceOf(DataStreamScanProvider.class);
  }

  @ParameterizedTest
  @CsvSource({
    ", ALWAYS",
    ", PLAN_ONLY",
    ", DISABLED",
    "false, ALWAYS",
    "false, PLAN_ONLY",
    "false, DISABLED"
  })
  void sinkUsesCallbackWhenLineageIsDisabled(Boolean emitLineage, UidGeneration uidGeneration) {
    Configuration config = lineageConfig(emitLineage);
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true);
    config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, uidGeneration);
    DynamicTableSink.SinkRuntimeProvider provider =
        tableSink(config).getSinkRuntimeProvider(mock(DynamicTableSink.Context.class));

    assertThat(provider).isInstanceOf(DataStreamSinkProvider.class);
  }

  @Test
  void defaultSinkCallbackPreservesUidAndParallelism() {
    Configuration config = new Configuration();
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true);
    config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.ALWAYS);
    DataStreamSinkProvider provider =
        (DataStreamSinkProvider)
            tableSink(config).getSinkRuntimeProvider(mock(DynamicTableSink.Context.class));
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);
    DataStream<RowData> input =
        IcebergSource.forRowData()
            .tableLoader(CATALOG_EXTENSION.tableLoader().clone())
            .buildStream(env);
    DataStreamSink<?> sink = provider.consumeDataStream(name -> Optional.empty(), input);

    assertThat(sink.getTransformation().getUid()).isEqualTo(SINK_UID);
    assertThat(sink.getTransformation().getName()).isEqualTo(SINK_UID);
    assertThat(sink.getTransformation().getParallelism()).isEqualTo(input.getParallelism());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(ints = 4)
  void sinkUsesDeclarativeProviderWhenLineageIsEnabled(Integer writeParallelism) {
    Configuration config = lineageConfig(true);
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true);
    config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, UidGeneration.ALWAYS);
    Map<String, String> writeOptions =
        writeParallelism != null
            ? Map.of(FlinkWriteOptions.WRITE_PARALLELISM.key(), writeParallelism.toString())
            : Map.of();
    IcebergTableSink sink =
        new IcebergTableSink(
            CATALOG_EXTENSION.tableLoader(), SimpleDataUtil.FLINK_SCHEMA, config, writeOptions);
    DynamicTableSink.SinkRuntimeProvider provider =
        sink.getSinkRuntimeProvider(mock(DynamicTableSink.Context.class));

    assertThat(provider).isInstanceOf(SinkV2Provider.class);
    SinkV2Provider sinkProvider = (SinkV2Provider) provider;
    assertThat(sinkProvider.createSink()).isInstanceOf(IcebergSink.class);
    assertThat(sinkProvider.getParallelism()).isEqualTo(Optional.ofNullable(writeParallelism));
  }

  @ParameterizedTest
  @NullSource
  @EnumSource(
      value = UidGeneration.class,
      names = {"PLAN_ONLY", "DISABLED"})
  void sinkFallsBackForUnsupportedUidGenerationWhenLineageIsEnabled(UidGeneration uidGeneration) {
    Configuration config = lineageConfig(true);
    config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK, true);
    if (uidGeneration != null) {
      config.set(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION, uidGeneration);
    }

    assertThat(tableSink(config).getSinkRuntimeProvider(mock(DynamicTableSink.Context.class)))
        .isInstanceOf(DataStreamSinkProvider.class);
  }

  @Test
  void legacySinkPreservesCallbackWhenLineageIsDisabled() {
    assertThat(
            tableSink(new Configuration())
                .getSinkRuntimeProvider(mock(DynamicTableSink.Context.class)))
        .isInstanceOf(DataStreamSinkProvider.class);
  }

  @Test
  void sourceRetainsPrefixAfterClosingLoader() throws IOException {
    RESTCatalog catalog = restCatalog();
    CatalogLoader catalogLoader = catalogLoader(catalog);
    try (TableLoader loader = TableLoader.fromCatalog(catalogLoader, TABLE_IDENTIFIER)) {
      IcebergSource<RowData> source = IcebergSource.forRowData().tableLoader(loader).build();

      assertThat(loader.isOpen()).isFalse();
      assertRetainedPrefix(source);
      verify(catalogLoader).loadCatalog();
      verify(catalogLoader, never()).properties();
      verify(catalog).close();
    }
  }

  @Test
  void sinkRetainsPrefixAfterClosingLoader() throws IOException {
    RESTCatalog catalog = restCatalog();
    CatalogLoader catalogLoader = catalogLoader(catalog);
    try (TableLoader loader = TableLoader.fromCatalog(catalogLoader, TABLE_IDENTIFIER)) {
      IcebergSink sink =
          IcebergSink.builder()
              .tableLoader(loader)
              .resolvedSchema(SimpleDataUtil.FLINK_SCHEMA)
              .build();

      assertThat(loader.isOpen()).isFalse();
      assertRetainedPrefix(sink);
      verify(catalogLoader).loadCatalog();
      verify(catalogLoader, never()).properties();
      verify(catalog).close();
    }
  }

  @Test
  void preloadedSourceCapturesPrefixWithoutClosingCallerLoader() throws IOException {
    RESTCatalog catalog = restCatalog();
    CatalogLoader catalogLoader = catalogLoader(catalog);
    try (TableLoader loader = TableLoader.fromCatalog(catalogLoader, TABLE_IDENTIFIER)) {
      loader.open();
      IcebergSource<RowData> source =
          IcebergSource.forRowData().table(table).tableLoader(loader).build();

      assertRetainedPrefix(source);
      assertThat(loader.isOpen()).isTrue();
      verify(catalogLoader).loadCatalog();
      verify(catalogLoader, never()).properties();
      verify(catalog, never()).loadTable(TABLE_IDENTIFIER);
      verify(catalog, never()).close();
    }
  }

  private RESTCatalog restCatalog() {
    RESTCatalog catalog = mock(RESTCatalog.class);
    when(catalog.properties()).thenReturn(Map.of("prefix", NATIVE_PREFIX));
    when(catalog.loadTable(TABLE_IDENTIFIER)).thenReturn(table);
    return catalog;
  }

  private static CatalogLoader catalogLoader(RESTCatalog catalog) {
    CatalogLoader loader = mock(CatalogLoader.class);
    when(loader.loadCatalog()).thenReturn(catalog);
    return loader;
  }

  private static void assertRetainedPrefix(LineageVertexProvider connector) {
    for (int i = 0; i < 2; i++) {
      List<LineageDataset> datasets = connector.getLineageVertex().datasets();
      assertThat(datasets).hasSize(1);
      DatasetConfigFacet facet = (DatasetConfigFacet) datasets.get(0).facets().get("iceberg");
      assertThat(facet.config()).containsEntry("catalog.prefix", NATIVE_PREFIX);
    }
  }

  private IcebergTableSource tableSource(Configuration config) {
    return new IcebergTableSource(
        CATALOG_EXTENSION.tableLoader(), SimpleDataUtil.FLINK_SCHEMA, Map.of(), config);
  }

  private IcebergTableSink tableSink(Configuration config) {
    return new IcebergTableSink(
        CATALOG_EXTENSION.tableLoader(),
        SimpleDataUtil.FLINK_SCHEMA,
        config,
        Map.of(FlinkWriteOptions.UID_SUFFIX.key(), SINK_UID));
  }

  private static Configuration lineageConfig(Boolean emitLineage) {
    Configuration config = new Configuration();
    if (emitLineage != null) {
      config.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_EMIT_LINEAGE, emitLineage);
    }

    return config;
  }
}
