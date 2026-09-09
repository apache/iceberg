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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.flink.sink.dynamic.DynamicTableRecordGenerator;
import org.apache.iceberg.flink.sink.dynamic.TableCreator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSink.class);

  private final TableLoader tableLoader;
  private final CatalogLoader catalogLoader;

  @SuppressWarnings("deprecation")
  @Deprecated
  private final TableSchema tableSchema;

  private final ResolvedSchema resolvedSchema;
  private final ReadableConfig readableConfig;
  private final Map<String, String> writeProps;
  private final String dynamicRecordGeneratorImpl;
  private boolean overwrite = false;
  private boolean useDynamicSink = false;

  private IcebergTableSink(IcebergTableSink toCopy) {
    this.tableLoader = toCopy.tableLoader;
    this.catalogLoader = toCopy.catalogLoader;
    this.tableSchema = toCopy.tableSchema;
    this.resolvedSchema = toCopy.resolvedSchema;
    this.overwrite = toCopy.overwrite;
    this.readableConfig = toCopy.readableConfig;
    this.writeProps = toCopy.writeProps;
    this.dynamicRecordGeneratorImpl = toCopy.dynamicRecordGeneratorImpl;
    this.useDynamicSink = toCopy.useDynamicSink;
  }

  /**
   * @deprecated since 1.10.0, will be removed in 2.0.0. Use {@link #IcebergTableSink(TableLoader,
   *     ResolvedSchema, ReadableConfig, Map)} instead
   */
  @Deprecated
  public IcebergTableSink(
      TableLoader tableLoader,
      TableSchema tableSchema,
      ReadableConfig readableConfig,
      Map<String, String> writeProps) {
    this.tableLoader = tableLoader;
    this.catalogLoader = null;
    this.tableSchema = tableSchema;
    this.resolvedSchema = null;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
    this.dynamicRecordGeneratorImpl = null;
  }

  public IcebergTableSink(
      TableLoader tableLoader,
      ResolvedSchema resolvedSchema,
      ReadableConfig readableConfig,
      Map<String, String> writeProps) {
    this.tableLoader = tableLoader;
    this.catalogLoader = null;
    this.tableSchema = null;
    this.resolvedSchema = resolvedSchema;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
    this.dynamicRecordGeneratorImpl = null;
  }

  public IcebergTableSink(
      CatalogLoader catalogLoader,
      String dynamicRecordGeneratorImpl,
      ResolvedSchema resolvedSchema,
      ReadableConfig readableConfig,
      Map<String, String> writeProps) {
    this.tableLoader = null;
    this.catalogLoader = catalogLoader;
    this.dynamicRecordGeneratorImpl = dynamicRecordGeneratorImpl;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
    this.tableSchema = null;
    this.resolvedSchema = resolvedSchema;
    this.useDynamicSink = true;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    Preconditions.checkState(
        !overwrite || context.isBounded(),
        "Unbounded data stream doesn't support overwrite operation.");

    if (canProvideSinkV2()) {
      IcebergSink sink = buildIcebergSink();
      Integer parallelism = sink.writeParallelism();
      return parallelism != null ? SinkV2Provider.of(sink, parallelism) : SinkV2Provider.of(sink);
    }

    return (DataStreamSinkProvider)
        (providerContext, dataStream) -> {
          if (useDynamicSink) {
            return createDynamicIcebergSink(dataStream);
          }

          ResolvedSchema physicalColumnsOnlySchema = physicalColumnsOnlySchema();
          List<String> equalityColumns = equalityColumns(physicalColumnsOnlySchema);
          if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
            return createIcebergSink(dataStream, equalityColumns, physicalColumnsOnlySchema);
          }

          return createLegacySink(dataStream, equalityColumns, physicalColumnsOnlySchema);
        };
  }

  /**
   * Whether the sink can be exposed as a {@link SinkV2Provider}, which is what it takes to report
   * sink lineage: the planner reads a FLIP-314 vertex off the {@code Sink} object (see {@code
   * CommonExecSink}), whereas a {@code DataStreamSinkProvider} only hands it a built
   * transformation. Requires {@link IcebergSink}, the only sink that reports lineage.
   *
   * <p>Also requires {@code TABLE_EXEC_UID_GENERATION=ALWAYS}. {@link IcebergSink}'s custom commit
   * topology puts explicit uids on its operators, so Flink demands one on the sink transformation
   * too ({@code SinkTransformationTranslator.SinkExpander}). The planner only sets it under {@code
   * ALWAYS} — under the default {@code PLAN_ONLY} only for a compiled plan, which a connector
   * cannot detect — so taking this path otherwise would fail job submission outright.
   */
  private boolean canProvideSinkV2() {
    if (useDynamicSink || !readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
      return false;
    }

    ExecutionConfigOptions.UidGeneration uidGeneration =
        readableConfig.get(ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION);
    if (uidGeneration != ExecutionConfigOptions.UidGeneration.ALWAYS) {
      LOG.info(
          "Writing without sink lineage: {} is {}, and IcebergSink can only be exposed as a "
              + "SinkV2Provider when it is ALWAYS.",
          ExecutionConfigOptions.TABLE_EXEC_UID_GENERATION.key(),
          uidGeneration);
      return false;
    }

    return true;
  }

  /**
   * The physical columns of {@link #resolvedSchema}, or null when this sink was built from the
   * deprecated {@code tableSchema} instead.
   */
  private ResolvedSchema physicalColumnsOnlySchema() {
    if (resolvedSchema == null) {
      return null;
    }
    return ResolvedSchema.of(
        resolvedSchema.getColumns().stream()
            .filter(Column::isPhysical)
            .collect(Collectors.toList()));
  }

  /** The primary-key columns used as Iceberg equality-delete fields. */
  @SuppressWarnings("deprecation")
  private List<String> equalityColumns(ResolvedSchema physicalColumnsOnlySchema) {
    if (physicalColumnsOnlySchema != null) {
      return physicalColumnsOnlySchema
          .getPrimaryKey()
          .map(UniqueConstraint::getColumns)
          .orElseGet(ImmutableList::of);
    }
    return tableSchema
        .getPrimaryKey()
        .map(org.apache.flink.table.legacy.api.constraints.UniqueConstraint::getColumns)
        .orElseGet(ImmutableList::of);
  }

  /**
   * The sink with no input stream attached, for the planner to wire up itself. Unlike {@link
   * #createIcebergSink} this builds no topology, which is what lets the planner read the sink's
   * lineage vertex. See {@link #canProvideSinkV2()}.
   */
  private IcebergSink buildIcebergSink() {
    ResolvedSchema physicalColumnsOnlySchema = physicalColumnsOnlySchema();
    return icebergSinkBuilder(
            IcebergSink.builder(),
            equalityColumns(physicalColumnsOnlySchema),
            physicalColumnsOnlySchema)
        .build();
  }

  /** Appends the {@link IcebergSink} operators to {@code dataStream}. */
  private DataStreamSink<?> createIcebergSink(
      DataStream<RowData> dataStream,
      List<String> equalityColumns,
      ResolvedSchema physicalColumnsOnlySchema) {
    return icebergSinkBuilder(
            IcebergSink.forRowData(dataStream), equalityColumns, physicalColumnsOnlySchema)
        .append();
  }

  /** The configuration both {@link IcebergSink} paths share. */
  @SuppressWarnings("deprecation")
  private IcebergSink.Builder icebergSinkBuilder(
      IcebergSink.Builder sinkBuilder,
      List<String> equalityColumns,
      ResolvedSchema physicalColumnsOnlySchema) {
    IcebergSink.Builder builder =
        sinkBuilder
            .tableLoader(tableLoader)
            .equalityFieldColumns(equalityColumns)
            .overwrite(overwrite)
            .setAll(writeProps)
            .flinkConf(readableConfig);

    if (physicalColumnsOnlySchema != null) {
      return builder.resolvedSchema(physicalColumnsOnlySchema);
    }

    return builder.tableSchema(tableSchema);
  }

  @Override
  public void applyStaticPartition(Map<String, String> partition) {
    // The flink's PartitionFanoutWriter will handle the static partition write policy
    // automatically.
  }

  @Override
  public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    ChangelogMode.Builder builder = ChangelogMode.newBuilder();
    for (RowKind kind : requestedMode.getContainedKinds()) {
      builder.addContainedKind(kind);
    }
    return builder.build();
  }

  @Override
  public DynamicTableSink copy() {
    return new IcebergTableSink(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table sink";
  }

  @Override
  public void applyOverwrite(boolean newOverwrite) {
    this.overwrite = newOverwrite;
  }

  private DataStreamSink<?> createLegacySink(
      DataStream<RowData> dataStream,
      List<String> equalityColumns,
      ResolvedSchema physicalColumnsOnlySchema) {
    FlinkSink.Builder builder =
        FlinkSink.forRowData(dataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(equalityColumns)
            .overwrite(overwrite)
            .setAll(writeProps)
            .flinkConf(readableConfig);

    if (physicalColumnsOnlySchema != null) {
      builder = builder.resolvedSchema(physicalColumnsOnlySchema);
    } else {
      builder = builder.tableSchema(tableSchema);
    }

    return builder.append();
  }

  private DataStreamSink<?> createDynamicIcebergSink(DataStream<RowData> dataStream) {
    Preconditions.checkArgument(
        catalogLoader != null && dynamicRecordGeneratorImpl != null,
        "Invalid value catalogLoader: %s, DynamicRecordGenerator Implementation class: %s. "
            + "Both should be not null to use dynamic iceberg sink.",
        catalogLoader,
        dynamicRecordGeneratorImpl);

    TableCreator tableCreator = createTableCreator();
    DynamicRecordGenerator<RowData> generator =
        createDynamicRecordGenerator(dynamicRecordGeneratorImpl);

    DynamicIcebergSink.Builder<RowData> builder =
        DynamicIcebergSink.forInput(dataStream)
            .generator(generator)
            .catalogLoader(catalogLoader)
            .setAll(writeProps)
            .tableCreator(tableCreator)
            .flinkConf(readableConfig);

    return builder.append();
  }

  private TableCreator createTableCreator() {
    final Map<String, String> tableProperties =
        PropertyUtil.propertiesWithPrefix(writeProps, "table.props.");
    final String location = writeProps.get("location");

    return (catalog, identifier, schema, spec) ->
        catalog
            .buildTable(identifier, schema)
            .withPartitionSpec(spec)
            .withLocation(location)
            .withProperties(tableProperties)
            .create();
  }

  private DynamicTableRecordGenerator createDynamicRecordGenerator(String generatorImpl) {
    RowType rowType = (RowType) resolvedSchema.toSourceRowDataType().getLogicalType();

    DynConstructors.Ctor<DynamicTableRecordGenerator> ctor;

    try {
      ctor =
          DynConstructors.builder(DynamicTableRecordGenerator.class)
              .loader(IcebergTableSink.class.getClassLoader())
              .impl(generatorImpl, RowType.class)
              .impl(generatorImpl, RowType.class, Map.class, Configuration.class)
              .buildChecked();
      return ctor.newInstance(rowType, writeProps, fromReadableConfig());
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Class %s does not implement DynamicRecordGeneratorSQL", generatorImpl), e);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to instantiate DynamicRecordGeneratorSQL %s", generatorImpl), e);
    }
  }

  private Configuration fromReadableConfig() {
    return readableConfig instanceof Configuration
        ? (Configuration) readableConfig
        : Configuration.fromMap(readableConfig.toMap());
  }
}
