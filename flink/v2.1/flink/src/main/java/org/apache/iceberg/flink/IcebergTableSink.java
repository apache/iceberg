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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
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

public class IcebergTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
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

  @SuppressWarnings("deprecation")
  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    Preconditions.checkState(
        !overwrite || context.isBounded(),
        "Unbounded data stream doesn't support overwrite operation.");

    return (DataStreamSinkProvider)
        (providerContext, dataStream) -> {
          if (useDynamicSink) {
            return createDynamicIcebergSink(dataStream);
          }

          ResolvedSchema physicalColumnsOnlySchema = null;
          List<String> equalityColumns;
          if (resolvedSchema != null) {
            physicalColumnsOnlySchema =
                ResolvedSchema.of(
                    resolvedSchema.getColumns().stream()
                        .filter(Column::isPhysical)
                        .collect(Collectors.toList()));

            equalityColumns =
                physicalColumnsOnlySchema
                    .getPrimaryKey()
                    .map(UniqueConstraint::getColumns)
                    .orElseGet(ImmutableList::of);

          } else {
            equalityColumns =
                tableSchema
                    .getPrimaryKey()
                    .map(org.apache.flink.table.legacy.api.constraints.UniqueConstraint::getColumns)
                    .orElseGet(ImmutableList::of);
          }
          if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
            return createIcebergSink(dataStream, equalityColumns, physicalColumnsOnlySchema);
          } else {
            return createLegacySink(dataStream, equalityColumns, physicalColumnsOnlySchema);
          }
        };
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
      DataStream<RowData> dataStream, List<String> equalityColumns, ResolvedSchema resolvedSchema) {
    FlinkSink.Builder builder =
        FlinkSink.forRowData(dataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(equalityColumns)
            .overwrite(overwrite)
            .setAll(writeProps)
            .flinkConf(readableConfig);

    if (resolvedSchema != null) {
      builder = builder.resolvedSchema(resolvedSchema);
    } else {
      builder = builder.tableSchema(tableSchema);
    }

    return builder.append();
  }

  private DataStreamSink<?> createIcebergSink(
      DataStream<RowData> dataStream, List<String> equalityColumns, ResolvedSchema resolvedSchema) {
    IcebergSink.Builder builder =
        IcebergSink.forRowData(dataStream)
            .tableLoader(tableLoader)
            .equalityFieldColumns(equalityColumns)
            .overwrite(overwrite)
            .setAll(writeProps)
            .flinkConf(readableConfig);

    if (resolvedSchema != null) {
      builder = builder.resolvedSchema(resolvedSchema);
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
              .buildChecked();
      return ctor.newInstance(rowType);
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Class %s does not implement DynamicRecordGeneratorSQL", generatorImpl), e);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to instantiate DynamicRecordGeneratorSQL %s", generatorImpl), e);
    }
  }
}
