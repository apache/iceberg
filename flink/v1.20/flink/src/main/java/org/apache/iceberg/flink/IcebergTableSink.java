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
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class IcebergTableSink implements DynamicTableSink, SupportsPartitioning, SupportsOverwrite {
  private final TableLoader tableLoader;
  @Deprecated private final TableSchema tableSchema;
  private final ResolvedSchema resolvedSchema;
  private final ReadableConfig readableConfig;
  private final Map<String, String> writeProps;

  private boolean overwrite = false;

  private IcebergTableSink(IcebergTableSink toCopy) {
    this.tableLoader = toCopy.tableLoader;
    this.tableSchema = toCopy.tableSchema;
    this.resolvedSchema = toCopy.resolvedSchema;
    this.overwrite = toCopy.overwrite;
    this.readableConfig = toCopy.readableConfig;
    this.writeProps = toCopy.writeProps;
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
    this.tableSchema = tableSchema;
    this.resolvedSchema = null;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
  }

  public IcebergTableSink(
      TableLoader tableLoader,
      ResolvedSchema resolvedSchema,
      ReadableConfig readableConfig,
      Map<String, String> writeProps) {
    this.tableLoader = tableLoader;
    this.tableSchema = null;
    this.resolvedSchema = resolvedSchema;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    Preconditions.checkState(
        !overwrite || context.isBounded(),
        "Unbounded data stream doesn't support overwrite operation.");

    if (resolvedSchema != null) {
      List<String> equalityColumns =
          resolvedSchema
              .getPrimaryKey()
              .map(UniqueConstraint::getColumns)
              .orElseGet(ImmutableList::of);

      return new DataStreamSinkProvider() {
        @Override
        public DataStreamSink<?> consumeDataStream(
            ProviderContext providerContext, DataStream<RowData> dataStream) {
          if (Boolean.TRUE.equals(
              readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK))) {
            return IcebergSink.forRowData(dataStream)
                .tableLoader(tableLoader)
                .resolvedSchema(resolvedSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(overwrite)
                .setAll(writeProps)
                .flinkConf(readableConfig)
                .append();
          } else {
            return FlinkSink.forRowData(dataStream)
                .tableLoader(tableLoader)
                .resolvedSchema(resolvedSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(overwrite)
                .setAll(writeProps)
                .flinkConf(readableConfig)
                .append();
          }
        }
      };
    } else {
      List<String> equalityColumns =
          tableSchema
              .getPrimaryKey()
              .map(org.apache.flink.table.api.constraints.UniqueConstraint::getColumns)
              .orElseGet(ImmutableList::of);

      return new DataStreamSinkProvider() {
        @Override
        public DataStreamSink<?> consumeDataStream(
            ProviderContext providerContext, DataStream<RowData> dataStream) {
          if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_V2_SINK)) {
            return IcebergSink.forRowData(dataStream)
                .tableLoader(tableLoader)
                .tableSchema(tableSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(overwrite)
                .setAll(writeProps)
                .flinkConf(readableConfig)
                .append();
          } else {
            return FlinkSink.forRowData(dataStream)
                .tableLoader(tableLoader)
                .tableSchema(tableSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(overwrite)
                .setAll(writeProps)
                .flinkConf(readableConfig)
                .append();
          }
        }
      };
    }
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
}
