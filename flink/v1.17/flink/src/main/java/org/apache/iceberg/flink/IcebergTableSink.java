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
import java.util.Optional;
import java.util.Set;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergTableSink
    implements DynamicTableSink,
        SupportsPartitioning,
        SupportsOverwrite,
        SupportsRowLevelUpdate,
        SupportsRowLevelDelete {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSink.class);
  private final TableLoader tableLoader;
  private final TableSchema tableSchema;
  private final ReadableConfig readableConfig;
  private final Map<String, String> writeProps;

  private boolean overwrite = false;
  private boolean forceOverwrite = false;
  private boolean forceUpsert = false;
  private boolean forRowLevelUpdate = false;
  private boolean forRowLevelDelete = false;
  private IcebergRowLevelModificationScanContext rowLevelModificationContext;

  private IcebergTableSink(IcebergTableSink toCopy) {
    this.tableLoader = toCopy.tableLoader;
    this.tableSchema = toCopy.tableSchema;
    this.overwrite = toCopy.overwrite;
    this.readableConfig = toCopy.readableConfig;
    this.writeProps = toCopy.writeProps;
  }

  public IcebergTableSink(
      TableLoader tableLoader,
      TableSchema tableSchema,
      ReadableConfig readableConfig,
      Map<String, String> writeProps) {
    this.tableLoader = tableLoader;
    this.tableSchema = tableSchema;
    this.readableConfig = readableConfig;
    this.writeProps = writeProps;
  }

  @Override
  public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    if (forRowLevelUpdate || forRowLevelDelete) {
      switch (rowLevelModificationContext.mode()) {
        case COPY_ON_WRITE:
          forceOverwrite = true;
          LOG.info("Modification mode is COW, enforcing overwrite mode.");
          break;
        case MERGE_ON_READ:
          forceUpsert = true;
          LOG.info("Modification mode is MOR, enforcing upsert mode.");
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported modification mode: " + rowLevelModificationContext.mode());
      }
    }

    Preconditions.checkState(
        !overwrite || forceOverwrite || context.isBounded(),
        "Unbounded data stream doesn't support overwrite operation.");

    List<String> equalityColumns =
        tableSchema.getPrimaryKey().map(UniqueConstraint::getColumns).orElseGet(ImmutableList::of);

    return new DataStreamSinkProvider() {
      @Override
      public DataStreamSink<?> consumeDataStream(
          ProviderContext providerContext, DataStream<RowData> dataStream) {
        FlinkSink.Builder builder =
            FlinkSink.forRowData(dataStream)
                .tableLoader(tableLoader)
                .tableSchema(tableSchema)
                .equalityFieldColumns(equalityColumns)
                .overwrite(overwrite)
                .setAll(writeProps)
                .flinkConf(readableConfig)
                .rowLevelModificationScanContext(rowLevelModificationContext);

        if (forceOverwrite) {
          builder = builder.overwrite(true);
        }

        return builder.append();
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

  @Override
  public SupportsRowLevelUpdate.RowLevelUpdateInfo applyRowLevelUpdate(
      List<Column> updatedColumns, RowLevelModificationScanContext context) {
    Preconditions.checkNotNull(context, "RowLevelModificationScanContext should not be null");
    // TODO: org.apache.iceberg.BaseRowDelta.conflictDetectionFilter
    rowLevelModificationContext = (IcebergRowLevelModificationScanContext) context;

    tableLoader.open();
    Table table = tableLoader.loadTable();
    forRowLevelUpdate = true;

    Set<String> identifierFieldNames = table.schema().identifierFieldNames();
    for (Column column : updatedColumns) {
      if (identifierFieldNames.contains(column.getName())) {
        throw new UnsupportedOperationException(
            String.format("Cannot update identifier field %s", column.getName()));
      }
    }

    return new SupportsRowLevelUpdate.RowLevelUpdateInfo() {
      @Override
      public Optional<List<Column>> requiredColumns() {
        return SupportsRowLevelUpdate.RowLevelUpdateInfo.super.requiredColumns();
      }

      @Override
      public SupportsRowLevelUpdate.RowLevelUpdateMode getRowLevelUpdateMode() {
        switch (rowLevelModificationContext.mode()) {
          case COPY_ON_WRITE:
            return RowLevelUpdateMode.ALL_ROWS;
          case MERGE_ON_READ:
            return RowLevelUpdateMode.UPDATED_ROWS;
          default:
            throw new UnsupportedOperationException(
                "Unsupported modification mode: " + rowLevelModificationContext.mode());
        }
      }
    };
  }

  @Override
  public SupportsRowLevelDelete.RowLevelDeleteInfo applyRowLevelDelete(
      RowLevelModificationScanContext context) {
    Preconditions.checkNotNull(context, "RowLevelModificationScanContext should not be null");
    rowLevelModificationContext = (IcebergRowLevelModificationScanContext) context;

    forRowLevelDelete = true;
    return new SupportsRowLevelDelete.RowLevelDeleteInfo() {
      @Override
      public Optional<List<Column>> requiredColumns() {
        return SupportsRowLevelDelete.RowLevelDeleteInfo.super.requiredColumns();
      }

      @Override
      public SupportsRowLevelDelete.RowLevelDeleteMode getRowLevelDeleteMode() {
        switch (rowLevelModificationContext.mode()) {
          case COPY_ON_WRITE:
            return RowLevelDeleteMode.REMAINING_ROWS;
          case MERGE_ON_READ:
            return RowLevelDeleteMode.DELETED_ROWS;
          default:
            throw new UnsupportedOperationException(
                "Unsupported modification mode: " + rowLevelModificationContext.mode());
        }
      }
    };
  }
}
