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
package org.apache.iceberg.flink.source;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.IcebergRowLevelModificationScanContext;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Flink Iceberg table source. */
@Internal
public class IcebergTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown,
        SupportsRowLevelModificationScan {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSource.class);

  private int[] projectedFields;
  private Long limit;
  private List<Expression> filters;

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;
  private IcebergRowLevelModificationScanContext rowLevelModificationScanContext;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
    this.rowLevelModificationScanContext = toCopy.rowLevelModificationScanContext;
  }

  public IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, null, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      int[] projectedFields,
      boolean isLimitPushDown,
      Long limit,
      List<Expression> filters,
      ReadableConfig readableConfig) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
  }

  @Override
  public void applyProjection(int[][] projectFields) {
    this.projectedFields = new int[projectFields.length];
    for (int i = 0; i < projectFields.length; i++) {
      Preconditions.checkArgument(
          projectFields[i].length == 1, "Don't support nested projection in iceberg source now.");
      this.projectedFields[i] = projectFields[i][0];
    }
  }

  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    FlinkSource.Builder builder =
        FlinkSource.forRowData()
            .env(execEnv)
            .tableLoader(loader)
            .properties(properties)
            .project(getProjectedSchema())
            .limit(limit)
            .filters(filters)
            .flinkConf(readableConfig);

    if (rowLevelModificationScanContext != null) {
      builder = builder.snapshotId(rowLevelModificationScanContext.snapshotId());
    }
    return builder.build();
  }

  private DataStreamSource<RowData> createFLIP27Stream(StreamExecutionEnvironment env) {
    SplitAssignerType assignerType =
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE);
    IcebergSource.Builder<RowData> builder =
        IcebergSource.forRowData()
            .tableLoader(loader)
            .assignerFactory(assignerType.factory())
            .properties(properties)
            .project(getProjectedSchema())
            .limit(limit)
            .filters(filters)
            .flinkConfig(readableConfig);
    if (rowLevelModificationScanContext != null) {
      builder = builder.useSnapshotId(rowLevelModificationScanContext.snapshotId());
    }
    IcebergSource<RowData> source = builder.build();
    DataStreamSource stream =
        env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            source.name(),
            TypeInformation.of(RowData.class));
    return stream;
  }

  private TableSchema getProjectedSchema() {
    if (projectedFields == null) {
      return schema;
    } else {
      String[] fullNames = schema.getFieldNames();
      DataType[] fullTypes = schema.getFieldDataTypes();
      return TableSchema.builder()
          .fields(
              Arrays.stream(projectedFields).mapToObj(i -> fullNames[i]).toArray(String[]::new),
              Arrays.stream(projectedFields).mapToObj(i -> fullTypes[i]).toArray(DataType[]::new))
          .build();
    }
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isPresent()) {
        expressions.add(icebergExpression.get());
        acceptedFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;

    if (rowLevelModificationScanContext != null) {
      Expression filter = this.filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
      this.filters = ImmutableList.of(rowLevelModificationScanContext.applyFilter(filter));
    }

    return Result.of(acceptedFilters, flinkFilters);
  }

  @Override
  public boolean supportsNestedProjection() {
    // TODO: support nested projection
    return false;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
          ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
        if (readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE)) {
          return createFLIP27Stream(execEnv);
        } else {
          return createDataStream(execEnv);
        }
      }

      @Override
      public boolean isBounded() {
        return FlinkSource.isBounded(properties);
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new IcebergTableSource(this);
  }

  @Override
  public String asSummaryString() {
    return "Iceberg table source";
  }

  @Override
  public RowLevelModificationScanContext applyRowLevelModificationScan(
      SupportsRowLevelModificationScan.RowLevelModificationType rowLevelModificationType,
      @Nullable RowLevelModificationScanContext previousContext) {
    loader.open();
    Table table = loader.loadTable();

    RowLevelOperationMode mode =
        rowLevelOperationMode(table, rowLevelModificationType, readableConfig);
    LOG.info("Applying row level modification scan in {} mode.", mode);

    Snapshot currentSnapshot = table.currentSnapshot();
    Long rowLevelModificationScanSnapshotId =
        currentSnapshot != null ? currentSnapshot.snapshotId() : null;

    if (mode == RowLevelOperationMode.COPY_ON_WRITE) {
      this.rowLevelModificationScanContext =
          IcebergRowLevelModificationScanContext.of(
              rowLevelModificationType, mode, rowLevelModificationScanSnapshotId, table.spec());
    } else {
      throw new UnsupportedOperationException("Unsupported row-level operation mode: " + mode);
    }

    return rowLevelModificationScanContext;
  }

  private static RowLevelOperationMode rowLevelOperationMode(
      Table table,
      SupportsRowLevelModificationScan.RowLevelModificationType rowLevelModificationType,
      ReadableConfig readableConfig) {
    String modeStr;
    switch (rowLevelModificationType) {
      case UPDATE:
        modeStr =
            PropertyUtil.propertyAsString(
                table.properties(),
                TableProperties.UPDATE_MODE,
                TableProperties.UPDATE_MODE_DEFAULT);
        break;
      case DELETE:
        modeStr =
            PropertyUtil.propertyAsString(
                table.properties(),
                TableProperties.DELETE_MODE,
                TableProperties.DELETE_MODE_DEFAULT);
        break;
      default:
        throw new UnsupportedOperationException(
            "Unsupported row level modification type: " + rowLevelModificationType);
    }

    String flinkConfigModeStr =
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_MODIFICATION_MODE);
    if (flinkConfigModeStr != null) {
      modeStr = flinkConfigModeStr;
    }

    RowLevelOperationMode modificationMode = RowLevelOperationMode.fromName(modeStr);

    if (table.schema().identifierFieldIds().isEmpty()) {
      Preconditions.checkArgument(
          modificationMode != RowLevelOperationMode.MERGE_ON_READ,
          "MOR is not supported for a table without identifier fields: %s.",
          table.name());
    }

    return modificationMode;
  }
}
