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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.AggregateExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.legacy.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.FlinkReadOptions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.StructRowData;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.AggregatePushDownUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Flink Iceberg table source. */
@Internal
public class IcebergTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown,
        SupportsAggregatePushDown,
        SupportsSourceWatermark {

  private static final Logger LOG = LoggerFactory.getLogger(IcebergTableSource.class);

  private int[] projectedFields;
  private Long limit;
  private List<Expression> filters;
  private AggregateEvaluator pushedAggregate;
  private DataType pushedAggregateProducedDataType;

  private final TableLoader loader;
  private final ResolvedSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;
  private final boolean caseSensitive;

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
    this.caseSensitive = toCopy.caseSensitive;
    this.pushedAggregate = toCopy.pushedAggregate;
    this.pushedAggregateProducedDataType = toCopy.pushedAggregateProducedDataType;
  }

  public IcebergTableSource(
      TableLoader loader,
      ResolvedSchema schema,
      Map<String, String> properties,
      ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, null, ImmutableList.of(), readableConfig);
  }

  private IcebergTableSource(
      TableLoader loader,
      ResolvedSchema schema,
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
    this.caseSensitive =
        PropertyUtil.propertyAsBoolean(
            properties,
            FlinkReadOptions.CASE_SENSITIVE,
            FlinkReadOptions.CASE_SENSITIVE_OPTION.defaultValue());
  }

  @Override
  public void applyProjection(int[][] projectFields, DataType producedDataType) {
    this.projectedFields = new int[projectFields.length];
    for (int i = 0; i < projectFields.length; i++) {
      Preconditions.checkArgument(
          projectFields[i].length == 1, "Don't support nested projection in iceberg source now.");
      this.projectedFields[i] = projectFields[i][0];
    }
  }

  @SuppressWarnings("deprecation")
  private DataStream<RowData> createDataStream(StreamExecutionEnvironment execEnv) {
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .setAll(properties)
        .project(TableSchema.fromResolvedSchema(getProjectedSchema()))
        .limit(limit)
        .filters(filters)
        .flinkConf(readableConfig)
        .build();
  }

  private DataStream<RowData> createFLIP27Stream(StreamExecutionEnvironment env) {
    SplitAssignerType assignerType =
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_SPLIT_ASSIGNER_TYPE);
    return IcebergSource.forRowData()
        .tableLoader(loader)
        .assignerFactory(assignerType.factory())
        .setAll(properties)
        .project(getProjectedSchema())
        .limit(limit)
        .filters(filters)
        .flinkConfig(readableConfig)
        .buildStream(env);
  }

  private ResolvedSchema getProjectedSchema() {
    if (projectedFields == null) {
      return schema;
    } else {
      List<Column> fullColumns = schema.getColumns();
      return ResolvedSchema.of(
          Arrays.stream(projectedFields).mapToObj(fullColumns::get).collect(Collectors.toList()));
    }
  }

  @Override
  public void applyLimit(long newLimit) {
    this.limit = newLimit;
  }

  @Override
  public Result applyFilters(List<ResolvedExpression> flinkFilters) {
    List<ResolvedExpression> acceptedFilters = Lists.newArrayList();
    List<ResolvedExpression> remainingFilters = Lists.newArrayList();
    List<Expression> expressions = Lists.newArrayList();

    Table table = null;

    for (ResolvedExpression resolvedExpression : flinkFilters) {
      Optional<Expression> icebergExpression = FlinkFilters.convert(resolvedExpression);
      if (icebergExpression.isEmpty()) {
        remainingFilters.add(resolvedExpression);
        continue;
      }

      Expression expression = icebergExpression.get();
      expressions.add(expression);
      acceptedFilters.add(resolvedExpression);

      if (table == null) {
        table = loadTable();
      }

      if (ExpressionUtil.selectsPartitions(expression, table, caseSensitive)) {
        LOG.info("Evaluating {} entirely on the Iceberg side", expression);
      } else {
        remainingFilters.add(resolvedExpression);
      }
    }

    this.filters = expressions;
    return Result.of(acceptedFilters, remainingFilters);
  }

  @Override
  public void applySourceWatermark() {
    Preconditions.checkArgument(
        readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE),
        "Source watermarks are supported only in flip-27 iceberg source implementation");

    Preconditions.checkNotNull(
        properties.get(FlinkReadOptions.WATERMARK_COLUMN),
        "watermark-column needs to be configured to use source watermark.");
  }

  @Override
  public boolean applyAggregates(
      List<int[]> groupingSets,
      List<AggregateExpression> aggregateExpressions,
      DataType producedDataType) {
    if (!readableConfig.get(FlinkConfigOptions.TABLE_EXEC_ICEBERG_AGGREGATE_PUSH_DOWN_ENABLED)) {
      LOG.info(
          "Skipping aggregate pushdown: table.exec.iceberg.aggregate-push-down-enabled is not enabled");
      return false;
    }

    if (!isBounded(properties)) {
      LOG.info("Skipping aggregate pushdown: streaming reads are not supported");
      return false;
    }

    if (groupingSets.size() != 1 || groupingSets.get(0).length > 0) {
      LOG.info("Skipping aggregate pushdown: GROUP BY push down is not supported");
      return false;
    }

    if (limit != null) {
      LOG.info("Skipping aggregate pushdown: a limit is present");
      return false;
    }

    List<Expression> icebergAggregates = convertAggregates(aggregateExpressions);
    if (icebergAggregates == null) {
      return false;
    }

    Table table = loadTable();
    if (table instanceof BaseMetadataTable) {
      LOG.info("Skipping aggregate pushdown: metadata tables are not supported");
      return false;
    }

    if (!filtersSelectWholePartitions(table)) {
      LOG.info("Skipping aggregate pushdown: a filter that doesn't select whole partitions");
      return false;
    }

    AggregateEvaluator evaluator = planAggregateEvaluator(table, icebergAggregates);
    if (evaluator == null) {
      return false;
    }

    this.pushedAggregate = evaluator;
    this.pushedAggregateProducedDataType = producedDataType;
    return true;
  }

  private List<Expression> convertAggregates(List<AggregateExpression> aggregateExpressions) {
    List<Expression> icebergAggregates =
        Lists.newArrayListWithExpectedSize(aggregateExpressions.size());
    for (AggregateExpression flinkAggregate : aggregateExpressions) {
      Expression icebergAggregate = FlinkAggregates.convert(flinkAggregate);
      if (icebergAggregate == null) {
        LOG.info("Skipping aggregate pushdown: unsupported aggregate {}", flinkAggregate);
        return null;
      }

      icebergAggregates.add(icebergAggregate);
    }

    return icebergAggregates;
  }

  private AggregateEvaluator planAggregateEvaluator(
      Table table, List<Expression> icebergAggregates) {
    AggregateEvaluator evaluator;
    try {
      evaluator = AggregateEvaluator.create(table.schema(), icebergAggregates);
    } catch (RuntimeException e) {
      LOG.info("Skipping aggregate pushdown: failed to bind aggregate expressions", e);
      return null;
    }

    if (!AggregatePushDownUtil.metricsModeSupportsAggregatePushDown(
        table, evaluator.aggregates())) {
      return null;
    }

    TableScan scan =
        table
            .newScan()
            .caseSensitive(caseSensitive)
            .includeColumnStats()
            .filter(filterExpression());

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      for (FileScanTask task : tasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return null;
        }

        if (task.residual().op() != Expression.Operation.TRUE) {
          LOG.info("Skipping aggregate pushdown: file {} needs row-level filtering", task.file());
          return null;
        }

        evaluator.update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: failed to plan files", e);
      return null;
    }

    if (!evaluator.allAggregatorsValid()) {
      LOG.info("Skipping aggregate pushdown: required metrics are not available for all files");
      return null;
    }

    return evaluator;
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
    if (pushedAggregate != null) {
      return new DataStreamScanProvider() {
        @Override
        public DataStream<RowData> produceDataStream(
            ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
          return createAggregateDataStream(execEnv);
        }

        @Override
        public boolean isBounded() {
          return true;
        }
      };
    }

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
        return IcebergTableSource.isBounded(properties);
      }

      @Override
      public Optional<Integer> getParallelism() {
        return Optional.ofNullable(
            PropertyUtil.propertyAsNullableInt(properties, FactoryUtil.SOURCE_PARALLELISM.key()));
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

  private Table loadTable() {
    try (TableLoader tableLoader = loader.clone()) {
      tableLoader.open();
      return tableLoader.loadTable();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean filtersSelectWholePartitions(Table table) {
    if (filters == null || filters.isEmpty()) {
      return true;
    }

    for (Expression filter : filters) {
      if (!ExpressionUtil.selectsPartitions(filter, table, caseSensitive)) {
        return false;
      }
    }

    return true;
  }

  private Expression filterExpression() {
    if (filters == null) {
      return Expressions.alwaysTrue();
    }

    return filters.stream().reduce(Expressions.alwaysTrue(), Expressions::and);
  }

  private DataStream<RowData> createAggregateDataStream(StreamExecutionEnvironment execEnv) {
    RowData row =
        new StructRowData(pushedAggregate.resultType()).setStruct(pushedAggregate.result());
    RowType rowType = (RowType) pushedAggregateProducedDataType.getLogicalType();
    return execEnv
        .fromData(Collections.singletonList(row), FlinkCompatibilityUtil.toTypeInfo(rowType))
        .setParallelism(1);
  }

  public static boolean isBounded(Map<String, String> properties) {
    return !PropertyUtil.propertyAsBoolean(properties, FlinkReadOptions.STREAMING, false);
  }
}
