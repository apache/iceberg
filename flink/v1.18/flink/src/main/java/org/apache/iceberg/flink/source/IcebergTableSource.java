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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsPartitioning;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning;
import org.apache.flink.table.connector.source.partitioning.Partitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.FlinkFilters;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SpecTransformToFlinkTransform;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.assigner.SplitAssignerType;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.transforms.PartitionSpecVisitor;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.ThreadPools;

/** Flink Iceberg table source. */
@Internal
public class IcebergTableSource
    implements ScanTableSource,
        SupportsProjectionPushDown,
        SupportsFilterPushDown,
        SupportsLimitPushDown,
        SupportsPartitioning {

  private int[] projectedFields;
  private Long limit;
  private List<Expression> filters;

  private final TableLoader loader;
  private final TableSchema schema;
  private final Map<String, String> properties;
  private final boolean isLimitPushDown;
  private final ReadableConfig readableConfig;

  /** The following section is needed for Storage Partition Join */
  private boolean shouldApplyPartitionedRead;

  private Optional<TransformExpression[]> groupingKeyTransforms;
  private Optional<Set<PartitionSpec>> specs;
  private Optional<Types.StructType> groupingKeyType;
  private Optional<Table> table; // cache table for lazy loading
  private Optional<List<IcebergSourceSplit>> batchSplits; // cache batch splits for lazy loading

  private IcebergTableSource(IcebergTableSource toCopy) {
    this.loader = toCopy.loader;
    this.schema = toCopy.schema;
    this.properties = toCopy.properties;
    this.projectedFields = toCopy.projectedFields;
    this.isLimitPushDown = toCopy.isLimitPushDown;
    this.limit = toCopy.limit;
    this.filters = toCopy.filters;
    this.readableConfig = toCopy.readableConfig;
    this.shouldApplyPartitionedRead = toCopy.shouldApplyPartitionedRead;
    this.groupingKeyTransforms = toCopy.groupingKeyTransforms;
    this.specs = toCopy.specs;
    this.groupingKeyType = toCopy.groupingKeyType;
    this.table = toCopy.table;
    this.batchSplits = toCopy.batchSplits;
  }

  public IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      ReadableConfig readableConfig) {
    this(loader, schema, properties, null, false, null, ImmutableList.of(), readableConfig, false);
  }

  private IcebergTableSource(
      TableLoader loader,
      TableSchema schema,
      Map<String, String> properties,
      int[] projectedFields,
      boolean isLimitPushDown,
      Long limit,
      List<Expression> filters,
      ReadableConfig readableConfig,
      boolean shouldApplyPartitionedRead) {
    this.loader = loader;
    this.schema = schema;
    this.properties = properties;
    this.projectedFields = projectedFields;
    this.isLimitPushDown = isLimitPushDown;
    this.limit = limit;
    this.filters = filters;
    this.readableConfig = readableConfig;
    this.shouldApplyPartitionedRead = shouldApplyPartitionedRead;
    this.table = Optional.empty();
    this.groupingKeyType = Optional.empty();
    this.specs = Optional.empty();
    this.groupingKeyTransforms = Optional.empty();
    this.batchSplits = Optional.empty();
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
    return FlinkSource.forRowData()
        .env(execEnv)
        .tableLoader(loader)
        .properties(properties)
        .project(getProjectedSchema())
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
        .properties(properties)
        .project(getProjectedSchema())
        .limit(limit)
        .filters(filters)
        .flinkConfig(readableConfig)
        // TODO -- one future optimization is to call .table(table.orElse(null)) to prevent double
        // loading
        // when SPJ is used. Not adding now, due to production risks (i.e, unknown side effects)
        .applyPartitionedRead(shouldApplyPartitionedRead)
        .batchSplits(batchSplits.orElse(null))
        .buildStream(env);
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
  public Partitioning outputPartitioning() {
    if (groupingKeyType().fields().isEmpty()) {
      // TODO -- discuss with Lu on what we plan to return in this case. Spark returns
      // `UnknownPartitioning(taskGroups().size());`
      return null;
    } else {
      List<FileScanTask> fileTasks = tasks();
      Set<StructLike> uniquePartitions =
          fileTasks.stream().map(task -> task.file().partition()).collect(Collectors.toSet());
      // Convert StructLike partitions to Flink Row objects with proper ordering
      Row[] partitionValues = discoverPartitionValues(uniquePartitions);
      return new KeyGroupedPartitioning(
          groupingKeyTransforms(), partitionValues, partitionValues.length);
    }
  }

  private List<FileScanTask> tasks() {
    if (batchSplits.isPresent()) {
      return batchSplits.get().stream()
          .flatMap(split -> split.task().tasks().stream())
          .collect(Collectors.toList());
    }
    org.apache.iceberg.flink.source.ScanContext.Builder contextBuilder =
        org.apache.iceberg.flink.source.ScanContext.builder();
    Preconditions.checkArgument(getTable().isPresent(), "Table must be defined");
    contextBuilder.resolveConfig(getTable().get(), properties, readableConfig);
    contextBuilder.filters(filters);
    Schema icebergSchema = getTable().get().schema();
    // TODO -- this is called twice now, we may want to cache in the future
    // Taken from Build in IcebergSource
    TableSchema projectedFlinkSchema = getProjectedSchema();
    if (projectedFlinkSchema != null) {
      contextBuilder.project(FlinkSchemaUtil.convert(icebergSchema, projectedFlinkSchema));
    }
    org.apache.iceberg.flink.source.ScanContext context = contextBuilder.build();
    Preconditions.checkArgument(
        !context.isStreaming(), "partition-awareness is only available in batch mode");
    ExecutorService workerPool =
        ThreadPools.newWorkerPool("IcebergTableSource", context.planParallelism());
    List<FileScanTask> fileTasks = null;
    try {
      try {
        this.batchSplits =
            Optional.of(
                FlinkSplitPlanner.planIcebergPartitionAwareSourceSplits(
                    getTable().get(), context, workerPool));
        fileTasks =
            batchSplits.get().stream()
                .flatMap(split -> split.task().tasks().stream())
                .collect(Collectors.toList());
      } catch (Exception e) {
        throw new RuntimeException("Failed to get batch splits: ", e);
      }
    } finally {
      workerPool.shutdown();
    }
    return fileTasks;
  }

  private Row[] discoverPartitionValues(Set<StructLike> uniquePartitions) {
    // TODO -- determine whether this is needed
    if (uniquePartitions.isEmpty()) {
      return new Row[0];
    }
    Types.StructType partitionGroupingKeyType = groupingKeyType();

    // Sort partitions using Iceberg's built-in comparator to ensure consistent ordering
    Schema groupingSchema = new Schema(partitionGroupingKeyType.fields());
    SortOrder.Builder sortBuilder = SortOrder.builderFor(groupingSchema);

    // Add each field to the sort order for consistent ordering
    for (Types.NestedField field : partitionGroupingKeyType.fields()) {
      sortBuilder.asc(field.name());
    }
    SortOrder sortOrder = sortBuilder.build();

    Comparator<StructLike> comparator = SortOrderComparators.forSchema(groupingSchema, sortOrder);

    List<StructLike> sortedPartitions =
        uniquePartitions.stream().sorted(comparator).collect(Collectors.toList());

    Row[] partitions = new Row[sortedPartitions.size()];
    int index = 0;

    for (StructLike partition : sortedPartitions) {
      Row row = Row.ofKind(RowKind.INSERT, new Object[partitionGroupingKeyType.fields().size()]);
      for (int i = 0; i < partitionGroupingKeyType.fields().size(); i++) {
        Object value = partition.get(i, Object.class);
        row.setField(i, value);
      }
      partitions[index++] = row;
    }
    return partitions;
  }

  @Override
  public void applyPartitionedRead() {
    this.shouldApplyPartitionedRead = true;
  }

  private Set<PartitionSpec> specs() {
    Preconditions.checkArgument(getTable().isPresent(), "Table must be defined");
    return Sets.newHashSet(getTable().get().specs().values());
  }

  private Types.StructType groupingKeyType() {
    if (!groupingKeyType.isPresent()) {
      // TODO -- determine whether schema is needed for Flink / current use-cases, its used
      // on the Spark-Side but given method definition, this should will also work as expected
      this.groupingKeyType =
          Optional.of(org.apache.iceberg.Partitioning.groupingKeyType(null, specs()));
    }
    return groupingKeyType.get();
  }

  // taken directly from SparkPartitioningAwarenessScan and Adapted for Flink
  private TransformExpression[] groupingKeyTransforms() {
    if (!groupingKeyTransforms.isPresent()) {
      Map<Integer, PartitionField> fieldsById = indexFieldsById(specs());

      List<PartitionField> groupingKeyFields =
          groupingKeyType().fields().stream()
              .map(field -> fieldsById.get(field.fieldId()))
              .collect(Collectors.toList());

      Preconditions.checkArgument(getTable().isPresent(), "Table must exist to get table schema");
      // TODO -- handle case where we need the schema for specific snapshot-id or branch as done in
      // spark
      // for now, same as getTable().get().schema() but leaving as is as note for future iterations
      Schema tableSchema = SnapshotUtil.schemaFor(getTable().get(), null);
      List<TransformExpression> transforms = Lists.newArrayList();
      SpecTransformToFlinkTransform visitor = new SpecTransformToFlinkTransform();
      for (PartitionField field : groupingKeyFields) {
        TransformExpression transform = PartitionSpecVisitor.visit(tableSchema, field, visitor);
        if (transform != null) {
          transforms.add(transform);
        }
      }
      this.groupingKeyTransforms = Optional.of(transforms.toArray(new TransformExpression[0]));
    }
    return groupingKeyTransforms.get();
  }

  private Map<Integer, PartitionField> indexFieldsById(Iterable<PartitionSpec> specIterable) {
    Map<Integer, PartitionField> fieldsById = Maps.newHashMap();

    for (PartitionSpec spec : specIterable) {
      for (PartitionField field : spec.fields()) {
        fieldsById.putIfAbsent(field.fieldId(), field);
      }
    }
    return fieldsById;
  }

  private Optional<Table> getTable() {
    if (!table.isPresent()) {
      try {
        this.loader.open();
        this.table = Optional.of(this.loader.loadTable());
        return table;
      } catch (Exception e) {
        throw new RuntimeException("Unable to load Source Table, bug", e);
      }
    }
    return table;
  }
}
