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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SparkDistributedDataScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.AggregateEvaluator;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundAggregate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkAggregates;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeMap;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc;
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownAggregates;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkScanBuilder extends BaseSparkScanBuilder
    implements SupportsPushDownV2Filters,
        SupportsPushDownRequiredColumns,
        SupportsPushDownLimit,
        SupportsPushDownAggregates {

  private static final Logger LOG = LoggerFactory.getLogger(SparkScanBuilder.class);

  private final Snapshot snapshot;
  private final String branch;
  private final TimeTravel timeTravel;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private Scan localScan;

  SparkScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    this(
        spark,
        table,
        table.schema(),
        table.currentSnapshot(),
        null /* no branch */,
        null /* no time travel */,
        options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      CaseInsensitiveStringMap options) {
    this(spark, table, schema, snapshot, branch, null /* no time travel */, options);
  }

  SparkScanBuilder(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      TimeTravel timeTravel,
      CaseInsensitiveStringMap options) {
    super(spark, table, schema, options);
    this.snapshot = snapshot;
    this.branch = branch;
    this.timeTravel = timeTravel;
    if (Spark3Util.containsIncrementalOptions(options)) {
      Preconditions.checkArgument(timeTravel == null, "Cannot use time travel in incremental scan");
      Pair<Long, Long> boundaries = readConf().incrementalAppendScanBoundaries();
      this.startSnapshotId = boundaries.first();
      this.endSnapshotId = boundaries.second();
    } else {
      this.startSnapshotId = null;
      this.endSnapshotId = null;
    }
    Spark3Util.validateNoLegacyTimeTravel(options);
    SparkTableUtil.validateReadBranch(spark, table, branch, options);
  }

  @Override
  public boolean pushAggregation(Aggregation aggregation) {
    if (!canPushDownAggregation()) {
      return false;
    }

    List<BoundAggregate<?, ?>> boundAggregates = bindAggregates(aggregation);
    if (boundAggregates == null) {
      return false;
    }

    if (!metricsModeSupportsAggregatePushDown(boundAggregates)) {
      return false;
    }

    List<Types.NestedField> groupByFields = resolveGroupByFields(aggregation);
    if (groupByFields == null) {
      return false;
    }

    if (groupByFields.isEmpty()) {
      return pushUngroupedAggregation(boundAggregates);
    } else {
      return pushGroupedAggregation(boundAggregates, groupByFields);
    }
  }

  private List<BoundAggregate<?, ?>> bindAggregates(Aggregation aggregation) {
    List<BoundAggregate<?, ?>> expressions =
        Lists.newArrayListWithExpectedSize(aggregation.aggregateExpressions().length);

    for (AggregateFunc aggregateFunc : aggregation.aggregateExpressions()) {
      try {
        Expression expr = SparkAggregates.convert(aggregateFunc);
        if (expr != null) {
          Expression bound = Binder.bind(projection().asStruct(), expr, caseSensitive());
          expressions.add((BoundAggregate<?, ?>) bound);
        } else {
          LOG.info(
              "Skipping aggregate pushdown: AggregateFunc {} can't be converted to iceberg expression",
              aggregateFunc);
          return null;
        }
      } catch (IllegalArgumentException e) {
        LOG.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", aggregateFunc, e);
        return null;
      }
    }

    return expressions;
  }

  private boolean pushUngroupedAggregation(List<BoundAggregate<?, ?>> boundAggregates) {
    AggregateEvaluator aggregateEvaluator = AggregateEvaluator.create(boundAggregates);

    try (CloseableIterable<FileScanTask> fileScanTasks = planFilesWithStats()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        aggregateEvaluator.update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    if (!aggregateEvaluator.allAggregatorsValid()) {
      return false;
    }

    StructType pushedAggregateSchema =
        SparkSchemaUtil.convert(new Schema(aggregateEvaluator.resultType().fields()));
    InternalRow[] pushedAggregateRows = new InternalRow[1];
    pushedAggregateRows[0] =
        new StructInternalRow(aggregateEvaluator.resultType())
            .setStruct(aggregateEvaluator.result());
    localScan = new SparkLocalScan(table(), pushedAggregateSchema, pushedAggregateRows, filters());

    return true;
  }

  /**
   * Pushes down aggregates that are grouped by identity partition columns. Because every row in a
   * data file shares the same value for an identity partition column, a file's metadata stats
   * belong entirely to a single group, so per-group min/max/count can be computed from manifests
   * without scanning data. Pushdown is skipped (false) if any group-by column is not an identity
   * partition column in the spec of every scanned data file (e.g. partition spec evolution or a
   * non-identity transform).
   */
  private boolean pushGroupedAggregation(
      List<BoundAggregate<?, ?>> boundAggregates, List<Types.NestedField> groupByFields) {
    Types.StructType groupKeyType = groupKeyType(groupByFields);
    StructLikeMap<AggregateEvaluator> evaluatorsByGroup = StructLikeMap.create(groupKeyType);

    try (CloseableIterable<FileScanTask> fileScanTasks = planFilesWithStats()) {
      for (FileScanTask task : fileScanTasks) {
        if (!task.deletes().isEmpty()) {
          LOG.info("Skipping aggregate pushdown: detected row level deletes");
          return false;
        }

        StructLike groupKey = buildGroupKey(task, groupByFields);
        if (groupKey == null) {
          LOG.info(
              "Skipping aggregate pushdown: group by column is not an identity partition "
                  + "in the spec of all data files");
          return false;
        }

        evaluatorsByGroup
            .computeIfAbsent(groupKey, () -> AggregateEvaluator.create(boundAggregates))
            .update(task.file());
      }
    } catch (IOException e) {
      LOG.info("Skipping aggregate pushdown: ", e);
      return false;
    }

    for (AggregateEvaluator evaluator : evaluatorsByGroup.values()) {
      if (!evaluator.allAggregatorsValid()) {
        return false;
      }
    }

    // Spark's SupportsPushDownAggregates contract expects the scan to output the group-by
    // columns first (in the given order) followed by the aggregate expressions (in the given
    // order). Fresh sequential field ids keep the combined schema self-consistent.
    Types.StructType aggregateResultType = AggregateEvaluator.create(boundAggregates).resultType();
    List<Types.NestedField> resultFields =
        Lists.newArrayListWithExpectedSize(
            groupByFields.size() + aggregateResultType.fields().size());
    int fieldId = 0;
    for (Types.NestedField groupByField : groupByFields) {
      resultFields.add(
          Types.NestedField.optional(fieldId++, groupByField.name(), groupByField.type()));
    }
    for (Types.NestedField aggregateField : aggregateResultType.fields()) {
      resultFields.add(
          Types.NestedField.optional(fieldId++, aggregateField.name(), aggregateField.type()));
    }
    Types.StructType resultType = Types.StructType.of(resultFields);
    StructType pushedAggregateSchema = SparkSchemaUtil.convert(new Schema(resultFields));

    List<InternalRow> pushedAggregateRows =
        Lists.newArrayListWithExpectedSize(evaluatorsByGroup.size());
    for (Map.Entry<StructLike, AggregateEvaluator> entry : evaluatorsByGroup.entrySet()) {
      StructLike joined =
          new JoinedStruct(entry.getKey(), groupByFields.size(), entry.getValue().result());
      pushedAggregateRows.add(new StructInternalRow(resultType).setStruct(joined));
    }

    localScan =
        new SparkLocalScan(
            table(),
            pushedAggregateSchema,
            pushedAggregateRows.toArray(new InternalRow[0]),
            filters());

    return true;
  }

  /**
   * Resolves the Spark group-by expressions to table-schema fields. Returns an empty list when
   * there is no group by, or {@code null} when any group-by expression is not a plain column
   * reference (e.g. a transform expression) or does not resolve to a known column, signaling that
   * aggregate pushdown must be skipped.
   */
  private List<Types.NestedField> resolveGroupByFields(Aggregation aggregation) {
    org.apache.spark.sql.connector.expressions.Expression[] groupByExpressions =
        aggregation.groupByExpressions();
    List<Types.NestedField> groupByFields =
        Lists.newArrayListWithExpectedSize(groupByExpressions.length);
    Schema projection = projection();

    for (org.apache.spark.sql.connector.expressions.Expression expr : groupByExpressions) {
      if (!(expr instanceof NamedReference)) {
        LOG.info("Skipping aggregate pushdown: group by expression {} is not a column", expr);
        return null;
      }

      String columnName = SparkUtil.toColumnName((NamedReference) expr);
      Types.NestedField field =
          caseSensitive()
              ? projection.findField(columnName)
              : projection.caseInsensitiveFindField(columnName);
      if (field == null) {
        LOG.info("Skipping aggregate pushdown: group by column {} was not found", columnName);
        return null;
      }

      groupByFields.add(field);
    }

    return groupByFields;
  }

  private Types.StructType groupKeyType(List<Types.NestedField> groupByFields) {
    List<Types.NestedField> keyFields = Lists.newArrayListWithExpectedSize(groupByFields.size());
    int fieldId = 0;
    for (Types.NestedField groupByField : groupByFields) {
      keyFields.add(
          Types.NestedField.optional(fieldId++, groupByField.name(), groupByField.type()));
    }
    return Types.StructType.of(keyFields);
  }

  /**
   * Builds the group key for a file from its partition tuple. Returns {@code null} if any group-by
   * column is not an identity partition field in this file's spec, which forces pushdown to be
   * skipped (correctness over coverage under partition spec evolution).
   */
  private StructLike buildGroupKey(FileScanTask task, List<Types.NestedField> groupByFields) {
    PartitionSpec spec = task.spec();
    StructLike partition = task.partition();
    List<PartitionField> partitionFields = spec.fields();

    Object[] keyValues = new Object[groupByFields.size()];
    for (int i = 0; i < groupByFields.size(); i++) {
      int sourceId = groupByFields.get(i).fieldId();

      int partitionPos = -1;
      for (int pos = 0; pos < partitionFields.size(); pos++) {
        PartitionField partitionField = partitionFields.get(pos);
        if (partitionField.sourceId() == sourceId && partitionField.transform().isIdentity()) {
          partitionPos = pos;
          break;
        }
      }

      if (partitionPos < 0) {
        return null;
      }

      keyValues[i] = partition.get(partitionPos, Object.class);
    }

    return new ArrayStructLike(keyValues);
  }

  private boolean canPushDownAggregation() {
    if (!isMainTable()) {
      return false;
    }

    return readConf().aggregatePushDownEnabled();
  }

  private boolean metricsModeSupportsAggregatePushDown(List<BoundAggregate<?, ?>> aggregates) {
    MetricsConfig config = MetricsConfig.forTable(table());
    for (BoundAggregate aggregate : aggregates) {
      String colName = aggregate.columnName();
      if (!colName.equals("*")) {
        MetricsModes.MetricsMode mode = config.columnMode(colName);
        if (mode instanceof MetricsModes.None) {
          LOG.info("Skipping aggregate pushdown: No metrics for column {}", colName);
          return false;
        } else if (mode instanceof MetricsModes.Counts) {
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from count for column {}",
                colName);
            return false;
          }
        } else if (aggregate.type().typeId() == Type.TypeID.STRING
            || aggregate.type().typeId() == Type.TypeID.BINARY) {
          // lower_bounds and upper_bounds may have been truncated before, so disable push down
          // regardless of the current mode
          if (aggregate.op() == Expression.Operation.MAX
              || aggregate.op() == Expression.Operation.MIN) {
            LOG.info(
                "Skipping aggregate pushdown: Cannot produce min or max from truncated values for column {}",
                colName);
            return false;
          }
        }
      }
    }

    return true;
  }

  @Override
  public Scan build() {
    if (localScan != null) {
      return localScan;
    } else if (startSnapshotId != null) {
      return buildIncrementalAppendScan();
    } else {
      return buildBatchScan();
    }
  }

  private Scan buildBatchScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkBatchQueryScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        buildIcebergBatchScan(projection, false /* use residuals */, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private Scan buildIncrementalAppendScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkIncrementalAppendScan(
        spark(),
        table(),
        startSnapshotId,
        endSnapshotId,
        buildIcebergIncrementalAppendScan(projection, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  public Scan buildCopyOnWriteScan() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkCopyOnWriteScan(
        spark(),
        table(),
        schema(),
        snapshot,
        branch,
        buildIcebergBatchScan(projection, true /* ignore residuals */, false /* no stats */),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private CloseableIterable<FileScanTask> planFilesWithStats() {
    Schema projection = projectionWithMetadataColumns();
    org.apache.iceberg.Scan<?, ?, ?> scan = buildIcebergScanWithStats(projection);
    if (scan != null) {
      return CloseableIterable.transform(scan.planFiles(), ScanTask::asFileScanTask);
    } else {
      return CloseableIterable.empty();
    }
  }

  private org.apache.iceberg.Scan<?, ?, ?> buildIcebergScanWithStats(Schema projection) {
    if (startSnapshotId != null) {
      return buildIcebergIncrementalAppendScan(projection, true /* with stats */);
    } else {
      return buildIcebergBatchScan(projection, false /* use residuals */, true /* with stats */);
    }
  }

  private IncrementalAppendScan buildIcebergIncrementalAppendScan(
      Schema projection, boolean withStats) {
    IncrementalAppendScan scan =
        table()
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(startSnapshotId)
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    return configureSplitPlanning(scan);
  }

  private BatchScan buildIcebergBatchScan(
      Schema projection, boolean ignoreResiduals, boolean withStats) {
    if (shouldPinSnapshot() && snapshot == null) {
      return null;
    }

    BatchScan scan =
        newIcebergBatchScan()
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (shouldPinSnapshot() || timeTravel != null) {
      scan = scan.useSnapshot(snapshot.snapshotId());
    }

    Preconditions.checkState(
        Objects.equals(snapshot, scan.snapshot()),
        "Failed to enforce scan consistency: resolved Spark table snapshot (%s) vs scan snapshot (%s)",
        snapshot,
        scan.snapshot());

    if (ignoreResiduals) {
      scan = scan.ignoreResiduals();
    }

    if (withStats) {
      scan = scan.includeColumnStats();
    }

    return configureSplitPlanning(scan);
  }

  private BatchScan newIcebergBatchScan() {
    if (readConf().distributedPlanningEnabled()) {
      return new SparkDistributedDataScan(spark(), table(), readConf());
    } else {
      return table().newBatchScan();
    }
  }

  private boolean shouldPinSnapshot() {
    return isMainTable() || isMetadataTableWithTimeTravel();
  }

  private boolean isMainTable() {
    return table() instanceof BaseTable;
  }

  private boolean isMetadataTableWithTimeTravel() {
    if (table() instanceof BaseMetadataTable metadataTable) {
      return metadataTable.supportsTimeTravel();
    } else {
      return false;
    }
  }

  /** Array-backed struct used as a group key; values are stored in raw internal form. */
  private static class ArrayStructLike implements StructLike {
    private final Object[] values;

    private ArrayStructLike(Object[] values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }

  /** Read-only concatenation of a group-key struct and its aggregate-result struct. */
  private static class JoinedStruct implements StructLike {
    private final StructLike left;
    private final int leftSize;
    private final StructLike right;

    private JoinedStruct(StructLike left, int leftSize, StructLike right) {
      this.left = left;
      this.leftSize = leftSize;
      this.right = right;
    }

    @Override
    public int size() {
      return leftSize + right.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      if (pos < leftSize) {
        return left.get(pos, javaClass);
      } else {
        return right.get(pos - leftSize, javaClass);
      }
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("JoinedStruct is read-only");
    }
  }
}
