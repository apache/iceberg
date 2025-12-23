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
import java.util.Objects;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.MetricsModes;
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
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
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
    if (!canPushDownAggregation(aggregation)) {
      return false;
    }

    AggregateEvaluator aggregateEvaluator;
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
          return false;
        }
      } catch (IllegalArgumentException e) {
        LOG.info("Skipping aggregate pushdown: Bind failed for AggregateFunc {}", aggregateFunc, e);
        return false;
      }
    }

    aggregateEvaluator = AggregateEvaluator.create(expressions);

    if (!metricsModeSupportsAggregatePushDown(aggregateEvaluator.aggregates())) {
      return false;
    }

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
    StructLike structLike = aggregateEvaluator.result();
    pushedAggregateRows[0] =
        new StructInternalRow(aggregateEvaluator.resultType()).setStruct(structLike);
    localScan = new SparkLocalScan(table(), pushedAggregateSchema, pushedAggregateRows, filters());

    return true;
  }

  private boolean canPushDownAggregation(Aggregation aggregation) {
    if (!isMainTable()) {
      return false;
    }

    if (!readConf().aggregatePushDownEnabled()) {
      return false;
    }

    // If group by expression is the same as the partition, the statistics information can still
    // be used to calculate min/max/count, will enable aggregate push down in next phase.
    // TODO: enable aggregate push down for partition col group by expression
    if (aggregation.groupByExpressions().length > 0) {
      LOG.info("Skipping aggregate pushdown: group by aggregation push down is not supported");
      return false;
    }

    return true;
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
        } else if (mode instanceof MetricsModes.Truncate) {
          // lower_bounds and upper_bounds may be truncated, so disable push down
          if (aggregate.type().typeId() == Type.TypeID.STRING) {
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
}
