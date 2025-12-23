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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.StrictMetricsEvaluator;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.spark.SparkV2Filters;
import org.apache.iceberg.spark.TimeTravel;
import org.apache.iceberg.spark.TimeTravel.AsOfTimestamp;
import org.apache.iceberg.spark.TimeTravel.AsOfVersion;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.RowLevelOperationBuilder;
import org.apache.spark.sql.connector.write.RowLevelOperationInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The main Spark table implementation that supports reads, writes, and row-level operations.
 *
 * <p>Note the table state (e.g. schema, snapshot) is pinned upon loading and must not change.
 */
public class SparkTable extends BaseSparkTable
    implements SupportsRead, SupportsWrite, SupportsDeleteV2, SupportsRowLevelOperations {

  private static final Logger LOG = LoggerFactory.getLogger(SparkTable.class);

  private static final Set<TableCapability> CAPABILITIES =
      ImmutableSet.of(
          TableCapability.AUTOMATIC_SCHEMA_EVOLUTION,
          TableCapability.BATCH_READ,
          TableCapability.BATCH_WRITE,
          TableCapability.MICRO_BATCH_READ,
          TableCapability.STREAMING_WRITE,
          TableCapability.OVERWRITE_BY_FILTER,
          TableCapability.OVERWRITE_DYNAMIC);
  private static final Set<TableCapability> CAPABILITIES_WITH_ACCEPT_ANY_SCHEMA =
      ImmutableSet.<TableCapability>builder()
          .addAll(CAPABILITIES)
          .add(TableCapability.ACCEPT_ANY_SCHEMA)
          .build();

  private final Schema schema; // effective schema (not necessarily current table schema)
  private final Snapshot snapshot; // always set unless table is empty
  private final String branch; // set if table is loaded for specific branch
  private final TimeTravel timeTravel; // set if table is loaded for time travel
  private final Set<TableCapability> capabilities;

  public SparkTable(Table table) {
    this(table, null /* main branch */);
  }

  private SparkTable(Table table, String branch) {
    this(
        table,
        table.schema(),
        determineLatestSnapshot(table, branch),
        branch,
        null /* no time travel */);
  }

  private SparkTable(Table table, long snapshotId, TimeTravel timeTravel) {
    this(
        table,
        SnapshotUtil.schemaFor(table, snapshotId),
        table.snapshot(snapshotId),
        null /* main branch */,
        timeTravel);
  }

  private SparkTable(
      Table table, Schema schema, Snapshot snapshot, String branch, TimeTravel timeTravel) {
    super(table, schema);
    this.schema = schema;
    this.snapshot = snapshot;
    this.branch = branch;
    this.timeTravel = timeTravel;
    this.capabilities = acceptAnySchema(table) ? CAPABILITIES_WITH_ACCEPT_ANY_SCHEMA : CAPABILITIES;
  }

  public SparkTable copyWithBranch(String newBranch) {
    return new SparkTable(table(), newBranch);
  }

  public Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  public String branch() {
    return branch;
  }

  @Override
  public String version() {
    return String.format("branch_%s_snapshot_%s", branch, snapshotId());
  }

  @Override
  public Set<TableCapability> capabilities() {
    return capabilities;
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return new SparkScanBuilder(spark(), table(), schema, snapshot, branch, timeTravel, options);
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    Preconditions.checkArgument(timeTravel == null, "Cannot write to table with time travel");
    return new SparkWriteBuilder(spark(), table(), branch, info);
  }

  @Override
  public RowLevelOperationBuilder newRowLevelOperationBuilder(RowLevelOperationInfo info) {
    Preconditions.checkArgument(timeTravel == null, "Cannot modify table with time travel");
    return new SparkRowLevelOperationBuilder(spark(), table(), snapshot, branch, info);
  }

  @Override
  public boolean canDeleteWhere(Predicate[] predicates) {
    Preconditions.checkArgument(timeTravel == null, "Cannot delete from table with time travel");

    Expression deleteExpr = Expressions.alwaysTrue();

    for (Predicate predicate : predicates) {
      Expression expr = SparkV2Filters.convert(predicate);
      if (expr != null) {
        deleteExpr = Expressions.and(deleteExpr, expr);
      } else {
        return false;
      }
    }

    return canDeleteUsingMetadata(deleteExpr);
  }

  // a metadata delete is possible iff matching files can be deleted entirely
  private boolean canDeleteUsingMetadata(Expression deleteExpr) {
    boolean caseSensitive = SparkUtil.caseSensitive(spark());

    if (ExpressionUtil.selectsPartitions(deleteExpr, table(), caseSensitive)) {
      return true;
    }

    TableScan scan =
        table()
            .newScan()
            .filter(deleteExpr)
            .caseSensitive(caseSensitive)
            .includeColumnStats()
            .ignoreResiduals();

    if (snapshot != null) {
      scan = scan.useSnapshot(snapshot.snapshotId());
    }

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Map<Integer, Evaluator> evaluators = Maps.newHashMap();
      StrictMetricsEvaluator metricsEvaluator = new StrictMetricsEvaluator(schema, deleteExpr);
      return Iterables.all(
          tasks,
          task -> {
            DataFile file = task.file();
            PartitionSpec spec = task.spec();
            Evaluator evaluator =
                evaluators.computeIfAbsent(
                    spec.specId(),
                    specId ->
                        new Evaluator(
                            spec.partitionType(), Projections.strict(spec).project(deleteExpr)));
            return evaluator.eval(file.partition()) || metricsEvaluator.eval(file);
          });

    } catch (IOException ioe) {
      LOG.warn("Failed to close task iterable", ioe);
      return false;
    }
  }

  @Override
  public void deleteWhere(Predicate[] predicates) {
    Expression deleteExpr = SparkV2Filters.convert(predicates);

    if (deleteExpr == Expressions.alwaysFalse()) {
      LOG.info("Skipping the delete operation as the condition is always false");
      return;
    }

    DeleteFiles deleteFiles =
        table()
            .newDelete()
            .set("spark.app.id", spark().sparkContext().applicationId())
            .deleteFromRowFilter(deleteExpr);

    if (branch != null) {
      deleteFiles.toBranch(branch);
    }

    if (!CommitMetadata.commitProperties().isEmpty()) {
      CommitMetadata.commitProperties().forEach(deleteFiles::set);
    }

    deleteFiles.commit();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    } else if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkTable that = (SparkTable) other;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && schema.schemaId() == that.schema.schemaId()
        && Objects.equals(snapshotId(), that.snapshotId())
        && Objects.equals(branch, that.branch)
        && Objects.equals(timeTravel, that.timeTravel);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(), table().uuid(), schema.schemaId(), snapshotId(), branch, timeTravel);
  }

  public static SparkTable create(Table table, String branch) {
    ValidationException.check(
        branch == null || SnapshotRef.MAIN_BRANCH.equals(branch) || table.snapshot(branch) != null,
        "Cannot use branch (does not exist): %s",
        branch);
    return new SparkTable(table, branch);
  }

  public static SparkTable create(Table table, TimeTravel timeTravel) {
    if (timeTravel == null) {
      return new SparkTable(table);
    } else if (timeTravel instanceof AsOfVersion asOfVersion) {
      return createWithVersion(table, asOfVersion);
    } else if (timeTravel instanceof AsOfTimestamp asOfTimestamp) {
      return createWithTimestamp(table, asOfTimestamp);
    } else {
      throw new IllegalArgumentException("Unknown time travel: " + timeTravel);
    }
  }

  private static SparkTable createWithVersion(Table table, AsOfVersion timeTravel) {
    if (timeTravel.isSnapshotId()) {
      return new SparkTable(table, Long.parseLong(timeTravel.version()), timeTravel);
    } else {
      SnapshotRef ref = table.refs().get(timeTravel.version());
      Preconditions.checkArgument(
          ref != null,
          "Cannot find matching snapshot ID or reference name for version %s",
          timeTravel.version());
      if (ref.isBranch()) {
        return new SparkTable(table, timeTravel.version());
      } else {
        return new SparkTable(table, ref.snapshotId(), timeTravel);
      }
    }
  }

  // Iceberg uses milliseconds for snapshot timestamps
  private static SparkTable createWithTimestamp(Table table, AsOfTimestamp timeTravel) {
    long timestampMillis = timeTravel.timestampMillis();
    long snapshotId = SnapshotUtil.snapshotIdAsOfTime(table, timestampMillis);
    return new SparkTable(table, snapshotId, timeTravel);
  }

  private static boolean acceptAnySchema(Table table) {
    return PropertyUtil.propertyAsBoolean(
        table.properties(),
        TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA,
        TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA_DEFAULT);
  }

  // returns latest snapshot for branch or current snapshot if branch is yet to be created
  private static Snapshot determineLatestSnapshot(Table table, String branch) {
    if (branch != null && table.refs().containsKey(branch)) {
      return SnapshotUtil.latestSnapshot(table, branch);
    } else {
      return table.currentSnapshot();
    }
  }
}
