/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.netflix.iceberg.TableMetadata.SnapshotLogEntry;
import com.netflix.iceberg.expressions.Expression;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.ResidualEvaluator;
import com.netflix.iceberg.io.CloseableGroup;
import com.netflix.iceberg.util.BinPacking;
import com.netflix.iceberg.util.ParallelIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;

import static com.netflix.iceberg.util.ThreadPools.getPlannerPool;
import static com.netflix.iceberg.util.ThreadPools.getWorkerPool;

/**
 * Base class for {@link TableScan} implementations.
 */
class BaseTableScan extends CloseableGroup implements TableScan {
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static final Logger LOG = LoggerFactory.getLogger(TableScan.class);

  private final TableOperations ops;
  private final Table table;
  private final Long snapshotId;
  private final Collection<String> columns;
  private final Expression rowFilter;

  BaseTableScan(TableOperations ops, Table table) {
    this(ops, table, null, Filterable.ALL_COLUMNS, Expressions.alwaysTrue());
  }

  private BaseTableScan(TableOperations ops, Table table, Long snapshotId, Collection<String> columns, Expression rowFilter) {
    this.ops = ops;
    this.table = table;
    this.snapshotId = snapshotId;
    this.columns = columns;
    this.rowFilter = rowFilter;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public TableScan useSnapshot(long snapshotId) {
    Preconditions.checkArgument(this.snapshotId == null,
        "Cannot override snapshot, already set to id=%s", snapshotId);
    Preconditions.checkArgument(ops.current().snapshot(snapshotId) != null,
        "Cannot find snapshot with ID %s", snapshotId);
    return new BaseTableScan(ops, table, snapshotId, columns, rowFilter);
  }

  @Override
  public TableScan asOfTime(long timestampMillis) {
    Preconditions.checkArgument(this.snapshotId == null,
        "Cannot override snapshot, already set to id=%s", snapshotId);

    Long lastSnapshotId = null;
    for (SnapshotLogEntry logEntry : ops.current().snapshotLog()) {
      if (logEntry.timestampMillis() <= timestampMillis) {
        lastSnapshotId = logEntry.snapshotId();
      }
    }

    // the snapshot ID could be null if no entries were older than the requested time. in that case,
    // there is no valid snapshot to read.
    Preconditions.checkArgument(lastSnapshotId != null,
        "Cannot find a snapshot older than %s", DATE_FORMAT.format(new Date(timestampMillis)));

    return useSnapshot(lastSnapshotId);
  }

  @Override
  public TableScan select(Collection<String> columns) {
    return new BaseTableScan(ops, table, snapshotId, columns, rowFilter);
  }

  @Override
  public TableScan filter(Expression expr) {
    return new BaseTableScan(ops, table, snapshotId, columns, Expressions.and(rowFilter, expr));
  }

  @Override
  public Iterable<FileScanTask> planFiles() {
    Snapshot snapshot = snapshotId != null ?
        ops.current().snapshot(snapshotId) :
        ops.current().currentSnapshot();

    if (snapshot != null) {
      LOG.info("Scanning table {} snapshot {} created at {} with filter {}", table,
          snapshot.snapshotId(), DATE_FORMAT.format(new Date(snapshot.timestampMillis())),
          rowFilter);

      Iterable<Iterable<FileScanTask>> readers = Iterables.transform(
          snapshot.manifests(),
          manifest -> {
            ManifestReader reader = ManifestReader.read(ops.newInputFile(manifest));
            addCloseable(reader);
            String schemaString = SchemaParser.toJson(reader.spec().schema());
            String specString = PartitionSpecParser.toJson(reader.spec());
            ResidualEvaluator residuals = new ResidualEvaluator(reader.spec(), rowFilter);
            return Iterables.transform(
                reader.filterRows(rowFilter).select(columns),
                file -> new BaseFileScanTask(file, schemaString, specString, residuals)
            );
          });

      if (snapshot.manifests().size() > 1) {
        return new ParallelIterable<>(readers, getPlannerPool(), getWorkerPool());
      } else {
        return Iterables.concat(readers);
      }

    } else {
      LOG.info("Scanning empty table {}", table);
      return Collections.emptyList();
    }
  }

  @Override
  public Iterable<CombinedScanTask> planTasks() {
    long splitSize = ops.current().propertyAsLong(
        TableProperties.SPLIT_SIZE, TableProperties.SPLIT_SIZE_DEFAULT);
    int lookback = ops.current().propertyAsInt(
        TableProperties.SPLIT_LOOKBACK, TableProperties.SPLIT_LOOKBACK_DEFAULT);

    return Iterables.transform(
        new BinPacking.PackingIterable<>(planFiles(), splitSize, lookback, FileScanTask::length),
        BaseCombinedScanTask::new);
  }

  @Override
  public Expression filter() {
    return rowFilter;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("table", table)
        .add("columns", columns)
        .add("filter", rowFilter)
        .toString();
  }
}
