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
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsFileFilter;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK;
import static org.apache.iceberg.TableProperties.SPLIT_LOOKBACK_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE_DEFAULT;

class SparkBatchScan extends BaseBatchScan implements SupportsFileFilter {
  private static final Logger LOG = LoggerFactory.getLogger(SparkBatchScan.class);

  private final List<Expression> filterExpressions;
  private final boolean ignoreResiduals;
  private final Long snapshotId;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final Long asOfTimestamp;

  // lazy cache of tasks
  private List<FileScanTask> files = null;
  private List<CombinedScanTask> tasks = null;

  SparkBatchScan(Table table, Broadcast<FileIO> io, Broadcast<EncryptionManager> encryption, boolean caseSensitive,
                 Schema expectedSchema, List<Expression> filters, boolean ignoreResiduals,
                 CaseInsensitiveStringMap options) {
    super(table, expectedSchema, io, encryption, caseSensitive, options);
    this.filterExpressions = filters;
    this.ignoreResiduals = ignoreResiduals;
    this.snapshotId = Spark3Util.propertyAsLong(options, "snapshot-id", null);
    this.asOfTimestamp = Spark3Util.propertyAsLong(options, "as-of-timestamp", null);

    if (snapshotId != null && asOfTimestamp != null) {
      throw new IllegalArgumentException(
          "Cannot scan using both snapshot-id and as-of-timestamp to select the table snapshot");
    }

    this.startSnapshotId = Spark3Util.propertyAsLong(options, "start-snapshot-id", null);
    this.endSnapshotId = Spark3Util.propertyAsLong(options, "end-snapshot-id", null);
    if (snapshotId != null || asOfTimestamp != null) {
      if (startSnapshotId != null || endSnapshotId != null) {
        throw new IllegalArgumentException(
            "Cannot specify start-snapshot-id and end-snapshot-id to do incremental scan when either snapshot-id or " +
                "as-of-timestamp is specified");
      }
    } else if (startSnapshotId == null && endSnapshotId != null) {
      throw new IllegalArgumentException("Cannot only specify option end-snapshot-id to do incremental scan");
    }
  }

  public Long snapshotId() {
    return snapshotId;
  }

  public List<Expression> filterExpressions() {
    return filterExpressions;
  }

  @Override
  public void filterFiles(Set<String> locations) {
    // invalidate cached tasks to trigger split planning again
    tasks = null;
    files = files().stream()
        .filter(file -> locations.contains(file.file().path().toString()))
        .collect(Collectors.toList());
  }

  @Override
  public Statistics estimateStatistics() {
    // its a fresh table, no data
    if (table.currentSnapshot() == null) {
      return new Stats(0L, 0L);
    }

    // estimate stats using snapshot summary only for partitioned tables (metadata tables are unpartitioned)
    if (!table.spec().isUnpartitioned() && (filterExpressions == null || filterExpressions.isEmpty())) {
      LOG.debug("using table metadata to estimate table statistics");
      long totalRecords = PropertyUtil.propertyAsLong(table.currentSnapshot().summary(),
          SnapshotSummary.TOTAL_RECORDS_PROP, Long.MAX_VALUE);
      Schema projectedSchema = expectedSchema != null ? expectedSchema : table.schema();
      return new Stats(
          SparkSchemaUtil.estimateSize(SparkSchemaUtil.convert(projectedSchema), totalRecords),
          totalRecords);
    }

    long sizeInBytes = 0L;
    long numRows = 0L;

    for (CombinedScanTask task : tasks()) {
      for (FileScanTask file : task.files()) {
        sizeInBytes += file.length();
        numRows += file.file().recordCount();
      }
    }

    return new Stats(sizeInBytes, numRows);
  }

  // TODO: does dynamic file filtering break equals and hashCode?

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchScan that = (SparkBatchScan) o;
    return table.name().equals(that.table.name()) &&
        readSchema().equals(that.readSchema()) && // compare Spark schemas to ignore field ids
        filterExpressions.toString().equals(that.filterExpressions.toString()) &&
        ignoreResiduals == that.ignoreResiduals &&
        Objects.equals(snapshotId, that.snapshotId) &&
        Objects.equals(startSnapshotId, that.startSnapshotId) &&
        Objects.equals(endSnapshotId, that.endSnapshotId) &&
        Objects.equals(asOfTimestamp, that.asOfTimestamp);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table.name(), readSchema(), filterExpressions.toString(), ignoreResiduals,
        snapshotId, startSnapshotId, endSnapshotId, asOfTimestamp);
  }

  @Override
  protected List<CombinedScanTask> tasks() {
    if (tasks == null) {
      Map<String, String> props = table.properties();

      long actualSplitSize;
      if (splitSize != null) {
        actualSplitSize = splitSize;
      } else {
        actualSplitSize = PropertyUtil.propertyAsLong(props, SPLIT_SIZE, SPLIT_SIZE_DEFAULT);
      }

      int actualSplitLookback;
      if (splitLookback != null) {
        actualSplitLookback = splitLookback;
      } else {
        actualSplitLookback = PropertyUtil.propertyAsInt(props, SPLIT_LOOKBACK, SPLIT_LOOKBACK_DEFAULT);
      }

      long actualOpenFileCost;
      if (splitOpenFileCost != null) {
        actualOpenFileCost = splitOpenFileCost;
      } else {
        actualOpenFileCost = PropertyUtil.propertyAsLong(props, SPLIT_OPEN_FILE_COST, SPLIT_OPEN_FILE_COST_DEFAULT);
      }

      CloseableIterable<FileScanTask> splitFiles = TableScanUtil.splitFiles(
          CloseableIterable.withNoopClose(files()),
          actualSplitSize);
      CloseableIterable<CombinedScanTask> scanTasks = TableScanUtil.planTasks(
          splitFiles, actualSplitSize,
          actualSplitLookback, actualOpenFileCost);
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  public List<FileScanTask> files() {
    if (files == null) {
      TableScan scan = table
          .newScan()
          .caseSensitive(caseSensitive)
          .project(expectedSchema);

      if (snapshotId != null) {
        scan = scan.useSnapshot(snapshotId);
      }

      if (asOfTimestamp != null) {
        scan = scan.asOfTime(asOfTimestamp);
      }

      if (startSnapshotId != null) {
        if (endSnapshotId != null) {
          scan = scan.appendsBetween(startSnapshotId, endSnapshotId);
        } else {
          scan = scan.appendsAfter(startSnapshotId);
        }
      }

      if (filterExpressions != null) {
        for (Expression filter : filterExpressions) {
          scan = scan.filter(filter);
        }
      }

      if (ignoreResiduals) {
        scan = scan.ignoreResiduals();
      }

      try (CloseableIterable<FileScanTask> filesIterable = scan.planFiles()) {
        this.files = Lists.newArrayList(filesIterable);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
      }
    }

    return files;
  }

  @Override
  public String description() {
    String filters = filterExpressions.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
    return String.format("%s [filters=%s]", table, filters);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table, expectedSchema.asStruct(), filterExpressions, caseSensitive);
  }
}
