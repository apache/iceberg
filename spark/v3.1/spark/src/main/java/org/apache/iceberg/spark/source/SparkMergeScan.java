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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter;
import org.apache.spark.sql.connector.read.Statistics;

class SparkMergeScan extends SparkBatchScan implements SupportsFileFilter {

  private final Table table;
  private final boolean ignoreResiduals;
  private final Schema expectedSchema;
  private final Long snapshotId;
  private final long splitSize;
  private final int splitLookback;
  private final long splitOpenFileCost;

  // lazy variables
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks
  private Set<String> filteredLocations = null;

  SparkMergeScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      boolean ignoreResiduals,
      Schema expectedSchema,
      List<Expression> filters) {

    super(spark, table, readConf, expectedSchema, filters);

    this.table = table;
    this.ignoreResiduals = ignoreResiduals;
    this.expectedSchema = expectedSchema;
    this.splitSize = readConf.splitSize();
    this.splitLookback = readConf.splitLookback();
    this.splitOpenFileCost = readConf.splitOpenFileCost();

    Preconditions.checkArgument(readConf.snapshotId() == null, "Can't set snapshot-id in options");
    Snapshot currentSnapshot = table.currentSnapshot();
    this.snapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;

    // init files with an empty list if the table is empty to avoid picking any concurrent changes
    this.files = currentSnapshot == null ? Collections.emptyList() : null;
  }

  Long snapshotId() {
    return snapshotId;
  }

  @Override
  public Statistics estimateStatistics() {
    if (snapshotId == null) {
      return new Stats(0L, 0L);
    }
    return super.estimateStatistics();
  }

  @Override
  public SupportsFileFilter.FileFilterMetric filterFiles(Set<String> locations) {
    // invalidate cached tasks to trigger split planning again
    tasks = null;
    filteredLocations = locations;
    List<FileScanTask> originalFile = files();
    files =
        originalFile.stream()
            .filter(file -> filteredLocations.contains(file.file().path().toString()))
            .collect(Collectors.toList());
    return new SupportsFileFilter.FileFilterMetric(originalFile.size(), files.size());
  }

  // should be accessible to the write
  synchronized List<FileScanTask> files() {
    if (files == null) {
      TableScan scan =
          table
              .newScan()
              .caseSensitive(caseSensitive())
              .useSnapshot(snapshotId)
              .project(expectedSchema);

      for (Expression filter : filterExpressions()) {
        scan = scan.filter(filter);
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
  protected synchronized List<CombinedScanTask> tasks() {
    if (tasks == null) {
      CloseableIterable<FileScanTask> splitFiles =
          TableScanUtil.splitFiles(CloseableIterable.withNoopClose(files()), splitSize);
      CloseableIterable<CombinedScanTask> scanTasks =
          TableScanUtil.planTasks(
              splitFiles, splitSize,
              splitLookback, splitOpenFileCost);
      tasks = Lists.newArrayList(scanTasks);
    }

    return tasks;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkMergeScan that = (SparkMergeScan) o;
    return table().name().equals(that.table().name())
        && readSchema().equals(that.readSchema())
        && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString())
        && ignoreResiduals == that.ignoreResiduals
        && Objects.equals(snapshotId, that.snapshotId)
        && Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        readSchema(),
        filterExpressions().toString(),
        ignoreResiduals,
        snapshotId,
        filteredLocations);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergMergeScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }
}
