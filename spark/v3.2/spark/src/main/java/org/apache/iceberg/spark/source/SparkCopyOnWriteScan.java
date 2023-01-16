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
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.In;

class SparkCopyOnWriteScan extends SparkScan implements SupportsRuntimeFiltering {

  private final TableScan scan;
  private final Snapshot snapshot;

  // lazy variables
  private List<FileScanTask> files = null; // lazy cache of files
  private List<CombinedScanTask> tasks = null; // lazy cache of tasks
  private Set<String> filteredLocations = null;

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {
    this(spark, table, null, null, readConf, expectedSchema, filters);
  }

  SparkCopyOnWriteScan(
      SparkSession spark,
      Table table,
      TableScan scan,
      Snapshot snapshot,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    super(spark, table, readConf, expectedSchema, filters);

    this.scan = scan;
    this.snapshot = snapshot;

    if (scan == null) {
      this.files = Collections.emptyList();
      this.tasks = Collections.emptyList();
      this.filteredLocations = Collections.emptySet();
    }
  }

  Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(snapshot);
  }

  public NamedReference[] filterAttributes() {
    NamedReference file = Expressions.column(MetadataColumns.FILE_PATH.name());
    return new NamedReference[] {file};
  }

  @Override
  public void filter(Filter[] filters) {
    Preconditions.checkState(
        Objects.equals(snapshotId(), currentSnapshotId()),
        "Runtime file filtering is not possible: the table has been concurrently modified. "
            + "Row-level operation scan snapshot ID: %s, current table snapshot ID: %s. "
            + "If multiple threads modify the table, use independent Spark sessions in each thread.",
        snapshotId(),
        currentSnapshotId());

    for (Filter filter : filters) {
      // Spark can only pass In filters at the moment
      if (filter instanceof In
          && ((In) filter).attribute().equalsIgnoreCase(MetadataColumns.FILE_PATH.name())) {
        In in = (In) filter;

        Set<String> fileLocations = Sets.newHashSet();
        for (Object value : in.values()) {
          fileLocations.add((String) value);
        }

        // Spark may call this multiple times for UPDATEs with subqueries
        // as such cases are rewritten using UNION and the same scan on both sides
        // so filter files only if it is beneficial
        if (filteredLocations == null || fileLocations.size() < filteredLocations.size()) {
          this.tasks = null;
          this.filteredLocations = fileLocations;
          this.files =
              files().stream()
                  .filter(file -> fileLocations.contains(file.file().path().toString()))
                  .collect(Collectors.toList());
        }
      }
    }
  }

  // should be accessible to the write
  synchronized List<FileScanTask> files() {
    if (files == null) {
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
          TableScanUtil.splitFiles(
              CloseableIterable.withNoopClose(files()), scan.targetSplitSize());
      CloseableIterable<CombinedScanTask> scanTasks =
          TableScanUtil.planTasks(
              splitFiles, scan.targetSplitSize(), scan.splitLookback(), scan.splitOpenFileCost());
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

    SparkCopyOnWriteScan that = (SparkCopyOnWriteScan) o;
    return table().name().equals(that.table().name())
        && readSchema().equals(that.readSchema())
        && // compare Spark schemas to ignore field ids
        filterExpressions().toString().equals(that.filterExpressions().toString())
        && Objects.equals(snapshotId(), that.snapshotId())
        && Objects.equals(filteredLocations, that.filteredLocations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        readSchema(),
        filterExpressions().toString(),
        snapshotId(),
        filteredLocations);
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergCopyOnWriteScan(table=%s, type=%s, filters=%s, caseSensitive=%s)",
        table(), expectedSchema().asStruct(), filterExpressions(), caseSensitive());
  }

  private Long currentSnapshotId() {
    Snapshot currentSnapshot = table().currentSnapshot();
    return currentSnapshot != null ? currentSnapshot.snapshotId() : null;
  }
}
