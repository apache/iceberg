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

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.types.StructType;

class SparkCdcScan implements Scan, SupportsReportStatistics {

  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final IncrementalChangelogScan scan;
  private final SparkReadConf readConf;
  private final Schema projection;
  private final SparkChangelogRange range;
  private final Long batchEndSnapshotId;

  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups = null;
  private StructType expectedSparkType = null;

  SparkCdcScan(
      SparkSession spark,
      Table table,
      IncrementalChangelogScan scan,
      SparkReadConf readConf,
      Schema projection,
      SparkChangelogRange range) {
    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), projection);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.scan = scan;
    this.readConf = readConf;
    this.projection = projection;
    this.range = range;
    Snapshot currentSnapshot = table.currentSnapshot();
    this.batchEndSnapshotId = currentSnapshot != null ? currentSnapshot.snapshotId() : null;
  }

  @Override
  public Statistics estimateStatistics() {
    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    return new Stats(sizeInBytes, rowsCount, Collections.emptyMap());
  }

  @Override
  public StructType readSchema() {
    if (expectedSparkType == null) {
      this.expectedSparkType = SparkSchemaUtil.convert(projection);
    }

    return expectedSparkType;
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(
        sparkContext,
        table,
        scan.fileIO(),
        readConf,
        EMPTY_GROUPING_KEY_TYPE,
        taskGroups(),
        projection,
        hashCode());
  }

  @Override
  public MicroBatchStream toMicroBatchStream(String checkpointLocation) {
    return new SparkChangelogMicroBatchStream(
        sparkContext, table, readConf, projection, checkpointLocation, range);
  }

  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups() {
    if (taskGroups == null) {
      range.validateVersions(table);
      this.taskGroups =
          batchEndSnapshotId == null
              ? Collections.emptyList()
              : range.planTasks(table, scan, null, batchEndSnapshotId);
    }

    return taskGroups;
  }

  @Override
  public String description() {
    return String.format(Locale.ROOT, "IcebergCdcScan(table=%s, range=%s)", table, range);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof SparkCdcScan that)) {
      return false;
    }

    return table.name().equals(that.table.name())
        && Objects.equals(table.uuid(), that.table.uuid())
        && readSchema().equals(that.readSchema())
        && range.equals(that.range)
        && Objects.equals(batchEndSnapshotId, that.batchEndSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(table.name(), table.uuid(), readSchema(), range, batchEndSnapshotId);
  }
}
