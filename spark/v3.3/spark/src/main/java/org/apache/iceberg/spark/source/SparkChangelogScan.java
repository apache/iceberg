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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.Statistics;
import org.apache.spark.sql.connector.read.SupportsReportStatistics;
import org.apache.spark.sql.types.StructType;

class SparkChangelogScan implements Scan, SupportsReportStatistics {

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final IncrementalChangelogScan scan;
  private final SparkReadConf readConf;
  private final Schema expectedSchema;
  private final List<Expression> filters;
  private final Long startSnapshotId;
  private final Long endSnapshotId;
  private final boolean readTimestampWithoutZone;

  // lazy variables
  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups = null;
  private StructType expectedSparkType = null;

  SparkChangelogScan(
      SparkSession spark,
      Table table,
      IncrementalChangelogScan scan,
      SparkReadConf readConf,
      Schema expectedSchema,
      List<Expression> filters) {

    SparkSchemaUtil.validateMetadataColumnReferences(table.schema(), expectedSchema);

    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.table = table;
    this.scan = scan;
    this.readConf = readConf;
    this.expectedSchema = expectedSchema;
    this.filters = filters != null ? filters : Collections.emptyList();
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    this.readTimestampWithoutZone = readConf.handleTimestampWithoutZone();
  }

  @Override
  public Statistics estimateStatistics() {
    long rowsCount = taskGroups().stream().mapToLong(ScanTaskGroup::estimatedRowsCount).sum();
    long sizeInBytes = SparkSchemaUtil.estimateSize(readSchema(), rowsCount);
    return new Stats(sizeInBytes, rowsCount);
  }

  @Override
  public StructType readSchema() {
    if (expectedSparkType == null) {
      Preconditions.checkArgument(
          readTimestampWithoutZone || !SparkUtil.hasTimestampWithoutZone(expectedSchema),
          SparkUtil.TIMESTAMP_WITHOUT_TIMEZONE_ERROR);

      this.expectedSparkType = SparkSchemaUtil.convert(expectedSchema);
    }

    return expectedSparkType;
  }

  @Override
  public Batch toBatch() {
    return new SparkBatch(sparkContext, table, readConf, taskGroups(), expectedSchema, hashCode());
  }

  private List<ScanTaskGroup<ChangelogScanTask>> taskGroups() {
    if (taskGroups == null) {
      try (CloseableIterable<ScanTaskGroup<ChangelogScanTask>> groups = scan.planTasks()) {
        this.taskGroups = Lists.newArrayList(groups);
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close changelog scan: " + scan, e);
      }
    }

    return taskGroups;
  }

  @Override
  public String description() {
    return String.format(
        "%s [fromSnapshotId=%d, toSnapshotId=%d, filters=%s]",
        table, startSnapshotId, endSnapshotId, filtersAsString());
  }

  @Override
  public String toString() {
    return String.format(
        "IcebergChangelogScan(table=%s, type=%s, fromSnapshotId=%d, toSnapshotId=%d, filters=%s)",
        table, expectedSchema.asStruct(), startSnapshotId, endSnapshotId, filtersAsString());
  }

  private String filtersAsString() {
    return filters.stream().map(Spark3Util::describe).collect(Collectors.joining(", "));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkChangelogScan that = (SparkChangelogScan) o;
    return table.name().equals(that.table.name())
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field IDs
        && filters.toString().equals(that.filters.toString())
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table.name(), readSchema(), filters.toString(), startSnapshotId, endSnapshotId);
  }
}
