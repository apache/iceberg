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

import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.iceberg.Scan;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Statistics;

class SparkBatchQueryScan extends SparkRuntimeFilterableScan {

  private final Snapshot snapshot;
  private final String branch;

  SparkBatchQueryScan(
      SparkSession spark,
      Table table,
      Schema schema,
      Snapshot snapshot,
      String branch,
      Scan<?, ? extends ScanTask, ? extends ScanTaskGroup<?>> scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, schema, scan, readConf, projection, filters, scanReportSupplier);
    this.snapshot = snapshot;
    this.branch = branch;
  }

  Long snapshotId() {
    return snapshot != null ? snapshot.snapshotId() : null;
  }

  @Override
  public Statistics estimateStatistics() {
    return estimateStatistics(snapshot);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SparkBatchQueryScan that = (SparkBatchQueryScan) o;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && Objects.equals(snapshot, that.snapshot)
        && Objects.equals(branch, that.branch)
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filtersDesc().equals(that.filtersDesc())
        && runtimeFiltersDesc().equals(that.runtimeFiltersDesc());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        table().uuid(),
        snapshot,
        branch,
        readSchema(),
        filtersDesc(),
        runtimeFiltersDesc());
  }

  @Override
  public String description() {
    return String.format(
        "IcebergScan(table=%s, schemaId=%s, snapshotId=%s, branch=%s, filters=%s, runtimeFilters=%s, groupedBy=%s)",
        table(),
        schema().schemaId(),
        snapshotId(),
        branch,
        filtersDesc(),
        runtimeFiltersDesc(),
        groupingKeyDesc());
  }
}
