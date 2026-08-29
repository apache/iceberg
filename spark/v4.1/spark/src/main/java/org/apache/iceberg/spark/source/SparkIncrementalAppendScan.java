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
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;

class SparkIncrementalAppendScan extends SparkRuntimeFilterableScan {

  private final long startSnapshotId;
  private final Long endSnapshotId;

  SparkIncrementalAppendScan(
      SparkSession spark,
      Table table,
      long startSnapshotId,
      Long endSnapshotId,
      IncrementalAppendScan scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters,
      Supplier<ScanReport> scanReportSupplier) {
    super(spark, table, table.schema(), scan, readConf, projection, filters, scanReportSupplier);
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    SparkIncrementalAppendScan that = (SparkIncrementalAppendScan) other;
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && startSnapshotId == that.startSnapshotId
        && Objects.equals(endSnapshotId, that.endSnapshotId)
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field ids
        && filtersDesc().equals(that.filtersDesc())
        && runtimeFiltersDesc().equals(that.runtimeFiltersDesc());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        table().uuid(),
        startSnapshotId,
        endSnapshotId,
        readSchema(),
        filtersDesc(),
        runtimeFiltersDesc());
  }

  @Override
  public String description() {
    return String.format(
        "IcebergIncrementalScan(table=%s, startSnapshotId=%s, endSnapshotId=%s, filters=%s, runtimeFilters=%s, groupedBy=%s)",
        table(),
        startSnapshotId,
        endSnapshotId,
        filtersDesc(),
        runtimeFiltersDesc(),
        groupingKeyDesc());
  }
}
