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

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.expressions.filter.PartitionPredicate;

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
      List<PartitionPredicate> partitionPredicates,
      Supplier<ScanReport> scanReportSupplier) {
    super(
        spark,
        table,
        table.schema(),
        scan,
        readConf,
        projection,
        filters,
        partitionPredicates,
        scanReportSupplier);
    this.startSnapshotId = startSnapshotId;
    this.endSnapshotId = endSnapshotId;
  }

  @Override
  protected List<Object> equalityKey() {
    return Arrays.asList(
        table().name(),
        table().uuid(),
        startSnapshotId,
        endSnapshotId,
        readSchema(), // compare Spark schemas to ignore field ids
        filtersDesc(),
        partitionPredicates(),
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
