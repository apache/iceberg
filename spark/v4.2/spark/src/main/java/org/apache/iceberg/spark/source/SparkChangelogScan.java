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
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.spark.sql.SparkSession;

class SparkChangelogScan extends SparkPartitioningAwareScan<PartitionScanTask> {
  private final Long startSnapshotId;
  private final Long endSnapshotId;

  SparkChangelogScan(
      SparkSession spark,
      Table table,
      IncrementalChangelogScan scan,
      SparkReadConf readConf,
      Schema projection,
      List<Expression> filters) {
    super(
        spark,
        table,
        table.schema(),
        scan,
        readConf,
        projection,
        filters,
        Collections.emptyList(),
        null);
    this.startSnapshotId = readConf.startSnapshotId();
    this.endSnapshotId = readConf.endSnapshotId();
    tasks();
  }

  @Override
  protected Class<PartitionScanTask> taskJavaClass() {
    return PartitionScanTask.class;
  }

  @Override
  public String description() {
    return String.format(
        Locale.ROOT,
        "IcebergChangelogScan(table=%s, fromSnapshotId=%d, toSnapshotId=%d, filters=%s)",
        table(),
        startSnapshotId,
        endSnapshotId,
        filtersDesc());
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
    return table().name().equals(that.table().name())
        && Objects.equals(table().uuid(), that.table().uuid())
        && readSchema().equals(that.readSchema()) // compare Spark schemas to ignore field IDs
        && filtersDesc().equals(that.filtersDesc())
        && Objects.equals(startSnapshotId, that.startSnapshotId)
        && Objects.equals(endSnapshotId, that.endSnapshotId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        table().name(),
        table().uuid(),
        readSchema(),
        filtersDesc(),
        startSnapshotId,
        endSnapshotId);
  }
}
