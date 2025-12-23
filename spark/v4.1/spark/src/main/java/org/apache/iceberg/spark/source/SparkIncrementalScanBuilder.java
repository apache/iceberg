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

import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkIncrementalScanBuilder extends BaseSparkScanBuilder {

  private final long startSnapshotId;
  private final Long endSnapshotId;

  SparkIncrementalScanBuilder(SparkSession spark, Table table, CaseInsensitiveStringMap options) {
    super(spark, table, table.schema(), options);
    Pair<Long, Long> incrementalOptions = extractIncrementalOptions(options);
    this.startSnapshotId = incrementalOptions.first();
    this.endSnapshotId = incrementalOptions.second();
  }

  @Override
  public Scan build() {
    Schema projection = projectionWithMetadataColumns();
    return new SparkIncrementalScan(
        spark(),
        table(),
        startSnapshotId,
        endSnapshotId,
        buildScan(projection),
        readConf(),
        projection,
        filters(),
        metricsReporter()::scanReport);
  }

  private IncrementalAppendScan buildScan(Schema projection) {
    IncrementalAppendScan scan =
        table()
            .newIncrementalAppendScan()
            .fromSnapshotExclusive(startSnapshotId)
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    return configureSplitPlanning(scan);
  }

  private Pair<Long, Long> extractIncrementalOptions(CaseInsensitiveStringMap options) {
    SparkReadConf readConf = new SparkReadConf(spark(), table(), options);

    Long startSnapshotId = readConf.startSnapshotId();
    Long endSnapshotId = readConf.endSnapshotId();
    Long startTimestamp = readConf.startTimestamp();
    Long endTimestamp = readConf.endTimestamp();

    Preconditions.checkArgument(
        startTimestamp == null && endTimestamp == null,
        "Only changelog scans support `%s` and `%s`. Use `%s` and `%s` for incremental scans.",
        SparkReadOptions.START_TIMESTAMP,
        SparkReadOptions.END_TIMESTAMP,
        SparkReadOptions.START_SNAPSHOT_ID,
        SparkReadOptions.END_SNAPSHOT_ID);

    Preconditions.checkArgument(
        startSnapshotId != null,
        "Cannot set only `%s` for incremental scans. Please, set `%s` too.",
        SparkReadOptions.END_SNAPSHOT_ID,
        SparkReadOptions.START_SNAPSHOT_ID);

    return Pair.of(startSnapshotId, endSnapshotId);
  }
}
