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

import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownLimit;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkChangelogScanBuilder extends BaseSparkScanBuilder
    implements SupportsPushDownV2Filters, SupportsPushDownRequiredColumns, SupportsPushDownLimit {

  SparkChangelogScanBuilder(
      SparkSession spark, Table table, Schema schema, CaseInsensitiveStringMap options) {
    super(spark, table, schema, options);
  }

  @Override
  public Scan build() {
    Long startSnapshotId = readConf().startSnapshotId();
    Long endSnapshotId = readConf().endSnapshotId();
    Long startTimestamp = readConf().startTimestamp();
    Long endTimestamp = readConf().endTimestamp();

    Preconditions.checkArgument(
        startSnapshotId == null || startTimestamp == null,
        "Cannot set both %s and %s for changelogs",
        SparkReadOptions.START_SNAPSHOT_ID,
        SparkReadOptions.START_TIMESTAMP);

    Preconditions.checkArgument(
        endSnapshotId == null || endTimestamp == null,
        "Cannot set both %s and %s for changelogs",
        SparkReadOptions.END_SNAPSHOT_ID,
        SparkReadOptions.END_TIMESTAMP);

    Preconditions.checkArgument(
        startTimestamp == null || endTimestamp == null || startTimestamp < endTimestamp,
        "Cannot set %s to be greater than %s for changelogs",
        SparkReadOptions.START_TIMESTAMP,
        SparkReadOptions.END_TIMESTAMP);

    if (startTimestamp != null) {
      if (noSnapshotsAfter(startTimestamp)) {
        return emptyChangelogScan();
      }
      startSnapshotId = getStartSnapshotId(startTimestamp);
    }

    if (endTimestamp != null) {
      endSnapshotId = getEndSnapshotId(endTimestamp);
      if (noSnapshotsBetween(startSnapshotId, endSnapshotId)) {
        return emptyChangelogScan();
      }
    }

    Schema projection = projectionWithMetadataColumns();
    IncrementalChangelogScan scan = buildIcebergScan(projection, startSnapshotId, endSnapshotId);
    return new SparkChangelogScan(spark(), table(), scan, readConf(), projection, filters());
  }

  private IncrementalChangelogScan buildIcebergScan(
      Schema projection, Long startSnapshotId, Long endSnapshotId) {
    IncrementalChangelogScan scan =
        table()
            .newIncrementalChangelogScan()
            .caseSensitive(caseSensitive())
            .filter(filter())
            .project(projection)
            .metricsReporter(metricsReporter());

    if (startSnapshotId != null) {
      scan = scan.fromSnapshotExclusive(startSnapshotId);
    }

    if (endSnapshotId != null) {
      scan = scan.toSnapshot(endSnapshotId);
    }

    scan = configureSplitPlanning(scan);

    return scan;
  }

  private SparkChangelogScan emptyChangelogScan() {
    return new SparkChangelogScan(
        spark(),
        table(),
        null /* no scan */,
        readConf(),
        projectionWithMetadataColumns(),
        filters());
  }

  private boolean noSnapshotsAfter(long timestamp) {
    Snapshot currentSnapshot = table().currentSnapshot();
    return currentSnapshot == null || timestamp > currentSnapshot.timestampMillis();
  }

  private boolean noSnapshotsBetween(Long startSnapshotId, Long endSnapshotId) {
    return (startSnapshotId == null && endSnapshotId == null)
        || (startSnapshotId != null && startSnapshotId.equals(endSnapshotId));
  }

  private Long getStartSnapshotId(Long startTimestamp) {
    Snapshot oldestSnapshotAfter = SnapshotUtil.oldestAncestorAfter(table(), startTimestamp);
    if (oldestSnapshotAfter == null) {
      return null;
    } else if (oldestSnapshotAfter.timestampMillis() == startTimestamp) {
      return oldestSnapshotAfter.snapshotId();
    } else {
      return oldestSnapshotAfter.parentId();
    }
  }

  private Long getEndSnapshotId(Long endTimestamp) {
    Long endSnapshotId = null;
    for (Snapshot snapshot : SnapshotUtil.currentAncestors(table())) {
      if (snapshot.timestampMillis() <= endTimestamp) {
        endSnapshotId = snapshot.snapshotId();
        break;
      }
    }
    return endSnapshotId;
  }
}
