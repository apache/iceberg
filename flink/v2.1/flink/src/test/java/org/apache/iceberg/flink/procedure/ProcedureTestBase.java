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
package org.apache.iceberg.flink.procedure;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.FlinkCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/** Base class for {@link org.apache.flink.table.procedures.Procedure} tests. */
public class ProcedureTestBase extends CatalogTestBase {
  private Snapshot firstSnapshot;
  private Catalog currentCatalog;

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);

    this.currentCatalog = ((FlinkCatalog) getTableEnv().getCatalog(catalogName).get()).catalog();
  }

  @AfterEach
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.T", flinkDatabase);
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  protected long getFirstSnapshotId(String tableName) {
    if (firstSnapshot == null) {
      firstSnapshot = getFirstSnapshot(tableName);
    }

    return firstSnapshot.snapshotId();
  }

  protected long getLastSnapshotId(String tableName) {
    Table table = getFreshTable(tableName);
    Snapshot lastSnapshot = table.currentSnapshot();

    return lastSnapshot.snapshotId();
  }

  private Snapshot getFirstSnapshot(String tableName) {
    Table table = getFreshTable(tableName);
    Snapshot snapshot = table.currentSnapshot();
    while (snapshot.parentId() != null) {
      snapshot = table.snapshot(snapshot.parentId());
    }

    return snapshot;
  }

  protected long getFirstSnapshotTimestamp(String tableName) {
    if (firstSnapshot == null) {
      firstSnapshot = getFirstSnapshot(tableName);
    }

    return firstSnapshot.timestampMillis();
  }

  protected long getLastSnapshotTimestamp(String tableName) {
    Table table = getFreshTable(tableName);
    Snapshot lastSnapshot = table.currentSnapshot();

    return lastSnapshot.timestampMillis();
  }

  protected List<Long> getSnapshotTimestampsSorted(String tableName) {
    List<Snapshot> snapshots = getSnapshotsSortedByTimestamps(tableName);
    List<Long> timestamps = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      timestamps.add(snapshot.timestampMillis());
    }
    return timestamps;
  }

  protected List<Long> getSnapshotIdsSortedByTimestamps(String tableName) {
    List<Snapshot> snapshots = getSnapshotsSortedByTimestamps(tableName);
    List<Long> snapshotIds = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      snapshotIds.add(snapshot.snapshotId());
    }
    return snapshotIds;
  }

  private Table getFreshTable(String tableName) {
    Table table = currentCatalog.loadTable(TableIdentifier.parse(tableName));
    table.refresh();
    return table;
  }

  private List<Snapshot> getSnapshotsSortedByTimestamps(String tableName) {
    Table table = getFreshTable(tableName);
    List<Snapshot> snapshots = Lists.newArrayList();
    table.snapshots().forEach(snapshots::add);
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis));
    return snapshots;
  }

  protected Object toDateTime(long millis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(millis), ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
  }
}
