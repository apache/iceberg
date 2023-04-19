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
package org.apache.iceberg;

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's known snapshots as rows.
 *
 * <p>This does not include snapshots that have been expired using {@link ExpireSnapshots}.
 */
public class SnapshotsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()),
          Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
          Types.NestedField.optional(4, "operation", Types.StringType.get()),
          Types.NestedField.optional(5, "manifest_list", Types.StringType.get()),
          Types.NestedField.optional(
              6,
              "summary",
              Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.StringType.get())));

  SnapshotsTable(Table table) {
    this(table, table.name() + ".snapshots");
  }

  SnapshotsTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new SnapshotsTableScan(table());
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_SCHEMA;
  }

  private DataTask task(BaseTableScan scan) {
    return StaticDataTask.of(
        table().io().newInputFile(table().operations().current().metadataFileLocation()),
        schema(),
        scan.schema(),
        table().snapshots(),
        SnapshotsTable::snapshotToRow);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.SNAPSHOTS;
  }

  private class SnapshotsTableScan extends StaticTableScan {
    SnapshotsTableScan(Table table) {
      super(table, SNAPSHOT_SCHEMA, MetadataTableType.SNAPSHOTS, SnapshotsTable.this::task);
    }

    SnapshotsTableScan(Table table, TableScanContext context) {
      super(
          table, SNAPSHOT_SCHEMA, MetadataTableType.SNAPSHOTS, SnapshotsTable.this::task, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new SnapshotsTableScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      // override planFiles to avoid the check for a current snapshot because this metadata table is
      // for all snapshots
      return CloseableIterable.withNoopClose(SnapshotsTable.this.task(this));
    }
  }

  private static StaticDataTask.Row snapshotToRow(Snapshot snap) {
    return StaticDataTask.Row.of(
        snap.timestampMillis() * 1000,
        snap.snapshotId(),
        snap.parentId(),
        snap.operation(),
        snap.manifestListLocation(),
        snap.summary());
  }
}
