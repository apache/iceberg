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

import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's known snapshots as rows.
 * <p>
 * This does not include snapshots that have been expired using {@link ExpireSnapshots}.
 */
public class SnapshotsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_SCHEMA = new Schema(
      Types.NestedField.required(1, "committed_at", Types.TimestampType.withZone()),
      Types.NestedField.required(2, "snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(3, "parent_id", Types.LongType.get()),
      Types.NestedField.optional(4, "operation", Types.StringType.get()),
      Types.NestedField.optional(5, "manifest_list", Types.StringType.get()),
      Types.NestedField.optional(6, "summary",
          Types.MapType.ofRequired(7, 8, Types.StringType.get(), Types.StringType.get()))
  );

  private final TableOperations ops;
  private final Table table;

  public SnapshotsTable(TableOperations ops, Table table) {
    this.ops = ops;
    this.table = table;
  }

  @Override
  Table table() {
    return table;
  }

  @Override
  String metadataTableName() {
    return "snapshots";
  }

  @Override
  public TableScan newScan() {
    return new SnapshotsTableScan();
  }

  @Override
  public String location() {
    return ops.current().file().location();
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_SCHEMA;
  }

  private DataTask task(BaseTableScan scan) {
    return StaticDataTask.of(ops.current().file(), ops.current().snapshots(), SnapshotsTable::snapshotToRow);
  }

  private class SnapshotsTableScan extends StaticTableScan {
    SnapshotsTableScan() {
      super(ops, table, SNAPSHOT_SCHEMA, SnapshotsTable.this::task);
    }
  }

  private static StaticDataTask.Row snapshotToRow(Snapshot snap) {
    return StaticDataTask.Row.of(
        snap.timestampMillis() * 1000,
        snap.snapshotId(),
        snap.parentId(),
        snap.operation(),
        snap.manifestListLocation(),
        snap.summary()
    );
  }
}
