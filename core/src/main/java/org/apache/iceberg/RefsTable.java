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

import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's known reference(branch or tag) as rows.
 * <p>
 * This does not include snapshots that have been expired using {@link ExpireSnapshots}.
 */
public class RefsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_SCHEMA = new Schema(
      Types.NestedField.required(1, "reference", Types.StringType.get()),
      Types.NestedField.required(2, "type", Types.StringType.get()),
      Types.NestedField.optional(3, "snapshot_id", Types.LongType.get()),
      Types.NestedField.optional(4, "max_ref_age_ms", Types.LongType.get()),
      Types.NestedField.optional(5, "min_snapshots_to_keep", Types.IntegerType.get()),
      Types.NestedField.optional(6, "max_snapshot_age_ms", Types.LongType.get())
  );

  RefsTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".refs");
  }

  RefsTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new RefsTableScan(operations(), table());
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_SCHEMA;
  }

  private DataTask task(BaseTableScan scan) {
    TableOperations ops = operations();
    return StaticDataTask.of(
        ops.io().newInputFile(ops.current().metadataFileLocation()),
        schema(), scan.schema(), ops.current().refs().entrySet(),
        RefsTable::snapshotRefToRow
    );
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.REFS;
  }

  private class RefsTableScan extends StaticTableScan {
    RefsTableScan(TableOperations ops, Table table) {
      super(ops, table, SNAPSHOT_SCHEMA, MetadataTableType.REFS, RefsTable.this::task);
    }

    RefsTableScan(TableOperations ops, Table table, TableScanContext context) {
      super(ops, table, SNAPSHOT_SCHEMA, MetadataTableType.REFS, RefsTable.this::task, context);
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new RefsTableScan(ops, table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      // override planFiles to avoid the check for a current snapshot because this metadata table is for all snapshots
      return CloseableIterable.withNoopClose(RefsTable.this.task(this));
    }
  }

  private static StaticDataTask.Row snapshotRefToRow(Map.Entry<String, SnapshotRef> refEntry) {
    SnapshotRef snapshotRef = refEntry.getValue();
    return StaticDataTask.Row.of(
        refEntry.getKey(),
        snapshotRef.type().name(),
        snapshotRef.snapshotId(),
        snapshotRef.maxRefAgeMs(),
        snapshotRef.minSnapshotsToKeep(),
        snapshotRef.maxSnapshotAgeMs()
    );
  }
}
