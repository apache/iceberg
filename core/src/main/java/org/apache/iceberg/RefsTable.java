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

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes a table's known snapshot references as rows.
 *
 * <p>{@link SnapshotRefType} stores the valid snapshot references type.
 */
public class RefsTable extends BaseMetadataTable {
  private static final Schema SNAPSHOT_REF_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "name", Types.StringType.get()),
          Types.NestedField.required(2, "type", Types.StringType.get()),
          Types.NestedField.required(3, "snapshot_id", Types.LongType.get()),
          Types.NestedField.optional(4, "max_reference_age_in_ms", Types.LongType.get()),
          Types.NestedField.optional(5, "min_snapshots_to_keep", Types.IntegerType.get()),
          Types.NestedField.optional(6, "max_snapshot_age_in_ms", Types.LongType.get()));

  RefsTable(Table table) {
    this(table, table.name() + ".refs");
  }

  RefsTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new RefsTableScan(table());
  }

  @Override
  public Schema schema() {
    return SNAPSHOT_REF_SCHEMA;
  }

  private DataTask task(BaseTableScan scan) {
    Collection<String> refNames = table().refs().keySet();
    return StaticDataTask.of(
        table().io().newInputFile(table().operations().current().metadataFileLocation()),
        schema(),
        scan.schema(),
        refNames,
        referencesToRows(table().refs()));
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.REFS;
  }

  private class RefsTableScan extends StaticTableScan {
    RefsTableScan(Table table) {
      super(table, SNAPSHOT_REF_SCHEMA, MetadataTableType.REFS, RefsTable.this::task);
    }

    RefsTableScan(Table table, TableScanContext context) {
      super(table, SNAPSHOT_REF_SCHEMA, MetadataTableType.REFS, RefsTable.this::task, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new RefsTableScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(RefsTable.this.task(this));
    }
  }

  private static Function<String, StaticDataTask.Row> referencesToRows(
      Map<String, SnapshotRef> refs) {
    return refName ->
        StaticDataTask.Row.of(
            refName,
            refs.get(refName).type().name(),
            refs.get(refName).snapshotId(),
            refs.get(refName).maxRefAgeMs(),
            refs.get(refName).minSnapshotsToKeep(),
            refs.get(refName).maxSnapshotAgeMs());
  }
}
