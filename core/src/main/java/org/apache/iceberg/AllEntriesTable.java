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

/**
 * A {@link Table} implementation that exposes a table's manifest entries as rows, for both delete
 * and data files.
 *
 * <p>WARNING: this table exposes internal details, like files that have been deleted. For a table
 * of the live data files, use {@link DataFilesTable}.
 */
public class AllEntriesTable extends BaseEntriesTable {

  AllEntriesTable(Table table) {
    this(table, table.name() + ".all_entries");
  }

  AllEntriesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new Scan(table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_ENTRIES;
  }

  private static class Scan extends BaseAllMetadataTableScan {

    Scan(Table table, Schema schema) {
      super(table, schema, MetadataTableType.ALL_ENTRIES);
    }

    private Scan(Table table, Schema schema, TableScanContext context) {
      super(table, schema, MetadataTableType.ALL_ENTRIES, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new Scan(table, schema, context);
    }

    @Override
    protected CloseableIterable<FileScanTask> doPlanFiles() {
      CloseableIterable<ManifestFile> manifests =
          reachableManifests(snapshot -> snapshot.allManifests(table().io()));
      return BaseEntriesTable.planFiles(table(), manifests, tableSchema(), schema(), context());
    }
  }
}
