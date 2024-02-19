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

import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.Pair;

/**
 * A {@link Table} implementation that exposes a table's valid data files as rows.
 *
 * <p>A valid data file is one that is readable from any snapshot currently tracked by the table.
 *
 * <p>This table may return duplicate rows.
 */
public class AllDataFilesTable extends BaseFilesTable {

  AllDataFilesTable(Table table) {
    this(table, table.name() + ".all_data_files");
  }

  AllDataFilesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllDataFilesTableScan(table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_DATA_FILES;
  }

  public static class AllDataFilesTableScan extends BaseAllFilesTableScan {

    AllDataFilesTableScan(Table table, Schema schema) {
      super(table, schema, MetadataTableType.ALL_DATA_FILES);
    }

    private AllDataFilesTableScan(Table table, Schema schema, TableScanContext context) {
      super(table, schema, MetadataTableType.ALL_DATA_FILES, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new AllDataFilesTableScan(table, schema, context);
    }

    @Override
    protected CloseableIterable<Pair<Snapshot, ManifestFile>> manifests() {
      return reachableManifests(
          snapshot ->
              snapshot.dataManifests(table().io()).stream()
                  .map(manifestFile -> Pair.of(snapshot, manifestFile))
                  .collect(Collectors.toSet()));
    }
  }
}
