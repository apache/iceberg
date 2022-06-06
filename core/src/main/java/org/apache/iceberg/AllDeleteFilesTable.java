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
 * A {@link Table} implementation that exposes its valid delete files as rows.
 * <p>
 * A valid delete file is one that is readable from any snapshot currently tracked by the table.
 * <p>
 * This table may return duplicate rows.
 */
public class AllDeleteFilesTable extends BaseFilesTable {

  AllDeleteFilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".all_delete_files");
  }

  AllDeleteFilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new AllDeleteFilesTableScan(operations(), table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.ALL_DELETE_FILES;
  }

  public static class AllDeleteFilesTableScan extends BaseAllFilesTableScan {

    AllDeleteFilesTableScan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema, MetadataTableType.ALL_DELETE_FILES);
    }

    private AllDeleteFilesTableScan(TableOperations ops, Table table, Schema schema,
                                    TableScanContext context) {
      super(ops, table, schema, MetadataTableType.ALL_DELETE_FILES, context);
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new AllDeleteFilesTableScan(ops, table, schema, context);
    }

    @Override
    protected CloseableIterable<ManifestFile> manifests() {
      return reachableManifests(snapshot -> snapshot.deleteManifests(tableOps().io()));
    }
  }
}
