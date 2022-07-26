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
 * A {@link Table} implementation that exposes a table's files as rows.
 */
public class FilesTable extends BaseFilesTable {

  FilesTable(TableOperations ops, Table table) {
    this(ops, table, table.name() + ".files");
  }

  FilesTable(TableOperations ops, Table table, String name) {
    super(ops, table, name);
  }

  @Override
  public TableScan newScan() {
    return new FilesTableScan(operations(), table(), schema());
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.FILES;
  }

  public static class FilesTableScan extends BaseFilesTableScan {

    FilesTableScan(TableOperations ops, Table table, Schema schema) {
      super(ops, table, schema, MetadataTableType.FILES);
    }

    FilesTableScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      super(ops, table, schema, MetadataTableType.FILES, context);
    }

    @Override
    protected TableScan newRefinedScan(TableOperations ops, Table table, Schema schema, TableScanContext context) {
      return new FilesTableScan(ops, table, schema, context);
    }

    @Override
    protected CloseableIterable<ManifestFile> manifests() {
      return CloseableIterable.withNoopClose(snapshot().allManifests(tableOps().io()));
    }
  }
}
