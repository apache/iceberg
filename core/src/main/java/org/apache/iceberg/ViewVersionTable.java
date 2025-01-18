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
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewVersion;

public class ViewVersionTable extends BaseViewMetadataTable {

  static final Schema VIEW_VERSION_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "version-id", Types.IntegerType.get()),
          Types.NestedField.required(2, "schema-id", Types.IntegerType.get()),
          Types.NestedField.required(3, "timestamp-ms", Types.TimestampType.withoutZone()),
          Types.NestedField.required(
              4,
              "summary",
              Types.MapType.ofRequired(6, 7, Types.StringType.get(), Types.StringType.get())),
          Types.NestedField.required(
              8,
              "representations",
              Types.ListType.ofRequired(
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(10, "type", Types.StringType.get()),
                      Types.NestedField.required(11, "sql", Types.StringType.get()),
                      Types.NestedField.required(12, "dialect", Types.StringType.get())))),
          Types.NestedField.optional(13, "default-catalog", Types.StringType.get()),
          Types.NestedField.required(14, "default-namespace", Types.StringType.get()));

  ViewVersionTable(View view) {
    super(view, view.name() + ".version");
  }

  @Override
  public TableScan newScan() {
    return new ViewVersionTableScan(this);
  }

  @Override
  public Schema schema() {
    return VIEW_VERSION_SCHEMA;
  }

  private DataTask task(BaseTableScan scan) {
    return StaticDataTask.of(
        io().newInputFile(location()),
        schema(),
        scan.schema(),
        operations().current().versions(),
        ViewVersionTable::viewVersionToRow);
  }

  private class ViewVersionTableScan extends StaticTableScan {
    ViewVersionTableScan(Table table) {
      super(table, VIEW_VERSION_SCHEMA, MetadataTableType.SNAPSHOTS, ViewVersionTable.this::task);
    }

    ViewVersionTableScan(Table table, TableScanContext context) {
      super(
          table,
          VIEW_VERSION_SCHEMA,
          MetadataTableType.SNAPSHOTS,
          ViewVersionTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new ViewVersionTableScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      // override planFiles to avoid the check for a current snapshot because this metadata table is
      // for all snapshots
      return CloseableIterable.withNoopClose(ViewVersionTable.this.task(this));
    }
  }

  private static StaticDataTask.Row viewVersionToRow(ViewVersion version) {
    return StaticDataTask.Row.of(
        version.versionId(),
        version.schemaId(),
        version.timestampMillis(),
        version.summary(),
        version.representations(),
        version.defaultCatalog(),
        version.defaultNamespace());
  }
}
