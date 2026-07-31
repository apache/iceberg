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

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

/** A {@link Table} implementation that exposes a table's metadata tables as rows. */
public class MetadataTablesTable extends BaseMetadataTable {

  private static final Schema METADATA_TABLES_SCHEMA =
      new Schema(Types.NestedField.required(1, "metadata_table_name", Types.StringType.get()));

  MetadataTablesTable(Table table) {
    this(table, table.name() + ".metadata_tables");
  }

  MetadataTablesTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new MetadataTablesScan(table());
  }

  @Override
  public Schema schema() {
    return METADATA_TABLES_SCHEMA;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.METADATA_TABLES;
  }

  private DataTask task(BaseTableScan scan) {
    String location =
        table().operations().current().metadataFileLocation() != null
            ? table().operations().current().metadataFileLocation()
            : scan.table().location();

    List<String> sortedMetadataTableTypes =
        Arrays.stream(MetadataTableType.values())
            .map(type -> type.name().toLowerCase(Locale.ROOT))
            .sorted()
            .collect(ImmutableList.toImmutableList());

    return StaticDataTask.of(
        table().io().newInputFile(location),
        schema(),
        scan.schema(),
        sortedMetadataTableTypes,
        StaticDataTask.Row::of);
  }

  private class MetadataTablesScan extends StaticTableScan {

    MetadataTablesScan(Table table) {
      super(
          table,
          METADATA_TABLES_SCHEMA,
          MetadataTableType.METADATA_TABLES,
          MetadataTablesTable.this::task);
    }

    MetadataTablesScan(Table table, TableScanContext context) {
      super(
          table,
          METADATA_TABLES_SCHEMA,
          MetadataTableType.METADATA_TABLES,
          MetadataTablesTable.this::task,
          context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new MetadataTablesScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(MetadataTablesTable.this.task(this));
    }
  }
}
