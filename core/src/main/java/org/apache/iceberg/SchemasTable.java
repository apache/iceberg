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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.types.Types;

public class SchemasTable extends BaseMetadataTable {

  private static final Schema SCHEMAS_TABLE =
      new Schema(
          Types.NestedField.required(0, "schema_id", Types.IntegerType.get()),
          Types.NestedField.required(1, "fields", Types.StringType.get()),
          Types.NestedField.required(2, "partition_keys", Types.StringType.get()),
          Types.NestedField.required(3, "primary_keys", Types.StringType.get()));

  SchemasTable(Table table) {
    this(table, table.name() + ".schemas");
  }

  SchemasTable(Table table, String name) {
    super(table, name);
  }

  @Override
  public TableScan newScan() {
    return new SchemasTableScan(table());
  }

  @Override
  public Schema schema() {
    return SCHEMAS_TABLE;
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.SCHEMAS;
  }

  protected DataTask task(TableScan scan) {
    Iterable<Map.Entry<Integer, Schema>> entries = table().schemas().entrySet();
    return StaticDataTask.of(
        io().newInputFile(table().operations().current().metadataFileLocation()),
        schema(),
        scan.schema(),
        entries,
        entry -> SchemasTable.schemasToRow(entry, table()));
  }

  private static StaticDataTask.Row schemasToRow(Map.Entry<Integer, Schema> entry, Table table) {
    List<String> fields =
        entry.getValue().asStruct().fields().stream()
            .map(
                field ->
                    "id:" + field.fieldId() + ", name:" + field.name() + ", type:" + field.type())
            .collect(Collectors.toList());
    List<String> partitions =
        table.spec().fields().stream().map(PartitionField::name).collect(Collectors.toList());
    return StaticDataTask.Row.of(
        entry.getKey(),
        fields.toString(),
        partitions.toString(),
        entry.getValue().identifierFieldNames().toString());
  }

  private class SchemasTableScan extends StaticTableScan {
    SchemasTableScan(Table table) {
      super(table, SCHEMAS_TABLE, MetadataTableType.SCHEMAS, SchemasTable.this::task);
    }
  }
}
