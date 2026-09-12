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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;

/**
 * A {@link Table} implementation that exposes the catalog-provided labels of its base table as
 * rows.
 *
 * <p>Each label key-value pair is a row. Object-level labels have {@code scope = "object"} and a
 * null {@code field_id}; field-level labels have {@code scope = "field"} and the field id they are
 * attached to. Labels are catalog-provided enrichment obtained at load time; a re-scan re-reads the
 * base table's labels and may observe different values.
 */
public class LabelsTable extends BaseMetadataTable {
  private static final String OBJECT_SCOPE = "object";
  private static final String FIELD_SCOPE = "field";

  private static final Schema LABELS_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "scope", Types.StringType.get()),
          Types.NestedField.optional(2, "field_id", Types.IntegerType.get()),
          Types.NestedField.optional(3, "field_name", Types.StringType.get()),
          Types.NestedField.required(4, "key", Types.StringType.get()),
          Types.NestedField.required(5, "value", Types.StringType.get()));

  LabelsTable(Table table) {
    this(table, table.name() + ".labels");
  }

  LabelsTable(Table table, String name) {
    super(table, name);
  }

  @Override
  MetadataTableType metadataTableType() {
    return MetadataTableType.LABELS;
  }

  @Override
  public TableScan newScan() {
    return new LabelsScan(table());
  }

  @Override
  public Schema schema() {
    return LABELS_SCHEMA;
  }

  private Labels labels() {
    return table().labels();
  }

  private DataTask task(TableScan scan) {
    Labels labels = labels();
    Schema tableSchema = table().schema();
    List<StaticDataTask.Row> rows = Lists.newArrayList();
    for (Map.Entry<String, String> entry : labels.objectLabels().entrySet()) {
      rows.add(StaticDataTask.Row.of(OBJECT_SCOPE, null, null, entry.getKey(), entry.getValue()));
    }

    for (FieldLabels fieldLabels : labels.fields()) {
      // null when the field id is not in the current schema (e.g. a dropped column)
      String fieldName = tableSchema.findColumnName(fieldLabels.fieldId());
      for (Map.Entry<String, String> entry : fieldLabels.labels().entrySet()) {
        rows.add(
            StaticDataTask.Row.of(
                FIELD_SCOPE, fieldLabels.fieldId(), fieldName, entry.getKey(), entry.getValue()));
      }
    }

    return StaticDataTask.of(
        table().io().newInputFile(table().operations().current().metadataFileLocation()),
        schema(),
        scan.schema(),
        rows,
        row -> row);
  }

  private class LabelsScan extends StaticTableScan {
    LabelsScan(Table table) {
      super(table, LABELS_SCHEMA, MetadataTableType.LABELS, LabelsTable.this::task);
    }

    LabelsScan(Table table, TableScanContext context) {
      super(table, LABELS_SCHEMA, MetadataTableType.LABELS, LabelsTable.this::task, context);
    }

    @Override
    protected TableScan newRefinedScan(Table table, Schema schema, TableScanContext context) {
      return new LabelsScan(table, context);
    }

    @Override
    public CloseableIterable<FileScanTask> planFiles() {
      return CloseableIterable.withNoopClose(LabelsTable.this.task(this));
    }
  }
}
