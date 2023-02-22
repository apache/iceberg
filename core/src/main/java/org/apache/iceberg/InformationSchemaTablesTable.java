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
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

public class InformationSchemaTablesTable extends InformationSchemaTable {
  private static final int NAME_COLUMN_ID = 1;
  private static final int NAMESPACE_COLUMN_ID = 2;

  private static final Schema TABLES_SCHEMA =
      new Schema(
          Types.NestedField.required(
              NAME_COLUMN_ID, "table_name", Types.StringType.get(), "Table name"),
          Types.NestedField.required(
              NAMESPACE_COLUMN_ID,
              "namespace_name",
              Types.StringType.get(),
              "Namespace identifier as a dotted string"));

  public InformationSchemaTablesTable(Catalog catalog) {
    super(catalog, Type.TABLES);
  }

  @Override
  public BatchScan newBatchScan() {
    return new TablesTableScan(new TableScanContext());
  }

  @Override
  public Schema schema() {
    return TABLES_SCHEMA;
  }

  private List<TableIdentifier> listAllTables(Expression filter, boolean caseSensitive) {
    return ImmutableList.of();
  }

  private DataTask task(TableScanContext context) {
    // extract filters on the parent column, which can be used to limit catalog queries
    Expression filters =
        ExpressionUtil.extractByIdInclusive(
            context.rowFilter(),
            TABLES_SCHEMA,
            context.caseSensitive(),
            NAME_COLUMN_ID,
            NAMESPACE_COLUMN_ID);

    // list namespaces recursively while applying filters
    List<TableIdentifier> tables = listAllTables(filters, context.caseSensitive());

    return StaticDataTask.of(
        new EmptyInputFile("information_schema:tables"),
        TABLES_SCHEMA,
        TABLES_SCHEMA,
        tables,
        table -> StaticDataTask.Row.of(table.toString()));
  }

  private class TablesTableScan extends BaseInformationSchemaTableScan {

    protected TablesTableScan(TableScanContext context) {
      super(InformationSchemaTablesTable.this, context);
    }

    @Override
    protected TablesTableScan newRefinedScan(TableScanContext newContext) {
      return new TablesTableScan(newContext);
    }

    @Override
    public Schema schema() {
      // this table does not project columns
      return TABLES_SCHEMA;
    }

    @Override
    public CloseableIterable<ScanTask> planFiles() {
      return CloseableIterable.withNoopClose(task(context()));
    }
  }
}
