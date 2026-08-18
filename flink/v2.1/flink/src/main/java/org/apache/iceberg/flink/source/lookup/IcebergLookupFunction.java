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
package org.apache.iceberg.flink.source.lookup;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkRowData;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.data.RowDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergLookupFunction extends LookupFunction {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergLookupFunction.class);

  private final TableLoader tableLoader;
  private final String[] projectedColumns;
  private final RowType projectedRowType;
  private final int[] lookupKeyIndices;
  private final List<Expression> pushedFilters;

  private transient IcebergLookUpReader reader;
  private transient RowDataWrapper lookupKeyWrapper;
  private transient RowData.FieldGetter[] rowFieldGetters;
  private transient TypeSerializer[] fieldSerializers;

  public IcebergLookupFunction(
      TableLoader tableLoader,
      String[] projectedColumns,
      RowType projectedRowType,
      int[] keyIndices,
      List<Expression> pushedFilters) {
    this.tableLoader = tableLoader;
    this.projectedColumns = projectedColumns;
    this.projectedRowType = projectedRowType;
    this.lookupKeyIndices = keyIndices;
    this.pushedFilters = pushedFilters == null ? ImmutableList.of() : pushedFilters;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    LOG.info(
        "IcebergLookupFunction opening, projected fields={}, keyIndices={}",
        Arrays.toString(projectedColumns),
        Arrays.toString(lookupKeyIndices));

    tableLoader.open();
    Table table = tableLoader.loadTable();
    Schema tableSchema = table.schema();
    Schema icebergProjection = tableSchema.select(projectedColumns);

    LogicalType[] lookupKeyTypes = new LogicalType[lookupKeyIndices.length];
    List<Types.NestedField> lookupKeyFields = Lists.newArrayList();
    for (int i = 0; i < lookupKeyIndices.length; i++) {
      int projectedIndex = lookupKeyIndices[i];
      lookupKeyTypes[i] = projectedRowType.getTypeAt(projectedIndex);
      lookupKeyFields.add(tableSchema.findField(projectedColumns[projectedIndex]));
    }

    this.lookupKeyWrapper =
        new RowDataWrapper(RowType.of(lookupKeyTypes), Types.StructType.of(lookupKeyFields));

    this.rowFieldGetters = new RowData.FieldGetter[projectedRowType.getFieldCount()];
    for (int i = 0; i < projectedRowType.getFieldCount(); i++) {
      this.rowFieldGetters[i] = FlinkRowData.createFieldGetter(projectedRowType.getTypeAt(i), i);
    }

    this.fieldSerializers =
        projectedRowType.getChildren().stream()
            .map(InternalSerializers::create)
            .toArray(TypeSerializer[]::new);

    this.reader = new IcebergLookUpReader(table, icebergProjection, pushedFilters, false, null);
  }

  @Override
  public Collection<RowData> lookup(RowData keyRow) throws IOException {
    List<RowData> rows = Lists.newArrayList();
    reader.read(buildKeyFilter(keyRow), row -> rows.add(copyRow(row)));
    return rows;
  }

  @Override
  public void close() throws Exception {
    if (tableLoader != null) {
      tableLoader.close();
    }

    super.close();
  }

  private Expression buildKeyFilter(RowData keyRow) {
    Expression filter = Expressions.alwaysTrue();
    RowDataWrapper wrappedKey = lookupKeyWrapper.wrap(keyRow);
    for (int i = 0; i < lookupKeyIndices.length; i++) {
      int projectedIndex = lookupKeyIndices[i];
      Object value = wrappedKey.get(i, Object.class);
      String columnName = projectedColumns[projectedIndex];
      Expression keyFilter =
          value == null ? Expressions.isNull(columnName) : Expressions.equal(columnName, value);
      filter = Expressions.and(filter, keyFilter);
    }

    return filter;
  }

  private RowData copyRow(RowData row) {
    return RowDataUtil.clone(
        row,
        new GenericRowData(projectedRowType.getFieldCount()),
        projectedRowType,
        fieldSerializers,
        rowFieldGetters);
  }
}
