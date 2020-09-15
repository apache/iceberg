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

package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Test {@link FlinkInputFormat}.
 */
public class TestFlinkInputFormat extends TestFlinkScan {

  private FlinkSource.Builder builder;

  public TestFlinkInputFormat(String fileFormat) {
    super(fileFormat);
  }

  @Override
  public void before() throws IOException {
    super.before();
    builder = FlinkSource.forRowData().tableLoader(TableLoader.fromHadoopTable(warehouse + "/default/t"));
  }

  @Override
  protected List<Row> execute(Table table, List<String> projectFields) throws IOException {
    Schema projected = new Schema(projectFields.stream().map(f ->
        table.schema().asStruct().field(f)).collect(Collectors.toList()));
    return run(builder.project(FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(projected))).buildFormat());
  }

  @Override
  protected List<Row> execute(Table table, ScanOptions options) throws IOException {
    return run(builder.options(options).buildFormat());
  }

  @Override
  protected List<Row> execute(Table table, List<Expression> filters, String sqlFilter) throws IOException {
    return run(builder.filters(filters).buildFormat());
  }

  @Override
  protected void assertResiduals(
      Schema schema, List<Row> results, List<Record> writeRecords, List<Record> filteredRecords) {
    // can not filter the data.
    assertRecords(results, writeRecords, schema);
  }

  @Override
  protected void assertNestedProjection(Table table, List<Record> records) throws IOException {
    TableSchema projectedSchema = TableSchema.builder()
        .field("nested", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING())))
        .field("data", DataTypes.STRING()).build();
    List<Row> result = run(builder.project(projectedSchema).buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : records) {
      Row nested = Row.of(((Record) record.get(1)).get(1));
      expected.add(Row.of(nested, record.get(0)));
    }

    assertRows(result, expected);
  }

  private List<Row> run(FlinkInputFormat inputFormat) throws IOException {
    FlinkInputSplit[] splits = inputFormat.createInputSplits(0);
    List<Row> results = Lists.newArrayList();

    RowType rowType = FlinkSchemaUtil.convert(inputFormat.projectedSchema());

    DataStructureConverter<Object, Object> converter = DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(rowType));

    for (FlinkInputSplit s : splits) {
      inputFormat.open(s);
      while (!inputFormat.reachedEnd()) {
        RowData row = inputFormat.nextRecord(null);
        results.add((Row) converter.toExternal(row));
      }
    }
    inputFormat.close();
    return results;
  }
}
