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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assume;
import org.junit.Test;

/** Test {@link FlinkInputFormat}. */
public class TestFlinkInputFormat extends TestFlinkSource {

  public TestFlinkInputFormat(String fileFormat) {
    super(fileFormat);
  }

  @Override
  protected List<Row> run(
      FlinkSource.Builder formatBuilder,
      Map<String, String> sqlOptions,
      String sqlFilter,
      String... sqlSelectedFields)
      throws Exception {
    return runFormat(formatBuilder.tableLoader(tableLoader()).buildFormat());
  }

  @Test
  public void testNestedProjection() throws Exception {
    Schema schema =
        new Schema(
            required(1, "data", Types.StringType.get()),
            required(
                2,
                "nested",
                Types.StructType.of(
                    Types.NestedField.required(3, "f1", Types.StringType.get()),
                    Types.NestedField.required(4, "f2", Types.StringType.get()),
                    Types.NestedField.required(5, "f3", Types.LongType.get()))),
            required(6, "id", Types.LongType.get()));

    Table table = catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), schema);

    List<Record> writeRecords = RandomGenericData.generate(schema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    // Schema: [data, nested[f1, f2, f3], id]
    // Projection: [nested.f2, data]
    // The Flink SQL output: [f2, data]
    // The FlinkInputFormat output: [nested[f2], data]

    TableSchema projectedSchema =
        TableSchema.builder()
            .field("nested", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING())))
            .field("data", DataTypes.STRING())
            .build();
    List<Row> result =
        runFormat(
            FlinkSource.forRowData()
                .tableLoader(tableLoader())
                .project(projectedSchema)
                .buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : writeRecords) {
      Row nested = Row.of(((Record) record.get(1)).get(1));
      expected.add(Row.of(nested, record.get(0)));
    }

    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testBasicProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(2, "time", Types.TimestampType.withZone()));

    Table table =
        catalogResource.catalog().createTable(TableIdentifier.of("default", "t"), writeSchema);

    List<Record> writeRecords = RandomGenericData.generate(writeSchema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    TableSchema projectedSchema =
        TableSchema.builder()
            .field("id", DataTypes.BIGINT())
            .field("data", DataTypes.STRING())
            .build();
    List<Row> result =
        runFormat(
            FlinkSource.forRowData()
                .tableLoader(tableLoader())
                .project(projectedSchema)
                .buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : writeRecords) {
      expected.add(Row.of(record.get(0), record.get(1)));
    }

    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testReadPartitionColumn() throws Exception {
    Assume.assumeTrue("Temporary skip ORC", FileFormat.ORC != fileFormat);

    Schema nestedSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "struct",
                Types.StructType.of(
                    Types.NestedField.optional(3, "innerId", Types.LongType.get()),
                    Types.NestedField.optional(4, "innerName", Types.StringType.get()))));
    PartitionSpec spec =
        PartitionSpec.builderFor(nestedSchema).identity("struct.innerName").build();

    Table table =
        catalogResource.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, nestedSchema, spec);
    List<Record> records = RandomGenericData.generate(nestedSchema, 10, 0L);
    GenericAppenderHelper appender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    for (Record record : records) {
      org.apache.iceberg.TestHelpers.Row partition =
          org.apache.iceberg.TestHelpers.Row.of(record.get(1, Record.class).get(1));
      appender.appendToTable(partition, Collections.singletonList(record));
    }

    TableSchema projectedSchema =
        TableSchema.builder()
            .field("struct", DataTypes.ROW(DataTypes.FIELD("innerName", DataTypes.STRING())))
            .build();
    List<Row> result =
        runFormat(
            FlinkSource.forRowData()
                .tableLoader(tableLoader())
                .project(projectedSchema)
                .buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : records) {
      Row nested = Row.of(((Record) record.get(1)).get(1));
      expected.add(Row.of(nested));
    }

    TestHelpers.assertRows(result, expected);
  }

  private List<Row> runFormat(FlinkInputFormat inputFormat) throws IOException {
    RowType rowType = FlinkSchemaUtil.convert(inputFormat.projectedSchema());
    return TestHelpers.readRows(inputFormat, rowType);
  }
}
