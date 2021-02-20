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
import java.util.Map;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * Test {@link FlinkInputFormat}.
 */
public class TestFlinkInputFormat extends TestFlinkSource {

  public TestFlinkInputFormat(String fileFormat) {
    super(fileFormat);
  }

  @Override
  public void before() throws IOException {
    super.before();
  }

  @Override
  protected List<Row> run(
      FlinkSource.Builder formatBuilder, Map<String, String> sqlOptions, String sqlFilter, String... sqlSelectedFields)
      throws Exception {
    return runFormat(formatBuilder.tableLoader(tableLoader()).buildFormat());
  }

  @Test
  public void testNestedProjection() throws Exception {
    Schema schema = new Schema(
        required(1, "data", Types.StringType.get()),
        required(2, "nested", Types.StructType.of(
            Types.NestedField.required(3, "f1", Types.StringType.get()),
            Types.NestedField.required(4, "f2", Types.StringType.get()),
            Types.NestedField.required(5, "f3", Types.LongType.get()))),
        required(6, "id", Types.LongType.get()));

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), schema);

    List<Record> writeRecords = RandomGenericData.generate(schema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    // Schema: [data, nested[f1, f2, f3], id]
    // Projection: [nested.f2, data]
    // The Flink SQL output: [f2, data]
    // The FlinkInputFormat output: [nested[f2], data]

    TableSchema projectedSchema = TableSchema.builder()
        .field("nested", DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.STRING())))
        .field("data", DataTypes.STRING()).build();
    List<Row> result = runFormat(FlinkSource.forRowData()
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
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(2, "time", Types.TimestampType.withZone())
    );

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), writeSchema);

    List<Record> writeRecords = RandomGenericData.generate(writeSchema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    TableSchema projectedSchema = TableSchema.builder()
        .field("id", DataTypes.BIGINT())
        .field("data", DataTypes.STRING())
        .build();
    List<Row> result = runFormat(FlinkSource.forRowData()
        .tableLoader(tableLoader()).project(projectedSchema).buildFormat());

    List<Row> expected = Lists.newArrayList();
    for (Record record : writeRecords) {
      expected.add(Row.of(record.get(0), record.get(1)));
    }

    TestHelpers.assertRows(result, expected);
  }

  @Test
  public void testSnapshotReadsWithAddColumn() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), writeSchema);
    List<Record> writeRecords = RandomGenericData.generate(writeSchema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    long timestampMillis = table.currentSnapshot().timestampMillis();
    waitUntilAfter(timestampMillis);

    table.updateSchema().addColumn("d", Types.IntegerType.get()).commit();

    Schema writeSchema1 = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(2, "d", Types.IntegerType.get())
    );

    List<Record> writeRecords1 = RandomGenericData.generate(writeSchema1, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords1);

    table.refresh();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    waitUntilAfter(timestampMillis1);

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("snapshot-id", Long.toString(snapshotId))),
        writeRecords, writeSchema);

    for (Record record : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(writeSchema1);
      genericRecord.set(0, record.get(0));
      genericRecord.set(1, record.get(1));
      genericRecord.set(2, null);
      writeRecords1.add(genericRecord);
    }

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("snapshot-id", Long.toString(snapshotId1))),
        writeRecords1, writeSchema1);
  }

  @Test
  public void testSnapshotReadsWithDeleteColumn() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(2, "d", Types.IntegerType.get())
    );

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), writeSchema);
    List<Record> writeRecords = RandomGenericData.generate(writeSchema, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords);

    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    long timestampMillis = table.currentSnapshot().timestampMillis();
    waitUntilAfter(timestampMillis);

    table.updateSchema().deleteColumn("d").commit();

    Schema writeSchema1 = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    List<Record> writeRecords1 = RandomGenericData.generate(writeSchema1, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(writeRecords1);

    table.refresh();
    long snapshotId1 = table.currentSnapshot().snapshotId();
    long timestampMillis1 = table.currentSnapshot().timestampMillis();
    waitUntilAfter(timestampMillis1);

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("snapshot-id", Long.toString(snapshotId))),
        writeRecords, writeSchema);

    for (Record record : writeRecords) {
      GenericRecord genericRecord = GenericRecord.create(writeSchema1);
      genericRecord.set(0, record.get(0));
      genericRecord.set(1, record.get(1));
      writeRecords1.add(genericRecord);
    }

    TestHelpers.assertRecords(
        runWithOptions(ImmutableMap.of("snapshot-id", Long.toString(snapshotId1))),
        writeRecords1, writeSchema1);
  }

  private List<Row> runFormat(FlinkInputFormat inputFormat) throws IOException {
    RowType rowType = FlinkSchemaUtil.convert(inputFormat.projectedSchema());
    return TestHelpers.readRows(inputFormat, rowType);
  }
}
