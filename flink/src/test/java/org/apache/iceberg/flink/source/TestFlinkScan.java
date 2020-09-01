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

import java.io.File;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public abstract class TestFlinkScan extends AbstractTestBase {

  private static final Schema SCHEMA = new Schema(
          required(1, "data", Types.StringType.get()),
          required(2, "id", Types.LongType.get()),
          required(3, "dt", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
          .identity("dt")
          .bucket("id", 1)
          .build();

  // before variables
  Configuration conf;
  String warehouse;
  private HadoopCatalog catalog;

  // parametrized variables
  private final FileFormat fileFormat;

  @Parameterized.Parameters(name = "format={0}")
  public static Object[] parameters() {
    return new Object[] {"avro", "parquet", "orc"};
  }

  TestFlinkScan(String fileFormat) {
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    conf = new Configuration();
    warehouse = "file:" + warehouseFile;
    catalog = new HadoopCatalog(conf, warehouse);
  }

  private List<Row> execute(Table table) throws IOException {
    return executeWithOptions(table, null, null, null, null, null, null, null, null);
  }

  private List<Row> execute(Table table, List<String> projectFields) throws IOException {
    return executeWithOptions(table, projectFields, null, null, null, null, null, null, null);
  }

  protected abstract List<Row> executeWithOptions(
      Table table, List<String> projectFields, CatalogLoader loader, Long snapshotId,
      Long startSnapshotId, Long endSnapshotId, Long asOfTimestamp, List<Expression> filters, String sqlFilter)
      throws IOException;

  /**
   * The Flink SQL has no residuals, because there will be operator to filter all the data that should be filtered.
   * But the FlinkInputFormat can't.
   */
  protected abstract void assertResiduals(Schema schema, List<Row> results, List<Record> writeRecords,
                                          List<Record> filteredRecords) throws IOException;

  /**
   * Schema: [data, nested[f1, f2, f3], id]
   * Projection: [nested.f2, data]
   * The Flink SQL output: [f2, data]
   * The FlinkInputFormat output: [nested[f2], data].
   */
  protected abstract void assertNestedProjection(Table table, List<Record> records) throws IOException;

  @Test
  public void testUnpartitionedTable() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);
    List<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 2, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(expectedRecords);
    assertRecords(execute(table), expectedRecords, SCHEMA);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);
    List<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(
        org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    assertRecords(execute(table), expectedRecords, SCHEMA);
  }

  @Test
  public void testProjection() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);
    List<Record> inputRecords = RandomGenericData.generate(SCHEMA, 1, 0L);
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(
        org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), inputRecords);
    assertRows(execute(table, Collections.singletonList("data")), Row.of(inputRecords.get(0).get(0)));
  }

  @Test
  public void testIdentityPartitionProjections() throws Exception {
    Schema logSchema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "dt", Types.StringType.get()),
        Types.NestedField.optional(3, "level", Types.StringType.get()),
        Types.NestedField.optional(4, "message", Types.StringType.get())
    );
    PartitionSpec spec =
        PartitionSpec.builderFor(logSchema).identity("dt").identity("level").build();

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), logSchema, spec);
    List<Record> inputRecords = RandomGenericData.generate(logSchema, 10, 0L);

    int idx = 0;
    AppendFiles append = table.newAppend();
    for (Record record : inputRecords) {
      record.set(1, "2020-03-2" + idx);
      record.set(2, Integer.toString(idx));
      append.appendFile(new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).writeFile(
          org.apache.iceberg.TestHelpers.Row.of("2020-03-2" + idx, Integer.toString(idx)), ImmutableList.of(record)));
      idx += 1;
    }
    append.commit();

    // individual fields
    validateIdentityPartitionProjections(table, Collections.singletonList("dt"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("level"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("message"), inputRecords);
    validateIdentityPartitionProjections(table, Collections.singletonList("id"), inputRecords);
    // field pairs
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "level"), inputRecords);
    // out-of-order pairs
    validateIdentityPartitionProjections(table, Arrays.asList("message", "dt"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("message", "level"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "dt"), inputRecords);
    // out-of-order triplets
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "level", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "dt", "message"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("dt", "message", "level"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("level", "message", "dt"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("message", "dt", "level"), inputRecords);
    validateIdentityPartitionProjections(table, Arrays.asList("message", "level", "dt"), inputRecords);
  }

  private void validateIdentityPartitionProjections(Table table, List<String> projectedFields,
      List<Record> inputRecords) throws IOException {
    List<Row> rows = execute(table, projectedFields);

    for (int pos = 0; pos < inputRecords.size(); pos++) {
      Record inputRecord = inputRecords.get(pos);
      Row actualRecord = rows.get(pos);

      for (int i = 0; i < projectedFields.size(); i++) {
        String name = projectedFields.get(i);
        Assert.assertEquals(
            "Projected field " + name + " should match", inputRecord.getField(name), actualRecord.getField(i));
      }
    }
  }

  @Test
  public void testSnapshotReads() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 1, 0L);
    helper.appendToTable(expectedRecords);
    long snapshotId = table.currentSnapshot().snapshotId();

    long timestampMillis = table.currentSnapshot().timestampMillis();

    // produce another timestamp
    Thread.sleep(10);
    helper.appendToTable(RandomGenericData.generate(SCHEMA, 1, 0L));

    assertRecords(
        executeWithOptions(table, null, null, snapshotId, null, null, null, null, null), expectedRecords, SCHEMA);
    assertRecords(
        executeWithOptions(table, null, null, null, null, null, timestampMillis, null, null), expectedRecords, SCHEMA);
  }

  @Test
  public void testIncrementalRead() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA);

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> records1 = RandomGenericData.generate(SCHEMA, 1, 0L);
    helper.appendToTable(records1);
    long snapshotId1 = table.currentSnapshot().snapshotId();

    // snapshot 2
    List<Record> records2 = RandomGenericData.generate(SCHEMA, 1, 0L);
    helper.appendToTable(records2);

    List<Record> records3 = RandomGenericData.generate(SCHEMA, 1, 0L);
    helper.appendToTable(records3);
    long snapshotId3 = table.currentSnapshot().snapshotId();

    // snapshot 4
    List<Record> records4 = RandomGenericData.generate(SCHEMA, 1, 0L);
    helper.appendToTable(RandomGenericData.generate(SCHEMA, 1, 0L));

    List<Record> expected1 = Lists.newArrayList();
    expected1.addAll(records2);
    expected1.addAll(records3);
    expected1.addAll(records4);
    assertRecords(executeWithOptions(table, null, null, null, snapshotId1, null, null, null, null), expected1, SCHEMA);

    List<Record> expected2 = Lists.newArrayList();
    expected2.addAll(records2);
    expected2.addAll(records3);
    assertRecords(
        executeWithOptions(table, null, null, null, snapshotId1, snapshotId3, null, null, null), expected2, SCHEMA);
  }

  @Test
  public void testCustomCatalog() throws IOException {
    String newWarehouse = TEMPORARY_FOLDER.newFolder().toURI().toString();
    Table table = new HadoopCatalog(conf, newWarehouse).createTable(TableIdentifier.of("default", "t"), SCHEMA);

    List<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER).appendToTable(
        org.apache.iceberg.TestHelpers.Row.of("2020-03-20", 0), expectedRecords);

    List<Row> rows = executeWithOptions(
        table, null, CatalogLoader.hadoop("new_catalog", newWarehouse), null, null, null, null, null, null);
    assertRecords(rows, expectedRecords, SCHEMA);
  }

  @Test
  public void testFilterExp() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);

    List<Record> expectedRecords = RandomGenericData.generate(SCHEMA, 2, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    expectedRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), expectedRecords);
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);
    List<Expression> filters = Collections.singletonList(Expressions.equal("dt", "2020-03-20"));
    assertRecords(
        executeWithOptions(table, null, null, null, null, null, null, filters, "dt='2020-03-20'"),
        expectedRecords, SCHEMA);
  }

  @Test
  public void testResiduals() throws Exception {
    Table table = catalog.createTable(TableIdentifier.of("default", "t"), SCHEMA, SPEC);

    List<Record> writeRecords = RandomGenericData.generate(SCHEMA, 2, 0L);
    writeRecords.get(0).set(1, 123L);
    writeRecords.get(0).set(2, "2020-03-20");
    writeRecords.get(1).set(1, 456L);
    writeRecords.get(1).set(2, "2020-03-20");

    GenericAppenderHelper helper = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.add(writeRecords.get(0));

    DataFile dataFile1 = helper.writeFile(TestHelpers.Row.of("2020-03-20", 0), writeRecords);
    DataFile dataFile2 = helper.writeFile(TestHelpers.Row.of("2020-03-21", 0),
        RandomGenericData.generate(SCHEMA, 2, 0L));
    helper.appendToTable(dataFile1, dataFile2);

    List<Expression> filters = Arrays.asList(Expressions.equal("dt", "2020-03-20"), Expressions.equal("id", 123));
    assertResiduals(
        SCHEMA, executeWithOptions(table, null, null, null, null, null, null, filters, "dt='2020-03-20' and id=123"),
        writeRecords, expectedRecords);
  }

  @Test
  public void testPartitionTypes() throws Exception {
    Schema typesSchema = new Schema(
        Types.NestedField.optional(1, "id", Types.IntegerType.get()),
        Types.NestedField.optional(2, "decimal", Types.DecimalType.of(38, 18)),
        Types.NestedField.optional(3, "str", Types.StringType.get()),
        Types.NestedField.optional(4, "binary", Types.BinaryType.get()),
        Types.NestedField.optional(5, "date", Types.DateType.get()),
        Types.NestedField.optional(6, "time", Types.TimeType.get()),
        Types.NestedField.optional(7, "timestamp", Types.TimestampType.withoutZone())
    );
    PartitionSpec spec = PartitionSpec.builderFor(typesSchema).identity("decimal").identity("str").identity("binary")
        .identity("date").identity("time").identity("timestamp").build();

    Table table = catalog.createTable(TableIdentifier.of("default", "t"), typesSchema, spec);
    List<Record> records = RandomGenericData.generate(typesSchema, 10, 0L);
    GenericAppenderHelper appender = new GenericAppenderHelper(table, fileFormat, TEMPORARY_FOLDER);
    for (Record record : records) {
      TestHelpers.Row partition = TestHelpers.Row.of(
          record.get(1),
          record.get(2),
          record.get(3),
          record.get(4) == null ? null : DateTimeUtil.daysFromDate((LocalDate) record.get(4)),
          record.get(5) == null ? null : DateTimeUtil.microsFromTime((LocalTime) record.get(5)),
          record.get(6) == null ? null : DateTimeUtil.microsFromTimestamp((LocalDateTime) record.get(6)));
      appender.appendToTable(partition, Collections.singletonList(record));
    }

    assertRecords(execute(table), records, typesSchema);
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

    assertNestedProjection(table, writeRecords);
  }

  static void assertRecords(List<Row> results, List<Record> expectedRecords, Schema schema) {
    List<Row> expected = Lists.newArrayList();
    @SuppressWarnings("unchecked")
    DataStructureConverter<RowData, Row> converter = (DataStructureConverter) DataStructureConverters.getConverter(
        TypeConversions.fromLogicalToDataType(FlinkSchemaUtil.convert(schema)));
    expectedRecords.forEach(r -> expected.add(converter.toExternal(RowDataConverter.convert(schema, r))));
    assertRows(results, expected);
  }

  private static void assertRows(List<Row> results, Row... expected) {
    assertRows(results, Arrays.asList(expected));
  }

  static void assertRows(List<Row> results, List<Row> expected) {
    expected.sort(Comparator.comparing(Row::toString));
    results.sort(Comparator.comparing(Row::toString));
    Assert.assertEquals(expected, results);
  }
}
