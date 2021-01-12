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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.runners.Parameterized.Parameter;
import static org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestHiveIcebergStorageHandlerLocalScan {

  @Parameters(name = "fileFormat={0}, catalog={1}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = new ArrayList<>();

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      testParams.add(new Object[] {fileFormat, TestTables.TestTableType.HIVE_CATALOG});
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) - skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[]{FileFormat.PARQUET, testTableType});
      }
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(0)
  public FileFormat fileFormat;

  @Parameter(1)
  public TestTables.TestTableType testTableType;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterClass
  public static void afterClass() {
    shell.stop();
  }

  @Before
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    // Uses spark as an engine so we can detect if we unintentionally try to use any execution engines
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, "spark");
  }

  @After
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
  }

  @Test
  public void testScanEmptyTable() throws IOException {
    Schema emptySchema = new Schema(required(1, "empty", Types.StringType.get()));
    testTables.createTable(shell, "empty", emptySchema, fileFormat, ImmutableList.of());

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.empty");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testScanTable() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Single fetch task: no MR job.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, rows.get(2));
  }

  @Test
  public void testScanTableCaseInsensitive() throws IOException {
    testTables.createTable(shell, "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA_WITH_UPPERCASE, fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");

    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy", "Pink"}, rows.get(2));

    rows = shell.executeStatement("SELECT * FROM default.customers where CustomER_Id < 2 " +
            "and first_name in ('Alice', 'Bob')");

    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice", "Brown"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob", "Green"}, rows.get(1));
  }


  @Test
  public void testDecimalTableWithPredicateLiterals() throws IOException {
    Schema schema = new Schema(required(1, "decimal_field", Types.DecimalType.of(7, 2)));
    List<Record> records = TestHelper.RecordsBuilder.newInstance(schema)
            .add(new BigDecimal("85.00"))
            .add(new BigDecimal("100.56"))
            .add(new BigDecimal("100.57"))
            .build();
    testTables.createTable(shell, "dec_test", schema, fileFormat, records);

    // Use integer literal in predicate
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field >= 85");
    Assert.assertEquals(3, rows.size());
    Assert.assertArrayEquals(new Object[] {"85.00"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(1));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(2));

    // Use decimal literal in predicate with smaller scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 99.1");
    Assert.assertEquals(2, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.56"}, rows.get(0));
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(1));

    // Use decimal literal in predicate with higher scale than schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 100.565");
    Assert.assertEquals(1, rows.size());
    Assert.assertArrayEquals(new Object[] {"100.57"}, rows.get(0));

    // Use decimal literal in predicate with the same scale as schema type definition
    rows = shell.executeStatement("SELECT * FROM default.dec_test where decimal_field > 640.34");
    Assert.assertEquals(0, rows.size());
  }

  @Test
  public void testColumnSelection() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> outOfOrderColumns = shell
            .executeStatement("SELECT first_name, customer_id, last_name FROM default.customers");

    Assert.assertEquals(3, outOfOrderColumns.size());
    Assert.assertArrayEquals(new Object[] {"Alice", 0L, "Brown"}, outOfOrderColumns.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", 1L, "Green"}, outOfOrderColumns.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", 2L, "Pink"}, outOfOrderColumns.get(2));

    List<Object[]> allButFirstColumn = shell.executeStatement("SELECT first_name, last_name FROM default.customers");

    Assert.assertEquals(3, allButFirstColumn.size());
    Assert.assertArrayEquals(new Object[] {"Alice", "Brown"}, allButFirstColumn.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", "Green"}, allButFirstColumn.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", "Pink"}, allButFirstColumn.get(2));

    List<Object[]> allButMiddleColumn = shell.executeStatement("SELECT customer_id, last_name FROM default.customers");

    Assert.assertEquals(3, allButMiddleColumn.size());
    Assert.assertArrayEquals(new Object[] {0L, "Brown"}, allButMiddleColumn.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Green"}, allButMiddleColumn.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Pink"}, allButMiddleColumn.get(2));

    List<Object[]> allButLastColumn = shell.executeStatement("SELECT customer_id, first_name FROM default.customers");

    Assert.assertEquals(3, allButLastColumn.size());
    Assert.assertArrayEquals(new Object[] {0L, "Alice"}, allButLastColumn.get(0));
    Assert.assertArrayEquals(new Object[] {1L, "Bob"}, allButLastColumn.get(1));
    Assert.assertArrayEquals(new Object[] {2L, "Trudy"}, allButLastColumn.get(2));
  }

  @Test
  public void selectSameColumnTwice() throws IOException {
    testTables.createTable(shell, "customers", HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    List<Object[]> columns = shell.executeStatement("SELECT first_name, first_name FROM default.customers");

    Assert.assertEquals(3, columns.size());
    Assert.assertArrayEquals(new Object[] {"Alice", "Alice"}, columns.get(0));
    Assert.assertArrayEquals(new Object[] {"Bob", "Bob"}, columns.get(1));
    Assert.assertArrayEquals(new Object[] {"Trudy", "Trudy"}, columns.get(2));
  }

  @Test
  public void testCreateTableWithColumnSpecification() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    Map<StructLike, List<Record>> data = new HashMap<>(1);
    data.put(null, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    String createSql = "CREATE EXTERNAL TABLE " + identifier +
        " (customer_id BIGINT, first_name STRING, last_name STRING)" +
        " STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier);
    runCreateAndReadTest(identifier, createSql, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(), data);
  }

  @Test
  public void testCreateTableWithColumnSpecificationPartitioned() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name").build();
    Map<StructLike, List<Record>> data = ImmutableMap.of(
        Row.of("Brown"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(0)),
        Row.of("Green"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(1)),
        Row.of("Pink"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2)));
    String createSql = "CREATE EXTERNAL TABLE " + identifier +
        " (customer_id BIGINT, first_name STRING) PARTITIONED BY (last_name STRING) " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier);
    runCreateAndReadTest(identifier, createSql, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, data);
  }

  @Test
  public void testCreatePartitionedTableByProperty() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA).identity("last_name").build();
    Map<StructLike, List<Record>> data = ImmutableMap.of(
        Row.of("Brown"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(0)),
        Row.of("Green"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(1)),
        Row.of("Pink"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2)));
    String createSql = "CREATE EXTERNAL TABLE " + identifier +
        " STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier) +
        "TBLPROPERTIES ('" + InputFormatConfig.PARTITION_SPEC + "'='" + PartitionSpecParser.toJson(spec) + "', " +
        "'" + InputFormatConfig.TABLE_SCHEMA + "'='" +
        SchemaParser.toJson(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA) + "')";
    runCreateAndReadTest(identifier, createSql, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, data);
  }

  @Test
  public void testCreateTableWithColumnSpecificationMultilevelPartitioned() throws IOException {
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    PartitionSpec spec = PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
        .identity("first_name").identity("last_name").build();
    Map<StructLike, List<Record>> data = ImmutableMap.of(
        Row.of("Alice", "Brown"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(0)),
        Row.of("Bob", "Green"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(1)),
        Row.of("Trudy", "Pink"), Collections.singletonList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.get(2)));
    String createSql = "CREATE EXTERNAL TABLE " + identifier + " (customer_id BIGINT) " +
        "PARTITIONED BY (first_name STRING, last_name STRING) " +
        "STORED BY 'org.apache.iceberg.mr.hive.HiveIcebergStorageHandler' " +
        testTables.locationForCreateTableSQL(identifier);
    runCreateAndReadTest(identifier, createSql, HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, spec, data);
  }

  @Test
  public void testArrayOfPrimitivesInTable() throws IOException {
    Schema schema =
            new Schema(required(1, "arrayofprimitives", Types.ListType.ofRequired(2, Types.IntegerType.get())));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access a single element from the array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofprimitives");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT arrayofprimitives[%d] FROM default.arraytable " + "LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testArrayOfArraysInTable() throws IOException {
    Schema schema =
            new Schema(
                    required(1, "arrayofarrays",
                            Types.ListType.ofRequired(2, Types.ListType.ofRequired(3, Types.DateType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a matrix
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofarrays");
      for (int j = 0; j < expectedList.size(); j++) {
        List<?> expectedInnerList = (List<?>) expectedList.get(j);
        for (int k = 0; k < expectedInnerList.size(); k++) {
          List<Object[]> queryResult = shell.executeStatement(
                  String.format("SELECT arrayofarrays[%d][%d] FROM default.arraytable " + "LIMIT 1 OFFSET %d",
                          j, k, i));
          Assert.assertEquals(expectedInnerList.get(k).toString(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testArrayOfMapsInTable() throws IOException {
    Schema schema =
            new Schema(required(1, "arrayofmaps", Types.ListType
                    .ofRequired(2, Types.MapType.ofRequired(3, 4, Types.StringType.get(),
                            Types.BooleanType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a map in an array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofmaps");
      for (int j = 0; j < expectedList.size(); j++) {
        Map<?, ?> expectedMap = (Map<?, ?>) expectedList.get(j);
        for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT arrayofmaps[%d][\"%s\"] FROM default.arraytable LIMIT 1 OFFSET %d", j,
                          entry.getKey(), i));
          Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testArrayOfStructsInTable() throws IOException {
    Schema schema =
            new Schema(
                    required(1, "arrayofstructs", Types.ListType.ofRequired(2, Types.StructType
                            .of(required(3, "something", Types.DoubleType.get()), required(4, "someone",
                                    Types.LongType.get()), required(5, "somewhere", Types.StringType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "arraytable", schema, fileFormat, 1);
    // access an element from a struct in an array
    for (int i = 0; i < records.size(); i++) {
      List<?> expectedList = (List<?>) records.get(i).getField("arrayofstructs");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(String.format("SELECT arrayofstructs[%d].something, " +
                "arrayofstructs[%d].someone, arrayofstructs[%d].somewhere FROM default.arraytable LIMIT 1 " +
                "OFFSET %d", j, j, j, i));
        GenericRecord genericRecord = (GenericRecord) expectedList.get(j);
        Assert.assertEquals(genericRecord.getField("something"), queryResult.get(0)[0]);
        Assert.assertEquals(genericRecord.getField("someone"), queryResult.get(0)[1]);
        Assert.assertEquals(genericRecord.getField("somewhere"), queryResult.get(0)[2]);
      }
    }
  }

  @Test
  public void testMapOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofprimitives", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.IntegerType.get())));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single value from the map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofprimitives");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT mapofprimitives[\"%s\"] " + "FROM default.maptable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testMapOfArraysInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofarrays",
                    Types.MapType.ofRequired(2, 3, Types.StringType.get(), Types.ListType.ofRequired(4,
                            Types.DateType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a list in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofarrays");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<?> expectedList = (List<?>) entry.getValue();
        for (int j = 0; j < expectedList.size(); j++) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT mapofarrays[\"%s\"]" + "[%d] FROM maptable LIMIT 1 OFFSET %d", entry.getKey(), j, i));
          Assert.assertEquals(expectedList.get(j).toString(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testMapOfMapsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofmaps", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a map in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofmaps");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        Map<?, ?> expectedInnerMap = (Map<?, ?>) entry.getValue();
        for (Map.Entry<?, ?> innerEntry : expectedInnerMap.entrySet()) {
          List<Object[]> queryResult = shell.executeStatement(String
                  .format("SELECT mapofmaps[\"%s\"]" + "[\"%s\"] FROM maptable LIMIT 1 OFFSET %d", entry.getKey(),
                          innerEntry.getKey(), i));
          Assert.assertEquals(innerEntry.getValue(), queryResult.get(0)[0]);
        }
      }
    }
  }

  @Test
  public void testMapOfStructsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "mapofstructs", Types.MapType.ofRequired(2, 3, Types.StringType.get(),
                    Types.StructType.of(required(4, "something", Types.DoubleType.get()),
                            required(5, "someone", Types.LongType.get()),
                            required(6, "somewhere", Types.StringType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "maptable", schema, fileFormat, 1);
    // access a single element from a struct in a map
    for (int i = 0; i < records.size(); i++) {
      Map<?, ?> expectedMap = (Map<?, ?>) records.get(i).getField("mapofstructs");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String.format("SELECT mapofstructs[\"%s\"].something, " +
                "mapofstructs[\"%s\"].someone, mapofstructs[\"%s\"].somewhere FROM default.maptable LIMIT 1 " +
                "OFFSET %d", entry.getKey(), entry.getKey(), entry.getKey(), i));
        GenericRecord genericRecord = (GenericRecord) entry.getValue();
        Assert.assertEquals(genericRecord.getField("something"), queryResult.get(0)[0]);
        Assert.assertEquals(genericRecord.getField("someone"), queryResult.get(0)[1]);
        Assert.assertEquals(genericRecord.getField("somewhere"), queryResult.get(0)[2]);
      }
    }
  }

  @Test
  public void testStructOfPrimitivesInTable() throws IOException {
    Schema schema = new Schema(required(1, "structofprimitives",
            Types.StructType.of(required(2, "key", Types.StringType.get()), required(3, "value",
                    Types.IntegerType.get()))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a single value in a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofprimitives");
      List<Object[]> queryResult = shell.executeStatement(String.format(
              "SELECT structofprimitives.key, structofprimitives.value FROM default.structtable LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedStruct.getField("value"), queryResult.get(0)[1]);
    }
  }

  @Test
  public void testStructOfArraysInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofarrays", Types.StructType
                    .of(required(2, "names", Types.ListType.ofRequired(3, Types.StringType.get())),
                            required(4, "birthdays", Types.ListType.ofRequired(5,
                                    Types.DateType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access an element of an array inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofarrays");
      List<?> expectedList = (List<?>) expectedStruct.getField("names");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT structofarrays.names[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j), queryResult.get(0)[0]);
      }
      expectedList = (List<?>) expectedStruct.getField("birthdays");
      for (int j = 0; j < expectedList.size(); j++) {
        List<Object[]> queryResult = shell.executeStatement(
                String.format("SELECT structofarrays.birthdays[%d] FROM default.structtable LIMIT 1 OFFSET %d", j, i));
        Assert.assertEquals(expectedList.get(j).toString(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfMapsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofmaps", Types.StructType
                    .of(required(2, "map1", Types.MapType.ofRequired(3, 4,
                            Types.StringType.get(), Types.StringType.get())), required(5, "map2",
                            Types.MapType.ofRequired(6, 7, Types.StringType.get(),
                                    Types.IntegerType.get())))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a map entry inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofmaps");
      Map<?, ?> expectedMap = (Map<?, ?>) expectedStruct.getField("map1");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT structofmaps.map1[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
      expectedMap = (Map<?, ?>) expectedStruct.getField("map2");
      for (Map.Entry<?, ?> entry : expectedMap.entrySet()) {
        List<Object[]> queryResult = shell.executeStatement(String
                .format("SELECT structofmaps.map2[\"%s\"] from default.structtable LIMIT 1 OFFSET %d", entry.getKey(),
                        i));
        Assert.assertEquals(entry.getValue(), queryResult.get(0)[0]);
      }
    }
  }

  @Test
  public void testStructOfStructsInTable() throws IOException {
    Schema schema = new Schema(
            required(1, "structofstructs", Types.StructType.of(required(2, "struct1", Types.StructType
                    .of(required(3, "key", Types.StringType.get()), required(4, "value",
                            Types.IntegerType.get()))))));
    List<Record> records = testTables.createTableWithGeneratedRecords(shell, "structtable", schema, fileFormat, 1);
    // access a struct element inside a struct
    for (int i = 0; i < records.size(); i++) {
      GenericRecord expectedStruct = (GenericRecord) records.get(i).getField("structofstructs");
      GenericRecord expectedInnerStruct = (GenericRecord) expectedStruct.getField("struct1");
      List<Object[]> queryResult = shell.executeStatement(String.format(
              "SELECT structofstructs.struct1.key, structofstructs.struct1.value FROM default.structtable " +
                      "LIMIT 1 OFFSET %d", i));
      Assert.assertEquals(expectedInnerStruct.getField("key"), queryResult.get(0)[0]);
      Assert.assertEquals(expectedInnerStruct.getField("value"), queryResult.get(0)[1]);
    }
  }

  private void runCreateAndReadTest(TableIdentifier identifier, String createSQL, Schema expectedSchema,
      PartitionSpec expectedSpec, Map<StructLike, List<Record>> data) throws IOException {
    shell.executeStatement(createSQL);

    org.apache.iceberg.Table icebergTable = testTables.loadTable(identifier);
    Assert.assertEquals(expectedSchema.asStruct(), icebergTable.schema().asStruct());
    Assert.assertEquals(expectedSpec, icebergTable.spec());

    List<Record> expected = Lists.newArrayList();
    for (StructLike partition : data.keySet()) {
      testTables.appendIcebergTable(shell.getHiveConf(), icebergTable, fileFormat, partition, data.get(partition));
      expected.addAll(data.get(partition));
    }

    List<Object[]> descRows = shell.executeStatement("SELECT * FROM " + identifier.toString());
    List<Record> records = HiveIcebergTestUtils.valueForRow(icebergTable.schema(), descRows);

    HiveIcebergTestUtils.validateData(expected, records, 0);
  }
}
