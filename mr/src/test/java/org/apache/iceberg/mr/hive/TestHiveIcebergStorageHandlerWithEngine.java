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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.hive.HiveSchemaUtil;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 200_000, unit = TimeUnit.MILLISECONDS)
public class TestHiveIcebergStorageHandlerWithEngine {

  private static final String[] EXECUTION_ENGINES = new String[] {"tez", "mr"};

  private static final Schema ORDER_SCHEMA =
      new Schema(
          required(1, "order_id", Types.LongType.get()),
          required(2, "customer_id", Types.LongType.get()),
          required(3, "total", Types.DoubleType.get()),
          required(4, "product_id", Types.LongType.get()));

  private static final List<Record> ORDER_RECORDS =
      TestHelper.RecordsBuilder.newInstance(ORDER_SCHEMA)
          .add(100L, 0L, 11.11d, 1L)
          .add(101L, 0L, 22.22d, 2L)
          .add(102L, 1L, 33.33d, 3L)
          .build();

  private static final Schema PRODUCT_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "name", Types.StringType.get()),
          optional(3, "price", Types.DoubleType.get()));

  private static final List<Record> PRODUCT_RECORDS =
      TestHelper.RecordsBuilder.newInstance(PRODUCT_SCHEMA)
          .add(1L, "skirt", 11.11d)
          .add(2L, "tee", 22.22d)
          .add(3L, "watch", 33.33d)
          .build();

  private static final List<Type> SUPPORTED_TYPES =
      ImmutableList.of(
          Types.BooleanType.get(),
          Types.IntegerType.get(),
          Types.LongType.get(),
          Types.FloatType.get(),
          Types.DoubleType.get(),
          Types.DateType.get(),
          Types.TimestampType.withZone(),
          Types.TimestampType.withoutZone(),
          Types.StringType.get(),
          Types.BinaryType.get(),
          Types.DecimalType.of(3, 1),
          Types.UUIDType.get(),
          Types.FixedType.ofLength(5),
          Types.TimeType.get());

  @Parameters(name = "fileFormat={0}, engine={1}, catalog={2}, isVectorized={3}")
  public static Collection<Object[]> parameters() {
    Collection<Object[]> testParams = Lists.newArrayList();
    String javaVersion = System.getProperty("java.specification.version");

    // Run tests with every FileFormat for a single Catalog (HiveCatalog)
    for (FileFormat fileFormat : HiveIcebergStorageHandlerTestUtils.FILE_FORMATS) {
      for (String engine : EXECUTION_ENGINES) {
        // include Tez tests only for Java 8
        if (javaVersion.equals("1.8") || "mr".equals(engine)) {
          testParams.add(
              new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, false});
          // test for vectorization=ON in case of ORC format and Tez engine
          if ((fileFormat == FileFormat.PARQUET || fileFormat == FileFormat.ORC)
              && "tez".equals(engine)
              && HiveVersion.min(HiveVersion.HIVE_3)) {
            testParams.add(
                new Object[] {fileFormat, engine, TestTables.TestTableType.HIVE_CATALOG, true});
          }
        }
      }
    }

    // Run tests for every Catalog for a single FileFormat (PARQUET) and execution engine (mr)
    // skip HiveCatalog tests as they are added before
    for (TestTables.TestTableType testTableType : TestTables.ALL_TABLE_TYPES) {
      if (!TestTables.TestTableType.HIVE_CATALOG.equals(testTableType)) {
        testParams.add(new Object[] {FileFormat.PARQUET, "mr", testTableType, false});
      }
    }

    return testParams;
  }

  private static TestHiveShell shell;

  private TestTables testTables;

  @Parameter(index = 0)
  public FileFormat fileFormat;

  @Parameter(index = 1)
  public String executionEngine;

  @Parameter(index = 2)
  public TestTables.TestTableType testTableType;

  @Parameter(index = 3)
  public boolean isVectorized;

  @TempDir public Path temp;

  @BeforeAll
  public static void beforeClass() {
    shell = HiveIcebergStorageHandlerTestUtils.shell();
  }

  @AfterAll
  public static void afterClass() throws Exception {
    shell.stop();
  }

  @BeforeEach
  public void before() throws IOException {
    testTables = HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    HiveIcebergStorageHandlerTestUtils.init(shell, testTables, temp, executionEngine);
    HiveConf.setBoolVar(
        shell.getHiveConf(), HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, isVectorized);
    if (isVectorized) {
      HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "none");
    } else {
      HiveConf.setVar(shell.getHiveConf(), HiveConf.ConfVars.HIVEFETCHTASKCONVERSION, "more");
    }
  }

  @AfterEach
  public void after() throws Exception {
    HiveIcebergStorageHandlerTestUtils.close(shell);
    // Mixing mr and tez jobs within the same JVM can cause problems. Mr jobs set the ExecMapper
    // status to done=false
    // at the beginning and to done=true at the end. However, tez jobs also rely on this value to
    // see if they should
    // proceed, but they do not reset it to done=false at the beginning. Therefore, without calling
    // this after each test
    // case, any tez job that follows a completed mr job will erroneously read done=true and will
    // not proceed.
    ExecMapper.setDone(false);
  }

  @TestTemplate
  public void testScanTable() throws IOException {
    testTables.createTable(
        shell,
        "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Adding the ORDER BY clause will cause Hive to spawn a local MR job this time.
    List<Object[]> descRows =
        shell.executeStatement(
            "SELECT first_name, customer_id FROM default.customers ORDER BY customer_id DESC");

    assertThat(descRows).hasSize(3);
    assertThat(descRows.get(0)).containsExactly("Trudy", 2L);
    assertThat(descRows.get(1)).containsExactly("Bob", 1L);
    assertThat(descRows.get(2)).containsExactly("Alice", 0L);
  }

  @TestTemplate
  public void testCBOWithSelectedColumnsNonOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "products", PRODUCT_SCHEMA, fileFormat, PRODUCT_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows =
        shell.executeStatement(
            "SELECT o.order_id, o.customer_id, o.total, p.name "
                + "FROM default.orders o JOIN default.products p ON o.product_id = p.id ORDER BY o.order_id");

    assertThat(rows).hasSize(3);
    assertThat(rows.get(0)).containsExactly(100L, 0L, 11.11d, "skirt");
    assertThat(rows.get(1)).containsExactly(101L, 0L, 22.22d, "tee");
    assertThat(rows.get(2)).containsExactly(102L, 1L, 33.33d, "watch");
  }

  @TestTemplate
  public void testDescribeTable() throws IOException {
    testTables.createTable(
        shell,
        "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    List<Object[]> rows = shell.executeStatement("DESCRIBE default.customers");
    assertThat(rows).hasSameSizeAs(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns());
    for (int i = 0; i < HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().size(); i++) {
      Types.NestedField field = HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA.columns().get(i);
      String comment = field.doc() == null ? "from deserializer" : field.doc();
      assertThat(rows.get(i))
          .containsExactly(
              field.name(), HiveSchemaUtil.convert(field.type()).getTypeName(), comment);
    }
  }

  @TestTemplate
  public void testCBOWithSelectedColumnsOverlapJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);
    testTables.createTable(
        shell,
        "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows =
        shell.executeStatement(
            "SELECT c.first_name, o.order_id "
                + "FROM default.orders o JOIN default.customers c ON o.customer_id = c.customer_id "
                + "ORDER BY o.order_id DESC");

    assertThat(rows).hasSize(3);
    assertThat(rows.get(0)).containsExactly("Bob", 102L);
    assertThat(rows.get(1)).containsExactly("Alice", 101L);
    assertThat(rows.get(2)).containsExactly("Alice", 100L);
  }

  @TestTemplate
  public void testCBOWithSelfJoin() throws IOException {
    shell.setHiveSessionValue("hive.cbo.enable", true);

    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    List<Object[]> rows =
        shell.executeStatement(
            "SELECT o1.order_id, o1.customer_id, o1.total "
                + "FROM default.orders o1 JOIN default.orders o2 ON o1.order_id = o2.order_id ORDER BY o1.order_id");

    assertThat(rows).hasSize(3);
    assertThat(rows.get(0)).containsExactly(100L, 0L, 11.11d);
    assertThat(rows.get(1)).containsExactly(101L, 0L, 22.22d);
    assertThat(rows.get(2)).containsExactly(102L, 1L, 33.33d);
  }

  @TestTemplate
  public void testJoinTablesSupportedTypes() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      if (isUnsupportedVectorizedTypeForHive(type)) {
        continue;
      }
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 1, 0L);

      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult =
          shell.executeStatement(
              "select s."
                  + columnName
                  + ", h."
                  + columnName
                  + " from default."
                  + tableName
                  + " s join default."
                  + tableName
                  + " h on h."
                  + columnName
                  + "=s."
                  + columnName);
      assertThat(queryResult)
          .as("Non matching record count for table " + tableName + " with type " + type)
          .hasSize(1);
    }
  }

  @TestTemplate
  public void testSelectDistinctFromTable() throws IOException {
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      if (isUnsupportedVectorizedTypeForHive(type)) {
        continue;
      }
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      String tableName = type.typeId().toString().toLowerCase() + "_table_" + i;
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema = new Schema(required(1, columnName, type));
      List<Record> records = TestHelper.generateRandomRecords(schema, 4, 0L);
      int size =
          records.stream().map(r -> r.getField(columnName)).collect(Collectors.toSet()).size();
      testTables.createTable(shell, tableName, schema, fileFormat, records);
      List<Object[]> queryResult =
          shell.executeStatement(
              "select count(distinct(" + columnName + ")) from default." + tableName);
      int distinctIds = ((Long) queryResult.get(0)[0]).intValue();
      assertThat(distinctIds).as(tableName).isEqualTo(size);
    }
  }

  @TestTemplate
  public void testInsert() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    Table table =
        testTables.createTable(
            shell,
            "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            ImmutableList.of());

    // The expected query is like
    // INSERT INTO customers VALUES (0, 'Alice'), (1, 'Bob'), (2, 'Trudy')
    StringBuilder query = new StringBuilder().append("INSERT INTO customers VALUES ");
    HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS.forEach(
        record ->
            query
                .append("(")
                .append(record.get(0))
                .append(",'")
                .append(record.get(1))
                .append("','")
                .append(record.get(2))
                .append("'),"));
    query.setLength(query.length() - 1);

    shell.executeStatement(query.toString());

    HiveIcebergTestUtils.validateData(
        table, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 0);
  }

  @TestTemplate
  public void testInsertSupportedTypes() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    for (int i = 0; i < SUPPORTED_TYPES.size(); i++) {
      Type type = SUPPORTED_TYPES.get(i);
      // TODO: remove this filter when issue #1881 is resolved
      if (type == Types.UUIDType.get() && fileFormat == FileFormat.PARQUET) {
        continue;
      }
      // TODO: remove this filter when we figure out how we could test binary types
      if (type.equals(Types.BinaryType.get()) || type.equals(Types.FixedType.ofLength(5))) {
        continue;
      }
      String columnName = type.typeId().toString().toLowerCase() + "_column";

      Schema schema =
          new Schema(required(1, "id", Types.LongType.get()), required(2, columnName, type));
      List<Record> expected = TestHelper.generateRandomRecords(schema, 5, 0L);

      Table table =
          testTables.createTable(
              shell,
              type.typeId().toString().toLowerCase() + "_table_" + i,
              schema,
              PartitionSpec.unpartitioned(),
              fileFormat,
              expected);

      HiveIcebergTestUtils.validateData(table, expected, 0);
    }
  }

  /**
   * Testing map only inserts.
   *
   * @throws IOException If there is an underlying IOException
   */
  @TestTemplate
  public void testInsertFromSelect() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    Table table =
        testTables.createTable(
            shell,
            "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    shell.executeStatement("INSERT INTO customers SELECT * FROM customers");

    // Check that everything is duplicated as expected
    List<Record> records = Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    records.addAll(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  /**
   * Testing map-reduce inserts.
   *
   * @throws IOException If there is an underlying IOException
   */
  @TestTemplate
  public void testInsertFromSelectWithOrderBy() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    Table table =
        testTables.createTable(
            shell,
            "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // We expect that there will be Mappers and Reducers here
    shell.executeStatement("INSERT INTO customers SELECT * FROM customers ORDER BY customer_id");

    // Check that everything is duplicated as expected
    List<Record> records = Lists.newArrayList(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    records.addAll(HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @TestTemplate
  public void testInsertFromSelectWithProjection() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    Table table =
        testTables.createTable(
            shell,
            "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            ImmutableList.of());
    testTables.createTable(shell, "orders", ORDER_SCHEMA, fileFormat, ORDER_RECORDS);

    shell.executeStatement(
        "INSERT INTO customers (customer_id, last_name) SELECT distinct(customer_id), 'test' FROM orders");

    List<Record> expected =
        TestHelper.RecordsBuilder.newInstance(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .add(0L, null, "test")
            .add(1L, null, "test")
            .build();

    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @TestTemplate
  public void testInsertUsingSourceTableWithSharedColumnsNames() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    List<Record> records = HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS;
    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .identity("last_name")
            .build();
    testTables.createTable(
        shell,
        "source_customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec,
        fileFormat,
        records);
    Table table =
        testTables.createTable(
            shell,
            "target_customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            spec,
            fileFormat,
            ImmutableList.of());

    // Below select from source table should produce:
    // "hive.io.file.readcolumn.names=customer_id,last_name".
    // Inserting into the target table should not fail because first_name is not selected from the
    // source table
    shell.executeStatement(
        "INSERT INTO target_customers SELECT customer_id, 'Sam', last_name FROM source_customers");

    List<Record> expected = Lists.newArrayListWithExpectedSize(records.size());
    records.forEach(
        r -> {
          Record copy = r.copy();
          copy.setField("first_name", "Sam");
          expected.add(copy);
        });
    HiveIcebergTestUtils.validateData(table, expected, 0);
  }

  @TestTemplate
  public void testInsertFromJoiningTwoIcebergTables() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .identity("last_name")
            .build();
    testTables.createTable(
        shell,
        "source_customers_1",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    testTables.createTable(
        shell,
        "source_customers_2",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        spec,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    Table table =
        testTables.createTable(
            shell,
            "target_customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            spec,
            fileFormat,
            ImmutableList.of());

    shell.executeStatement(
        "INSERT INTO target_customers SELECT a.customer_id, b.first_name, a.last_name FROM "
            + "source_customers_1 a JOIN source_customers_2 b ON a.last_name = b.last_name");

    HiveIcebergTestUtils.validateData(
        table, HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, 0);
  }

  @TestTemplate
  public void testWriteArrayOfPrimitivesInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "arrayofprimitives", Types.ListType.ofRequired(3, Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteArrayOfArraysInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "arrayofarrays",
                Types.ListType.ofRequired(
                    3, Types.ListType.ofRequired(4, Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 3, 1L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteArrayOfMapsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "arrayofmaps",
                Types.ListType.ofRequired(
                    3,
                    Types.MapType.ofRequired(
                        4, 5, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteArrayOfStructsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "arrayofstructs",
                Types.ListType.ofRequired(
                    3,
                    Types.StructType.of(
                        required(4, "something", Types.StringType.get()),
                        required(5, "someone", Types.StringType.get()),
                        required(6, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteMapOfPrimitivesInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "mapofprimitives",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.StringType.get())));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteMapOfArraysInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "mapofarrays",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.ListType.ofRequired(5, Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteMapOfMapsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "mapofmaps",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.MapType.ofRequired(
                        5, 6, Types.StringType.get(), Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteMapOfStructsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "mapofstructs",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(5, "something", Types.StringType.get()),
                        required(6, "someone", Types.StringType.get()),
                        required(7, "somewhere", Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteStructOfPrimitivesInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "structofprimitives",
                Types.StructType.of(
                    required(3, "key", Types.StringType.get()),
                    required(4, "value", Types.StringType.get()))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteStructOfArraysInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "structofarrays",
                Types.StructType.of(
                    required(3, "names", Types.ListType.ofRequired(4, Types.StringType.get())),
                    required(
                        5, "birthdays", Types.ListType.ofRequired(6, Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 1L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteStructOfMapsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "structofmaps",
                Types.StructType.of(
                    required(
                        3,
                        "map1",
                        Types.MapType.ofRequired(
                            4, 5, Types.StringType.get(), Types.StringType.get())),
                    required(
                        6,
                        "map2",
                        Types.MapType.ofRequired(
                            7, 8, Types.StringType.get(), Types.StringType.get())))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testWriteStructOfStructsInTable() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "structofstructs",
                Types.StructType.of(
                    required(
                        3,
                        "struct1",
                        Types.StructType.of(
                            required(4, "key", Types.StringType.get()),
                            required(5, "value", Types.StringType.get()))))));
    List<Record> records = TestHelper.generateRandomRecords(schema, 5, 0L);
    testComplexTypeWrite(schema, records);
  }

  @TestTemplate
  public void testPartitionedWrite() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .bucket("customer_id", 3)
            .build();

    List<Record> records =
        TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table =
        testTables.createTable(
            shell,
            "partitioned_customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            spec,
            fileFormat,
            records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @TestTemplate
  public void testIdentityPartitionedWrite() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .identity("customer_id")
            .build();

    List<Record> records =
        TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table =
        testTables.createTable(
            shell,
            "partitioned_customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            spec,
            fileFormat,
            records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @TestTemplate
  public void testMultilevelIdentityPartitionedWrite() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    PartitionSpec spec =
        PartitionSpec.builderFor(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA)
            .identity("customer_id")
            .identity("last_name")
            .build();

    List<Record> records =
        TestHelper.generateRandomRecords(HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 4, 0L);

    Table table =
        testTables.createTable(
            shell,
            "partitioned_customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            spec,
            fileFormat,
            records);

    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  @TestTemplate
  public void testMultiTableInsert() throws IOException {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");

    testTables.createTable(
        shell,
        "customers",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    Schema target1Schema =
        new Schema(
            optional(1, "customer_id", Types.LongType.get()),
            optional(2, "first_name", Types.StringType.get()));

    Schema target2Schema =
        new Schema(
            optional(1, "last_name", Types.StringType.get()),
            optional(2, "customer_id", Types.LongType.get()));

    List<Record> target1Records =
        TestHelper.RecordsBuilder.newInstance(target1Schema)
            .add(0L, "Alice")
            .add(1L, "Bob")
            .add(2L, "Trudy")
            .build();

    List<Record> target2Records =
        TestHelper.RecordsBuilder.newInstance(target2Schema)
            .add("Brown", 0L)
            .add("Green", 1L)
            .add("Pink", 2L)
            .build();

    Table target1 =
        testTables.createTable(shell, "target1", target1Schema, fileFormat, ImmutableList.of());
    Table target2 =
        testTables.createTable(shell, "target2", target2Schema, fileFormat, ImmutableList.of());

    // simple insert: should create a single vertex writing to both target tables
    shell.executeStatement(
        "FROM customers "
            + "INSERT INTO target1 SELECT customer_id, first_name "
            + "INSERT INTO target2 SELECT last_name, customer_id");

    // Check that everything is as expected
    HiveIcebergTestUtils.validateData(target1, target1Records, 0);
    HiveIcebergTestUtils.validateData(target2, target2Records, 1);

    // truncate the target tables
    testTables.truncateIcebergTable(target1);
    testTables.truncateIcebergTable(target2);

    // complex insert: should use a different vertex for each target table
    shell.executeStatement(
        "FROM customers "
            + "INSERT INTO target1 SELECT customer_id, first_name ORDER BY first_name "
            + "INSERT INTO target2 SELECT last_name, customer_id ORDER BY last_name");

    // Check that everything is as expected
    HiveIcebergTestUtils.validateData(target1, target1Records, 0);
    HiveIcebergTestUtils.validateData(target2, target2Records, 1);
  }

  /**
   * Fix vectorized parquet <a href="https://github.com/apache/iceberg/issues/4403">issue-4403</a>.
   */
  @TestTemplate
  public void testStructMapWithNull() throws IOException {
    assumeThat(!("PARQUET".equals(fileFormat.name()) && isVectorized))
        .as("Vectorized parquet throw class cast exception see : issue 4403")
        .isTrue();
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "mapofstructs",
                Types.MapType.ofRequired(
                    3,
                    4,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(5, "something", Types.StringType.get()),
                        required(6, "someone", Types.StringType.get()),
                        required(7, "somewhere", Types.StringType.get())))));

    List<Record> records =
        TestHelper.RecordsBuilder.newInstance(schema).add(0L, ImmutableMap.of()).build();

    testTables.createTable(shell, "mapwithnull", schema, fileFormat, records);

    List<Object[]> results =
        shell.executeStatement("select mapofstructs['context'].someone FROM mapwithnull");
    assertThat(results).hasSize(1);
    assertThat(results.get(0)[0]).isNull();
  }

  @TestTemplate
  public void testWriteWithDefaultWriteFormat() {
    assumeThat(
            executionEngine.equals("mr")
                && testTableType == TestTables.TestTableType.HIVE_CATALOG
                && fileFormat == FileFormat.ORC)
        .as("Testing the default file format is enough for a single scenario.")
        .isTrue();

    TableIdentifier identifier = TableIdentifier.of("default", "customers");

    // create Iceberg table without specifying a write format in the tbl properties
    // it should fall back to using the default file format
    shell.executeStatement(
        String.format(
            "CREATE EXTERNAL TABLE %s (id bigint, name string) STORED BY '%s' %s",
            identifier,
            HiveIcebergStorageHandler.class.getName(),
            testTables.locationForCreateTableSQL(identifier)));

    shell.executeStatement(String.format("INSERT INTO %s VALUES (10, 'Linda')", identifier));
    List<Object[]> results = shell.executeStatement(String.format("SELECT * FROM %s", identifier));

    assertThat(results).hasSize(1);
    assertThat(results.get(0)).containsExactly(10L, "Linda");
  }

  @TestTemplate
  public void testInsertEmptyResultSet() throws IOException {
    Table source =
        testTables.createTable(
            shell,
            "source",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            ImmutableList.of());
    Table target =
        testTables.createTable(
            shell,
            "target",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            ImmutableList.of());

    shell.executeStatement("INSERT INTO target SELECT * FROM source");
    HiveIcebergTestUtils.validateData(target, ImmutableList.of(), 0);

    testTables.appendIcebergTable(
        shell.getHiveConf(),
        source,
        fileFormat,
        null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    shell.executeStatement("INSERT INTO target SELECT * FROM source WHERE first_name = 'Nobody'");
    HiveIcebergTestUtils.validateData(target, ImmutableList.of(), 0);
  }

  @TestTemplate
  public void testStatsPopulation() throws Exception {
    assumeThat(executionEngine).as("Tez write is not implemented yet").isEqualTo("mr");
    assumeThat(testTableType)
        .as("Only HiveCatalog can remove stats which become obsolete")
        .isEqualTo(TestTables.TestTableType.HIVE_CATALOG);
    shell.setHiveSessionValue(HiveConf.ConfVars.HIVESTATSAUTOGATHER.varname, true);

    // create the table using a catalog which supports updating Hive stats (KEEP_HIVE_STATS is true)
    shell.setHiveSessionValue(ConfigProperties.KEEP_HIVE_STATS, true);
    TableIdentifier identifier = TableIdentifier.of("default", "customers");
    testTables.createTable(
        shell,
        identifier.name(),
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        PartitionSpec.unpartitioned(),
        fileFormat,
        ImmutableList.of());

    // insert some data and check the stats are up-to-date
    String insert =
        testTables.getInsertQuery(
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS, identifier, false);
    shell.executeStatement(insert);
    String stats =
        shell
            .metastore()
            .getTable(identifier)
            .getParameters()
            .get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    assertThat(stats)
        .startsWith("{\"BASIC_STATS\":\"true\""); // it's followed by column stats in Hive3

    // Create a Catalog where the KEEP_HIVE_STATS is false
    shell.metastore().hiveConf().set(ConfigProperties.KEEP_HIVE_STATS, StatsSetupConst.FALSE);
    TestTables nonHiveTestTables =
        HiveIcebergStorageHandlerTestUtils.testTables(shell, testTableType, temp);
    Table nonHiveTable = nonHiveTestTables.loadTable(identifier);

    // Append data to the table through a non-Hive engine (in this case, via the java API) -> should
    // remove stats
    nonHiveTestTables.appendIcebergTable(
        shell.getHiveConf(),
        nonHiveTable,
        fileFormat,
        null,
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);
    stats =
        shell
            .metastore()
            .getTable(identifier)
            .getParameters()
            .get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    assertThat(stats).isNull();

    // insert some data again using Hive catalog, and check the stats are back
    shell.executeStatement(insert);
    stats =
        shell
            .metastore()
            .getTable(identifier)
            .getParameters()
            .get(StatsSetupConst.COLUMN_STATS_ACCURATE);
    assertThat(stats)
        .startsWith("{\"BASIC_STATS\":\"true\""); // it's followed by column stats in Hive3
  }

  /**
   * Tests that vectorized ORC reading code path correctly handles when the same ORC file is split
   * into multiple parts. Although the split offsets and length will not always include the file
   * tail that contains the metadata, the vectorized reader needs to make sure to handle the tail
   * reading regardless of the offsets. If this is not done correctly, the last SELECT query will
   * fail.
   *
   * @throws Exception - any test error
   */
  @TestTemplate
  public void testVectorizedOrcMultipleSplits() throws Exception {
    assumeThat(isVectorized && FileFormat.ORC.equals(fileFormat)).isTrue();

    // This data will be held by a ~870kB ORC file
    List<Record> records =
        TestHelper.generateRandomRecords(
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA, 20000, 0L);

    // To support splitting the ORC file, we need to specify the stripe size to a small value. It
    // looks like the min
    // value is about 220kB, no smaller stripes are written by ORC. Anyway, this setting will
    // produce 4 stripes.
    shell.setHiveSessionValue("orc.stripe.size", "210000");

    testTables.createTable(
        shell,
        "targettab",
        HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
        fileFormat,
        records);

    // Will request 4 splits, separated on the exact stripe boundaries within the ORC file.
    // (Would request 5 if ORC split generation wouldn't be split (aka stripe) offset aware).
    shell.setHiveSessionValue(InputFormatConfig.SPLIT_SIZE, "210000");
    List<Object[]> result = shell.executeStatement("SELECT * FROM targettab ORDER BY last_name");

    assertThat(result).hasSize(20000);
  }

  @TestTemplate
  public void testRemoveAndAddBackColumnFromIcebergTable() throws IOException {
    assumeThat(isVectorized && FileFormat.PARQUET.equals(fileFormat)).isTrue();
    // Create an Iceberg table with the columns customer_id, first_name and last_name with some
    // initial data.
    Table icebergTable =
        testTables.createTable(
            shell,
            "customers",
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_SCHEMA,
            fileFormat,
            HiveIcebergStorageHandlerTestUtils.CUSTOMER_RECORDS);

    // Remove the first_name column
    icebergTable.updateSchema().deleteColumn("first_name").commit();
    // Add a new column with the name first_name
    icebergTable
        .updateSchema()
        .addColumn("first_name", Types.StringType.get(), "This is new first name")
        .commit();

    // Add new data to the table with the new first_name column filled.
    icebergTable = testTables.loadTable(TableIdentifier.of("default", "customers"));
    Schema customerSchemaWithNewFirstName =
        new Schema(
            optional(1, "customer_id", Types.LongType.get()),
            optional(2, "last_name", Types.StringType.get(), "This is last name"),
            optional(
                3, "first_name", Types.StringType.get(), "This is the newly added first name"));
    List<Record> newCustomersWithNewFirstName =
        TestHelper.RecordsBuilder.newInstance(customerSchemaWithNewFirstName)
            .add(3L, "Red", "James")
            .build();
    testTables.appendIcebergTable(
        shell.getHiveConf(), icebergTable, fileFormat, null, newCustomersWithNewFirstName);

    TestHelper.RecordsBuilder customersWithNewFirstNameBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaWithNewFirstName)
            .add(0L, "Brown", null)
            .add(1L, "Green", null)
            .add(2L, "Pink", null)
            .add(3L, "Red", "James");
    List<Record> customersWithNewFirstName = customersWithNewFirstNameBuilder.build();

    // Run a 'select *' from Hive and check if the first_name column is returned.
    // It should be null for the old data and should be filled in the entry added after the column
    // addition.
    List<Object[]> rows = shell.executeStatement("SELECT * FROM default.customers");
    HiveIcebergTestUtils.validateData(
        customersWithNewFirstName,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstName, rows),
        0);

    Schema customerSchemaWithNewFirstNameOnly =
        new Schema(
            optional(1, "customer_id", Types.LongType.get()),
            optional(
                3, "first_name", Types.StringType.get(), "This is the newly added first name"));

    TestHelper.RecordsBuilder customersWithNewFirstNameOnlyBuilder =
        TestHelper.RecordsBuilder.newInstance(customerSchemaWithNewFirstNameOnly)
            .add(0L, null)
            .add(1L, null)
            .add(2L, null)
            .add(3L, "James");
    List<Record> customersWithNewFirstNameOnly = customersWithNewFirstNameOnlyBuilder.build();

    // Run a 'select first_name' from Hive to check if the new first-name column can be queried.
    rows = shell.executeStatement("SELECT customer_id, first_name FROM default.customers");
    HiveIcebergTestUtils.validateData(
        customersWithNewFirstNameOnly,
        HiveIcebergTestUtils.valueForRow(customerSchemaWithNewFirstNameOnly, rows),
        0);
  }

  /**
   * Checks if the certain type is an unsupported vectorized types in Hive 3.1.2
   *
   * @param type - data type
   * @return - true if unsupported
   */
  private boolean isUnsupportedVectorizedTypeForHive(Type type) {
    if (!isVectorized) {
      return false;
    }
    switch (fileFormat) {
      case PARQUET:
        return Types.DecimalType.of(3, 1).equals(type)
            || type == Types.TimestampType.withoutZone()
            || type == Types.TimeType.get();
      case ORC:
        return type == Types.TimestampType.withZone() || type == Types.TimeType.get();
      default:
        return false;
    }
  }

  private void testComplexTypeWrite(Schema schema, List<Record> records) throws IOException {
    String tableName = "complex_table";
    Table table =
        testTables.createTable(shell, "complex_table", schema, fileFormat, ImmutableList.of());

    String dummyTableName = "dummy";
    shell.executeStatement("CREATE TABLE default." + dummyTableName + "(a int)");
    shell.executeStatement("INSERT INTO TABLE default." + dummyTableName + " VALUES(1)");
    records.forEach(
        r ->
            shell.executeStatement(
                insertQueryForComplexType(tableName, dummyTableName, schema, r)));
    HiveIcebergTestUtils.validateData(table, records, 0);
  }

  private String insertQueryForComplexType(
      String tableName, String dummyTableName, Schema schema, Record record) {
    StringBuilder query =
        new StringBuilder("INSERT INTO TABLE ")
            .append(tableName)
            .append(" SELECT ")
            .append(record.get(0))
            .append(", ");
    Type type = schema.asStruct().fields().get(1).type();
    query.append(buildComplexTypeInnerQuery(record.get(1), type));
    query.setLength(query.length() - 1);
    query.append(" FROM ").append(dummyTableName).append(" LIMIT 1");
    return query.toString();
  }

  private StringBuilder buildComplexTypeInnerQuery(Object field, Type type) {
    StringBuilder query = new StringBuilder();
    if (type instanceof Types.ListType) {
      query.append("array(");
      List<Object> elements = (List<Object>) field;
      assertThat(elements).as("Hive can not handle empty array() inserts").isNotEmpty();
      Type innerType = ((Types.ListType) type).fields().get(0).type();
      if (!elements.isEmpty()) {
        elements.forEach(e -> query.append(buildComplexTypeInnerQuery(e, innerType)));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.MapType) {
      query.append("map(");
      Map<Object, Object> entries = (Map<Object, Object>) field;
      Type keyType = ((Types.MapType) type).fields().get(0).type();
      Type valueType = ((Types.MapType) type).fields().get(1).type();
      if (!entries.isEmpty()) {
        entries
            .entrySet()
            .forEach(
                e ->
                    query.append(
                        buildComplexTypeInnerQuery(e.getKey(), keyType)
                            .append(buildComplexTypeInnerQuery(e.getValue(), valueType))));
        query.setLength(query.length() - 1);
      }
      query.append("),");
    } else if (type instanceof Types.StructType) {
      query.append("named_struct(");
      ((GenericRecord) field)
          .struct().fields().stream()
              .forEach(
                  f ->
                      query
                          .append(buildComplexTypeInnerQuery(f.name(), Types.StringType.get()))
                          .append(
                              buildComplexTypeInnerQuery(
                                  ((GenericRecord) field).getField(f.name()), f.type())));
      query.setLength(query.length() - 1);
      query.append("),");
    } else if (type instanceof Types.StringType) {
      if (field != null) {
        query.append("'").append(field).append("',");
      }
    } else {
      throw new RuntimeException("Unsupported type in complex query build.");
    }
    return query;
  }
}
