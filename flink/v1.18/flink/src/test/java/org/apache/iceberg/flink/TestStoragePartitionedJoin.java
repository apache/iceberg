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
package org.apache.iceberg.flink;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.connector.source.partitioning.KeyGroupedPartitioning;
import org.apache.flink.table.connector.source.partitioning.Partitioning;
import org.apache.flink.table.expressions.TransformExpression;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.source.IcebergTableSource;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for Flink storage partitioned joins using tables with mixed partition transforms.
 *
 * <p>This test validates that tables partitioned with both identity and bucket transforms can be
 * joined efficiently by leveraging Flink's partition-aware capabilities.
 */
// TODO: add to end integration testing after importing flink libraries after table planner change
public class TestStoragePartitionedJoin {

  @RegisterExtension
  protected static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @TempDir protected Path temporaryDirectory;

  @RegisterExtension
  protected static final HadoopCatalogExtension catalogExtension =
      new HadoopCatalogExtension("testdb", "test_table");

  private static final String TABLE_1_NAME = "partitioned_table_1";
  private static final String TABLE_2_NAME = "partitioned_table_2";
  private static final String TABLE_3_NAME = "partitioned_table_3";
  private static final String TABLE_4_NAME = "partitioned_table_4_month";
  private static final String TABLE_5_NAME = "partitioned_table_5_month";
  private static final String TABLE_6_NAME = "partitioned_table_6_day";
  private static final int BUCKET_COUNT = 128;

  private static final Schema TABLE_1_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "dt", Types.StringType.get()),
          optional(3, "name", Types.StringType.get()),
          optional(4, "salary", Types.IntegerType.get()));

  private static final Schema TABLE_2_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "dt", Types.StringType.get()),
          optional(3, "company", Types.StringType.get()),
          optional(4, "title", Types.StringType.get()));

  private static final Schema TABLE_3_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "dt", Types.StringType.get()),
          optional(3, "data", Types.StringType.get()),
          optional(4, "value", Types.DoubleType.get()));

  // Schema for tables with timestamp partitioning (month/day)
  private static final Schema TABLE_4_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "event_time", Types.TimestampType.withoutZone()),
          optional(3, "event_type", Types.StringType.get()),
          optional(4, "count", Types.IntegerType.get()));

  private static final Schema TABLE_5_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "event_time", Types.TimestampType.withoutZone()),
          optional(3, "category", Types.StringType.get()),
          optional(4, "amount", Types.DecimalType.of(10, 2)));

  private static final Schema TABLE_6_SCHEMA =
      new Schema(
          optional(1, "id", Types.LongType.get()),
          optional(2, "event_time", Types.TimestampType.withoutZone()),
          optional(3, "status", Types.StringType.get()),
          optional(4, "metric", Types.DoubleType.get()));

  // Partition spec for both tables: partitioned by dt (identity) and bucket(id, 128)
  private static final PartitionSpec TABLE_1_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_1_SCHEMA).identity("dt").bucket("id", BUCKET_COUNT).build();

  private static final PartitionSpec TABLE_2_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_2_SCHEMA).identity("dt").bucket("id", BUCKET_COUNT).build();

  private static final PartitionSpec TABLE_3_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_3_SCHEMA)
          .bucket("id", BUCKET_COUNT) // bucket first
          .identity("dt") // dt second
          .build();

  private static final PartitionSpec TABLE_4_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_4_SCHEMA).month("event_time").build();

  private static final PartitionSpec TABLE_5_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_5_SCHEMA).month("event_time").build();

  private static final PartitionSpec TABLE_6_PARTITION_SPEC =
      PartitionSpec.builderFor(TABLE_6_SCHEMA).day("event_time").build();

  private Table table1;
  private Table table2;
  private Table table3;
  private Table table4;
  private Table table5;
  private Table table6;
  private TableEnvironment tableEnv;

  private static final Transform<Long, Integer> BUCKET_TRANSFORM =
      Transforms.bucket(Types.LongType.get(), BUCKET_COUNT);

  private static final Transform<Long, Integer> MONTH_TRANSFORM =
      Transforms.month(Types.TimestampType.withoutZone());

  private static final Transform<Long, Integer> DAY_TRANSFORM =
      Transforms.day(Types.TimestampType.withoutZone());

  @BeforeEach
  public void setupTables() throws Exception {
    // Create table 1 with schema (id, dt, name, salary)
    table1 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_1_NAME), TABLE_1_SCHEMA, TABLE_1_PARTITION_SPEC);

    // Create table 2 with schema (id, dt, company, title)
    table2 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_2_NAME), TABLE_2_SCHEMA, TABLE_2_PARTITION_SPEC);

    // Create table 3 with schema (id, dt, data, value)
    table3 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_3_NAME), TABLE_3_SCHEMA, TABLE_3_PARTITION_SPEC);

    // Create table 4 with schema (id, event_time, event_type, count)
    table4 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_4_NAME), TABLE_4_SCHEMA, TABLE_4_PARTITION_SPEC);

    // Create table 5 with schema (id, event_time, category, amount)
    table5 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_5_NAME), TABLE_5_SCHEMA, TABLE_5_PARTITION_SPEC);

    // Create table 6 with schema (id, event_time, status, metric)
    table6 =
        catalogExtension
            .catalog()
            .createTable(
                TableIdentifier.of("testdb", TABLE_6_NAME), TABLE_6_SCHEMA, TABLE_6_PARTITION_SPEC);

    setupTableEnvironment();
    writeTestDataToTables();
  }

  @AfterEach
  public void cleanupTables() {
    if (table1 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_1_NAME));
    }
    if (table2 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_2_NAME));
    }
    if (table3 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_3_NAME));
    }
    if (table4 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_4_NAME));
    }
    if (table5 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_5_NAME));
    }
    if (table6 != null) {
      catalogExtension.catalog().dropTable(TableIdentifier.of("testdb", TABLE_6_NAME));
    }
  }

  @Test
  public void testSimplePartitionedTables() throws Exception {
    testPartitionCompatibility(
        table1, TABLE_1_SCHEMA, "table1", table2, TABLE_2_SCHEMA, "table2", true);
  }

  @Test
  public void testSimpleIncompatiblePartitionedTables() throws Exception {
    testPartitionCompatibility(
        table1, TABLE_1_SCHEMA, "table1", table3, TABLE_3_SCHEMA, "table3", false);
  }

  @Test
  public void testMonthPartitionedTables() throws Exception {
    testPartitionCompatibility(
        table4, TABLE_4_SCHEMA, "table4", table5, TABLE_5_SCHEMA, "table5", true);
  }

  @Test
  public void testMonthVsDayPartitionedTables() throws Exception {
    testPartitionCompatibility(
        table4, TABLE_4_SCHEMA, "table4", table6, TABLE_6_SCHEMA, "table6", false);
  }

  /**
   * Generic helper method to test partition compatibility between two tables.
   *
   * @param t1 First table to test
   * @param schema1 Schema of first table
   * @param table1Name Name of first table for error messages
   * @param t2 Second table to test
   * @param schema2 Schema of second table
   * @param table2Name Name of second table for error messages
   * @param expectedCompatible Whether tables should be compatible (true) or incompatible (false)
   */
  private void testPartitionCompatibility(
      Table t1,
      Schema schema1,
      String table1Name,
      Table t2,
      Schema schema2,
      String table2Name,
      boolean expectedCompatible)
      throws Exception {
    // Extract output partitioning for both tables
    Partitioning table1Partitioning = extractOutputPartitioning(t1, schema1, table1Name);
    Partitioning table2Partitioning = extractOutputPartitioning(t2, schema2, table2Name);

    // Verify both tables use KeyGroupedPartitioning
    assertThat(table1Partitioning)
        .withFailMessage("%s is not partitioned by KeyGroupedPartitioning", table1Name)
        .isInstanceOf(KeyGroupedPartitioning.class);
    assertThat(table2Partitioning)
        .withFailMessage("%s is not partitioned by KeyGroupedPartitioning", table2Name)
        .isInstanceOf(KeyGroupedPartitioning.class);

    // Test compatibility
    KeyGroupedPartitioning table1KGP = (KeyGroupedPartitioning) table1Partitioning;
    KeyGroupedPartitioning table2KGP = (KeyGroupedPartitioning) table2Partitioning;

    String compatibilityMessage =
        expectedCompatible
            ? String.format(
                "%s and %s should have compatible partitioning for storage partitioned joins",
                table1Name, table2Name)
            : String.format(
                "%s and %s should have incompatible partitioning for storage partitioned joins",
                table1Name, table2Name);

    assertThat(table1KGP.isCompatible(table2KGP))
        .withFailMessage(compatibilityMessage)
        .isEqualTo(expectedCompatible);

    // Test symmetry - compatibility should be symmetric
    assertThat(table2KGP.isCompatible(table1KGP))
        .withFailMessage(
            "Compatibility should be symmetric between %s and %s", table2Name, table1Name)
        .isEqualTo(expectedCompatible);
  }

  private void setupTableEnvironment() {
    // Ensure batch execution mode
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
    tableEnv = TableEnvironment.create(settings);

    // Configure storage partition join optimization (can be toggled for testing)
    setStoragePartitionJoinEnabled(true);

    tableEnv.executeSql(
        "CREATE CATALOG iceberg_catalog WITH ("
            + "'type'='iceberg',"
            + "'catalog-type'='hadoop',"
            + "'warehouse'='"
            + catalogExtension.warehouse()
            + "'"
            + ")");

    tableEnv.executeSql("USE CATALOG iceberg_catalog");
    tableEnv.executeSql("USE testdb");
  }

  protected void setStoragePartitionJoinEnabled(boolean enabled) {
    tableEnv
        .getConfig()
        .getConfiguration()
        .setString("table.optimizer.storage-partition-join-enabled", String.valueOf(enabled));
  }

  protected List<Row> sql(String query, Object... args) {
    TableResult tableResult = exec(query, args);
    try (CloseableIterator<Row> iter = tableResult.collect()) {
      return Lists.newArrayList(iter);
    } catch (Exception e) {
      throw new RuntimeException("Failed to collect table result", e);
    }
  }

  // TODO -- maybe combine the above
  protected TableResult exec(String query, Object... args) {
    return tableEnv.executeSql(String.format(query, args));
  }

  private List<Record> createTable1TestData() {
    List<Record> records = Lists.newArrayList();
    String[] dates = {"2025-01-01", "2025-05-02", "2025-06-03"};
    String[] names = {"Alice", "Bob", "Charlie", "Diana", "Eve"};

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_1_SCHEMA);
      record.setField("id", (long) i);
      record.setField("dt", dates[i % dates.length]);
      record.setField("name", names[i % names.length]);
      record.setField("salary", 50000 + (i % 20) * 2500);
      records.add(record);
    }

    return records;
  }

  private List<Record> createTable2TestData() {
    List<Record> records = Lists.newArrayList();
    String[] dates = {"2025-01-01", "2025-05-02", "2025-06-03"};
    String[] companies = {"TechCorp", "DataSys", "CloudInc", "AILabs", "DevCo"};
    String[] titles = {"Engineer", "Manager", "Analyst", "Director", "Scientist"};

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_2_SCHEMA);
      record.setField("id", (long) i);
      record.setField("dt", dates[i % dates.length]);
      record.setField("company", companies[i % companies.length]);
      record.setField("title", titles[i % titles.length]);
      records.add(record);
    }

    return records;
  }

  private List<Record> createTable3TestData() {
    List<Record> records = Lists.newArrayList();
    String[] dates = {"2025-01-01", "2025-05-02", "2025-06-03"};
    String[] data = {"data1", "data2", "data3", "data4", "data5"};
    double[] values = {10.0, 20.0, 30.0, 40.0, 50.0};

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_3_SCHEMA);
      record.setField("id", (long) i);
      record.setField("dt", dates[i % dates.length]);
      record.setField("data", data[i % data.length]);
      record.setField("value", values[i % values.length]);
      records.add(record);
    }

    return records;
  }

  private List<Record> createTable4TestData() {
    List<Record> records = Lists.newArrayList();
    LocalDateTime[] eventTimes = {
      LocalDateTime.of(2022, 2, 1, 10, 30, 0),
      LocalDateTime.of(2022, 3, 1, 10, 30, 0),
      LocalDateTime.of(2022, 4, 1, 10, 30, 0)
    };
    String[] eventTypes = {"click", "view", "purchase"};
    int[] counts = {10, 20, 30};

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_4_SCHEMA);
      record.setField("id", (long) i);
      record.setField("event_time", eventTimes[i % eventTimes.length]);
      record.setField("event_type", eventTypes[i % eventTypes.length]);
      record.setField("count", counts[i % counts.length]);
      records.add(record);
    }

    return records;
  }

  private List<Record> createTable5TestData() {
    List<Record> records = Lists.newArrayList();
    LocalDateTime[] eventTimes = {
      LocalDateTime.of(2022, 2, 1, 10, 30, 0),
      LocalDateTime.of(2022, 3, 1, 10, 30, 0),
      LocalDateTime.of(2022, 4, 1, 10, 30, 0)
    };
    String[] categories = {"A", "B", "C"};
    BigDecimal[] amounts = {
      new BigDecimal("10.99"), new BigDecimal("20.99"), new BigDecimal("30.99")
    };

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_5_SCHEMA);
      record.setField("id", (long) i);
      record.setField("event_time", eventTimes[i % eventTimes.length]);
      record.setField("category", categories[i % categories.length]);
      record.setField("amount", amounts[i % amounts.length]);
      records.add(record);
    }

    return records;
  }

  private List<Record> createTable6TestData() {
    List<Record> records = Lists.newArrayList();
    LocalDateTime[] eventTimes = {
      LocalDateTime.of(2022, 2, 1, 10, 30, 0),
      LocalDateTime.of(2022, 3, 15, 10, 30, 0),
      LocalDateTime.of(2022, 4, 20, 10, 30, 0)
    };
    String[] statuses = {"success", "failure", "pending"};
    double[] metrics = {0.5, 0.7, 0.9};

    for (int i = 0; i < 50; i++) {
      Record record = GenericRecord.create(TABLE_6_SCHEMA);
      record.setField("id", (long) i);
      record.setField("event_time", eventTimes[i % eventTimes.length]);
      record.setField("status", statuses[i % statuses.length]);
      record.setField("metric", metrics[i % metrics.length]);
      records.add(record);
    }

    return records;
  }

  private Partitioning extractOutputPartitioning(Table table, Schema schema, String tableName)
      throws Exception {
    // Create TableLoader and IcebergTableSource
    TableLoader tableLoader = TableLoader.fromHadoopTable(table.location());
    IcebergTableSource tableSource =
        new IcebergTableSource(
            tableLoader,
            FlinkSchemaUtil.toSchema(schema),
            java.util.Collections.emptyMap(),
            new org.apache.flink.configuration.Configuration());
    // Get output partitioning
    Partitioning outputPartitioning = tableSource.outputPartitioning();
    if (!(outputPartitioning instanceof KeyGroupedPartitioning)) {
      // TODO -- adapt later
      return null;
    }
    assertThat(outputPartitioning)
        .withFailMessage("OutputPartitioning for %s should be KeyGroupedPartitioning", tableName)
        .isInstanceOf(KeyGroupedPartitioning.class);
    KeyGroupedPartitioning keyGroupedPartitioning = (KeyGroupedPartitioning) outputPartitioning;

    // Extract partitioning info
    int numPartitions = keyGroupedPartitioning.getPartitionValues().length;
    TransformExpression[] keys = keyGroupedPartitioning.keys();
    org.apache.flink.types.Row[] partitionValues = keyGroupedPartitioning.getPartitionValues();

    // Validate basic structure
    assertThat(numPartitions)
        .withFailMessage("NumPartitions for %s should be > 0", tableName)
        .isGreaterThan(0);
    assertThat(keys).withFailMessage("Keys for %s should not be empty", tableName).isNotEmpty();
    assertThat(partitionValues)
        .withFailMessage("PartitionValues for %s should not be empty", tableName)
        .isNotEmpty();
    return outputPartitioning;
  }

  // TODO -- we want to make this generic for any table types as we expand these tests. For now,
  // we are do this as the rest is out of scope for the PoC
  private void writeTestDataToTables() throws Exception {
    // Create test data for both tables
    List<Record> table1Records = createTable1TestData();
    List<Record> table2Records = createTable2TestData();
    List<Record> table3Records = createTable3TestData();
    List<Record> table4Records = createTable4TestData();
    List<Record> table5Records = createTable5TestData();
    List<Record> table6Records = createTable6TestData();

    // Write data using GenericAppenderHelper with proper partition values
    GenericAppenderHelper table1Appender =
        new GenericAppenderHelper(table1, FileFormat.PARQUET, temporaryDirectory);
    GenericAppenderHelper table2Appender =
        new GenericAppenderHelper(table2, FileFormat.PARQUET, temporaryDirectory);
    GenericAppenderHelper table3Appender =
        new GenericAppenderHelper(table3, FileFormat.PARQUET, temporaryDirectory);
    GenericAppenderHelper table4Appender =
        new GenericAppenderHelper(table4, FileFormat.PARQUET, temporaryDirectory);
    GenericAppenderHelper table5Appender =
        new GenericAppenderHelper(table5, FileFormat.PARQUET, temporaryDirectory);
    GenericAppenderHelper table6Appender =
        new GenericAppenderHelper(table6, FileFormat.PARQUET, temporaryDirectory);

    // Group records by partition and write them
    for (Record record : table1Records) {
      String dt = (String) record.getField("dt");
      Long id = (Long) record.getField("id");
      int bucket = BUCKET_TRANSFORM.apply(id);

      // Create partition row: [dt, bucket_value]
      org.apache.iceberg.TestHelpers.Row partitionRow =
          org.apache.iceberg.TestHelpers.Row.of(dt, bucket);
      table1Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }

    for (Record record : table2Records) {
      String dt = (String) record.getField("dt");
      Long id = (Long) record.getField("id");
      int bucket = BUCKET_TRANSFORM.apply(id);

      // Create partition row: [dt, bucket_value]
      org.apache.iceberg.TestHelpers.Row partitionRow =
          org.apache.iceberg.TestHelpers.Row.of(dt, bucket);
      table2Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }

    for (Record record : table3Records) {
      String dt = (String) record.getField("dt");
      Long id = (Long) record.getField("id");
      int bucket = BUCKET_TRANSFORM.apply(id);

      // Create partition row: [bucket_value, dt]
      org.apache.iceberg.TestHelpers.Row partitionRow =
          org.apache.iceberg.TestHelpers.Row.of(bucket, dt);
      table3Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }

    for (Record record : table4Records) {
      LocalDateTime eventTime = (LocalDateTime) record.getField("event_time");
      int month = MONTH_TRANSFORM.apply(eventTime.toEpochSecond(ZoneOffset.UTC));

      // Create partition row: [month]
      org.apache.iceberg.TestHelpers.Row partitionRow =
          org.apache.iceberg.TestHelpers.Row.of(month);
      table4Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }

    for (Record record : table5Records) {
      LocalDateTime eventTime = (LocalDateTime) record.getField("event_time");
      int month = MONTH_TRANSFORM.apply(eventTime.toEpochSecond(ZoneOffset.UTC));

      // Create partition row: [month]
      org.apache.iceberg.TestHelpers.Row partitionRow =
          org.apache.iceberg.TestHelpers.Row.of(month);
      table5Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }

    for (Record record : table6Records) {
      LocalDateTime eventTime = (LocalDateTime) record.getField("event_time");
      int day = DAY_TRANSFORM.apply(eventTime.toEpochSecond(ZoneOffset.UTC));

      // Create partition row: [day]
      org.apache.iceberg.TestHelpers.Row partitionRow = org.apache.iceberg.TestHelpers.Row.of(day);
      table6Appender.appendToTable(partitionRow, Lists.newArrayList(record));
    }
  }
}
