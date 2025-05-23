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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Use the IcebergSource (FLIP-27) */
public class TestIcebergSourceSql extends TestSqlBase {
  private static final Schema SCHEMA_TS =
      new Schema(
          required(1, "t1", Types.TimestampType.withoutZone()),
          required(2, "t2", Types.LongType.get()));

  @BeforeEach
  @Override
  public void before() throws IOException {
    setUpTableEnv(getTableEnv());
    setUpTableEnv(getStreamingTableEnv());
  }

  private static void setUpTableEnv(TableEnvironment tableEnvironment) {
    Configuration tableConf = tableEnvironment.getConfig().getConfiguration();
    tableConf.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, true);
    // Disable inferring parallelism to avoid interfering watermark tests
    // that check split assignment is ordered by the watermark column.
    // The tests assumes default parallelism of 1 with single reader task
    // in order to check the order of read records.
    tableConf.set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_INFER_SOURCE_PARALLELISM, false);

    tableEnvironment.getConfig().set("table.exec.resource.default-parallelism", "1");
    SqlHelpers.sql(
        tableEnvironment,
        "create catalog iceberg_catalog with ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_EXTENSION.warehouse());
    SqlHelpers.sql(tableEnvironment, "use catalog iceberg_catalog");

    tableConf.set(TableConfigOptions.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED, true);
  }

  @AfterEach
  public void after() throws IOException {
    CATALOG_EXTENSION.catalog().dropTable(TestFixtures.TABLE_IDENTIFIER);
  }

  private Record generateRecord(Instant t1, long t2) {
    Record record = GenericRecord.create(SCHEMA_TS);
    record.setField("t1", t1.atZone(ZoneId.systemDefault()).toLocalDateTime());
    record.setField("t2", t2);
    return record;
  }

  /** Generates the records in the expected order, with respect to their datafile */
  private List<Record> generateExpectedRecords(boolean ascending) throws Exception {
    Table table = CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA_TS);
    long baseTime = 1702382109000L;

    GenericAppenderHelper helper =
        new GenericAppenderHelper(table, FileFormat.PARQUET, temporaryFolder);

    Record file1Record1 =
        generateRecord(Instant.ofEpochMilli(baseTime), baseTime + (1000 * 60 * 60 * 24 * 30L));
    Record file1Record2 =
        generateRecord(
            Instant.ofEpochMilli(baseTime - 10 * 1000L), baseTime + (1000 * 60 * 60 * 24 * 35L));

    List<Record> recordsDataFile1 = Lists.newArrayList();
    recordsDataFile1.add(file1Record1);
    recordsDataFile1.add(file1Record2);
    DataFile dataFile1 = helper.writeFile(recordsDataFile1);

    Record file2Record1 =
        generateRecord(
            Instant.ofEpochMilli(baseTime + 14 * 1000L), baseTime - (1000 * 60 * 60 * 24 * 30L));
    Record file2Record2 =
        generateRecord(
            Instant.ofEpochMilli(baseTime + 12 * 1000L), baseTime - (1000 * 60 * 61 * 24 * 35L));

    List<Record> recordsDataFile2 = Lists.newArrayList();
    recordsDataFile2.add(file2Record1);
    recordsDataFile2.add(file2Record2);

    DataFile dataFile2 = helper.writeFile(recordsDataFile2);
    helper.appendToTable(dataFile1, dataFile2);

    // Expected records if the splits are ordered
    //     - ascending (watermark from t1) - records from the split with early timestamps, then
    // records from the split with late timestamps
    //     - descending (watermark from t2) - records from the split with old longs, then records
    // from the split with new longs
    List<Record> expected = Lists.newArrayList();
    if (ascending) {
      expected.addAll(recordsDataFile1);
      expected.addAll(recordsDataFile2);
    } else {
      expected.addAll(recordsDataFile2);
      expected.addAll(recordsDataFile1);
    }
    return expected;
  }

  /** Tests the order of splits returned when setting the watermark-column options */
  @Test
  public void testWatermarkOptionsAscending() throws Exception {
    List<Record> expected = generateExpectedRecords(true);
    TestHelpers.assertRecordsWithOrder(
        run(
            ImmutableMap.of("watermark-column", "t1", "split-file-open-cost", "128000000"),
            "",
            "*"),
        expected,
        SCHEMA_TS);
  }

  /**
   * Tests the order of splits returned when setting the watermark-column and
   * watermark-column-time-unit" options
   */
  @Test
  public void testWatermarkOptionsDescending() throws Exception {
    List<Record> expected = generateExpectedRecords(false);
    TestHelpers.assertRecordsWithOrder(
        run(
            ImmutableMap.of(
                "watermark-column",
                "t2",
                "watermark-column-time-unit",
                "MILLISECONDS",
                "split-file-open-cost",
                "128000000"),
            "",
            "*"),
        expected,
        SCHEMA_TS);
  }

  @Test
  public void testReadFlinkDynamicTable() throws Exception {
    List<Record> expected = generateExpectedRecords(false);
    SqlHelpers.sql(
        getTableEnv(),
        "create table `default_catalog`.`default_database`.flink_table LIKE iceberg_catalog.`default`.%s",
        TestFixtures.TABLE);

    // Read from table in flink catalog
    TestHelpers.assertRecords(
        SqlHelpers.sql(
            getTableEnv(), "select * from `default_catalog`.`default_database`.flink_table"),
        expected,
        SCHEMA_TS);
  }

  @Test
  public void testWatermarkInvalidConfig() {
    CATALOG_EXTENSION.catalog().createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA_TS);

    String flinkTable = "`default_catalog`.`default_database`.flink_table";
    SqlHelpers.sql(
        getStreamingTableEnv(),
        "CREATE TABLE %s "
            + "(eventTS AS CAST(t1 AS TIMESTAMP(3)), "
            + "WATERMARK FOR eventTS AS SOURCE_WATERMARK()) LIKE iceberg_catalog.`default`.%s",
        flinkTable,
        TestFixtures.TABLE);

    assertThatThrownBy(() -> SqlHelpers.sql(getStreamingTableEnv(), "SELECT * FROM %s", flinkTable))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("watermark-column needs to be configured to use source watermark.");
  }

  @Test
  public void testWatermarkValidConfig() throws Exception {
    List<Record> expected = generateExpectedRecords(true);

    String flinkTable = "`default_catalog`.`default_database`.flink_table";

    SqlHelpers.sql(
        getStreamingTableEnv(),
        "CREATE TABLE %s "
            + "(eventTS AS CAST(t1 AS TIMESTAMP(3)), "
            + "WATERMARK FOR eventTS AS SOURCE_WATERMARK()) WITH ('watermark-column'='t1') LIKE iceberg_catalog.`default`.%s",
        flinkTable,
        TestFixtures.TABLE);

    TestHelpers.assertRecordsWithOrder(
        SqlHelpers.sql(
            getStreamingTableEnv(),
            "SELECT t1, t2 FROM TABLE(TUMBLE(TABLE %s, DESCRIPTOR(eventTS), INTERVAL '1' SECOND))",
            flinkTable),
        expected,
        SCHEMA_TS);
  }
}
