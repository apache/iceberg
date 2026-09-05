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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.apache.iceberg.flink.TestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFlinkTableSourceAggregatePushDown extends TestBase {

  @Parameters(name = "useFlip27Source = {0}")
  private static Object[][] parameters() {
    return new Object[][] {
      {false}, {true},
    };
  }

  @Parameter(index = 0)
  private boolean useFlip27Source;

  private static final String CATALOG_NAME = "test_catalog";
  private static final String DATABASE_NAME = "test_db";
  private static final String TABLE_NAME = "test_table";

  @Override
  protected TableEnvironment getTableEnv() {
    super.getTableEnv().getConfig().getConfiguration().set(CoreOptions.DEFAULT_PARALLELISM, 1);
    super.getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_USE_FLIP27_SOURCE, useFlip27Source);
    return super.getTableEnv();
  }

  @BeforeEach
  public void before() throws IOException {
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .removeConfig(FlinkConfigOptions.TABLE_EXEC_ICEBERG_AGGREGATE_PUSH_DOWN_ENABLED);
    File warehouseFile = File.createTempFile("junit", null, temporaryDirectory.toFile());
    assertThat(warehouseFile.delete()).isTrue();
    String warehouse = String.format("file:%s", warehouseFile);

    sql(
        "CREATE CATALOG %s WITH ('type'='iceberg', 'catalog-type'='hadoop', 'warehouse'='%s')",
        CATALOG_NAME, warehouse);
    sql("USE CATALOG %s", CATALOG_NAME);
    sql("CREATE DATABASE %s", DATABASE_NAME);
    sql("USE %s", DATABASE_NAME);
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR, d DOUBLE) WITH ('write.format.default'='%s')",
        TABLE_NAME, FileFormat.PARQUET.name());
    sql(
        "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
        TABLE_NAME);
  }

  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, TABLE_NAME);
    dropDatabase(DATABASE_NAME, true);
    dropCatalog(CATALOG_NAME, true);
  }

  @TestTemplate
  public void countStarPushDown() {
    enableAggregatePushDown();

    String query = String.format("SELECT COUNT(*) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should be pushed into the scan")
        .contains("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(3L));
  }

  @TestTemplate
  public void countColumnPushDown() {
    enableAggregatePushDown();

    String query = String.format("SELECT COUNT(data) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should be pushed into the scan")
        .contains("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(2L));
  }

  @TestTemplate
  public void maxMinPushDown() {
    enableAggregatePushDown();

    String query = String.format("SELECT MAX(id), MIN(id) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should be pushed into the scan")
        .contains("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(3, 1));
  }

  @TestTemplate
  public void aggregatePushDownAcrossMultipleDataFiles() {
    enableAggregatePushDown();
    sql("INSERT INTO %s VALUES (4,'d',40)", TABLE_NAME);
    sql("INSERT INTO %s VALUES (5,'e',50),(6,'f',60)", TABLE_NAME);

    String query = String.format("SELECT COUNT(*), MAX(id), MIN(id) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should be pushed into the scan across multiple data files")
        .contains("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(6L, 6, 1));
  }

  @TestTemplate
  public void aggregatePushDownDisabledByDefault() {
    String query = String.format("SELECT COUNT(*) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should not be pushed into the scan when disabled")
        .doesNotContain("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(3L));
  }

  @TestTemplate
  public void aggregatePushDownSkippedWithFilter() {
    enableAggregatePushDown();

    String query = String.format("SELECT COUNT(*) FROM %s WHERE id > 1", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should not be pushed into the scan when a filter is present")
        .doesNotContain("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(2L));
  }

  @TestTemplate
  public void aggregatePushDownWithPartitionAlignedFilter() {
    enableAggregatePushDown();

    String partitionedTable = "partitioned_table";
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR, d DOUBLE) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s')",
        partitionedTable, FileFormat.PARQUET.name());
    try {
      sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)", partitionedTable);

      String query = String.format("SELECT COUNT(*) FROM %s WHERE data = 'a'", partitionedTable);
      assertThat(explain(query))
          .as("Local aggregate should be pushed into the scan for a partition-aligned filter")
          .contains("aggregates=[");
      assertThat(sql(query)).hasSize(1).containsExactly(Row.of(2L));

      String nonAlignedQuery =
          String.format("SELECT COUNT(*) FROM %s WHERE id > 2", partitionedTable);
      assertThat(explain(nonAlignedQuery))
          .as("Local aggregate should not be pushed into the scan for a non-aligned filter")
          .doesNotContain("aggregates=[");
      assertThat(sql(nonAlignedQuery)).hasSize(1).containsExactly(Row.of(2L));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, partitionedTable);
    }
  }

  @TestTemplate
  public void filterPushDownOnPartitionedTableWithoutAggregate() {
    String partitionedTable = "partitioned_table";
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR, d DOUBLE) PARTITIONED BY (data) "
            + "WITH ('write.format.default'='%s')",
        partitionedTable, FileFormat.PARQUET.name());
    try {
      sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)", partitionedTable);

      String query = String.format("SELECT id FROM %s WHERE data = 'a'", partitionedTable);
      assertThat(sql(query)).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

      String nonAligned = String.format("SELECT id FROM %s WHERE id > 2", partitionedTable);
      assertThat(sql(nonAligned)).containsExactlyInAnyOrder(Row.of(3), Row.of(4));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, partitionedTable);
    }
  }

  @TestTemplate
  public void aggregatePushDownSkippedWithGroupBy() {
    enableAggregatePushDown();

    String query = String.format("SELECT data, COUNT(*) FROM %s GROUP BY data", TABLE_NAME);
    assertThat(explain(query))
        .as("Local aggregate should not be pushed into the scan for GROUP BY queries")
        .doesNotContain("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(3);
  }

  private void enableAggregatePushDown() {
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .set(FlinkConfigOptions.TABLE_EXEC_ICEBERG_AGGREGATE_PUSH_DOWN_ENABLED, true);
  }

  private String explain(String query) {
    return getTableEnv().explainSql(query);
  }
}
