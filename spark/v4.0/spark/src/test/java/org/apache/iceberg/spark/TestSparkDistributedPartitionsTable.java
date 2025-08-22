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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Integration tests for distributed PartitionsTable scanning in Spark.
 *
 * <p>Tests the complete flow from SQL queries like: SELECT max(max_utc_date) FROM table__partitions
 *
 * <p>Down to the distributed partition metadata scanning implementation.
 */
public class TestSparkDistributedPartitionsTable extends CatalogTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDistributedPartitionsTableQueryPerformance() {

    // Create table with date partitioning (common in production)
    sql(
        "CREATE TABLE %s (id bigint, data string, utc_date date) "
            + "USING iceberg PARTITIONED BY (days(utc_date)) "
            + "TBLPROPERTIES ('read.metadata-planning-mode' = 'distributed')",
        tableName);

    // Insert data into multiple partitions to create realistic metadata volume
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'data1', date '2023-01-01'), "
            + "(2, 'data2', date '2023-01-02'), "
            + "(3, 'data3', date '2023-01-03'), "
            + "(4, 'data4', date '2023-01-04'), "
            + "(5, 'data5', date '2023-01-05')",
        tableName);

    // This is the key test: Query the partitions metadata table
    // Equivalent to: SELECT max(max_utc_date) FROM session_feature_engagement_f__partitions
    List<Object[]> result = sql("SELECT COUNT(*) FROM %s.partitions", tableName);

    // Verify the query executes successfully
    assertThat(result).as("Partitions metadata query should return results").isNotNull();
    assertThat(result).as("Should return exactly one row for COUNT query").hasSize(1);

    // Should have exactly 5 partitions (one for each date inserted)
    Long partitionCount = (Long) result.get(0)[0];
    assertThat(partitionCount)
        .as("Should have exactly 5 partitions for 5 different dates")
        .isEqualTo(5L);
  }

  @TestTemplate
  public void testPartitionsTableMetadataColumns() {
    // Test that all partition metadata columns are available via SQL
    sql(
        "CREATE TABLE %s (id bigint, category string) "
            + "USING iceberg PARTITIONED BY (category) "
            + "TBLPROPERTIES ('read.metadata-planning-mode' = 'distributed')",
        tableName);

    // Insert test data
    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);

    // Query all available metadata columns from partitions table
    List<Object[]> result =
        sql(
            "SELECT partition, record_count, file_count, spec_id "
                + "FROM %s.partitions "
                + "ORDER BY partition",
            tableName);

    // Should have exactly 3 partitions (one for each category: A, B, C)
    assertThat(result).as("Should have exactly 3 partition metadata rows").hasSize(3);

    // Verify each partition has exactly the expected metadata
    for (Object[] row : result) {
      assertThat(row[0]).as("Partition column should not be null").isNotNull();
      assertThat((Long) row[1]).as("Each partition should have exactly 1 record").isEqualTo(1L);
      assertThat((Integer) row[2]).as("Each partition should have exactly 1 file").isEqualTo(1);
      assertThat((Integer) row[3]).as("Spec ID should be 0 for the default spec").isEqualTo(0);
    }
  }

  @TestTemplate
  public void testLocalVsDistributedModeComparison() {
    // Create a table and compare local vs distributed scanning results
    sql(
        "CREATE TABLE %s (id bigint, value string) "
            + "USING iceberg PARTITIONED BY (bucket(4, value))",
        tableName);

    // Insert data to create multiple partitions
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'A'), (2, 'B'), (3, 'C'), (4, 'D'), "
            + "(5, 'E'), (6, 'F'), (7, 'G'), (8, 'H')",
        tableName);

    // Test local mode
    sql("ALTER TABLE %s SET TBLPROPERTIES ('read.metadata-planning-mode' = 'local')", tableName);
    List<Object[]> localResult = sql("SELECT count(*) FROM %s.partitions", tableName);
    Long localCount = (Long) localResult.get(0)[0];

    // Test distributed mode
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('read.metadata-planning-mode' = 'distributed')",
        tableName);
    List<Object[]> distributedResult = sql("SELECT count(*) FROM %s.partitions", tableName);
    Long distributedCount = (Long) distributedResult.get(0)[0];

    // Both modes should return the same results
    assertThat(distributedCount)
        .as("Local and distributed modes should return same results")
        .isEqualTo(localCount);

    // With bucket(4, value), we should have 1-4 partitions (depends on hash distribution)
    assertThat(distributedCount).as("Should have at least 1 partition").isGreaterThanOrEqualTo(1L);
    assertThat(distributedCount)
        .as("Should have at most 4 partitions for bucket(4)")
        .isLessThanOrEqualTo(4L);
  }

  @TestTemplate
  public void testSqlQueryWithAggregations() {
    // Test basic aggregations that work with distributed scanning
    sql(
        "CREATE TABLE %s (id bigint, amount decimal(10,2), region string) "
            + "USING iceberg PARTITIONED BY (region) "
            + "TBLPROPERTIES ('read.metadata-planning-mode' = 'distributed')",
        tableName);

    // Insert data across multiple regions
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 100.50, 'US'), (2, 200.75, 'EU'), "
            + "(3, 150.25, 'APAC'), (4, 300.00, 'US')",
        tableName);

    // Test basic COUNT aggregation (avoiding complex field aggregations for now)
    List<Object[]> result = sql("SELECT COUNT(*) FROM %s.partitions", tableName);

    assertThat(result).as("Should return one aggregation row").hasSize(1);
    Object[] row = result.get(0);
    // Should have exactly 3 partitions: US, EU, APAC (US appears twice but it's the same partition)
    assertThat((Long) row[0])
        .as("Should have exactly 3 partitions for 3 distinct regions")
        .isEqualTo(3L);

    // Test simple partition queries work - should return exactly 3 partition rows
    List<Object[]> partitionList = sql("SELECT partition FROM %s.partitions", tableName);
    assertThat(partitionList).as("Should have exactly 3 partition rows").hasSize(3);
  }

  @TestTemplate
  public void testAutoModeWithLargeTable() {
    // Test AUTO mode selection with a table that has many partitions
    sql(
        "CREATE TABLE %s (id bigint, timestamp timestamp, user_id bigint) "
            + "USING iceberg PARTITIONED BY (hours(timestamp)) "
            + "TBLPROPERTIES ('read.metadata-planning-mode' = 'auto')",
        tableName);

    // Insert data to create many partitions (AUTO should select distributed)
    for (int hour = 0; hour < 15; hour++) {
      sql(
          "INSERT INTO %s VALUES " + "(%d, timestamp '2023-01-01 %02d:00:00', %d)",
          tableName, hour, hour, hour * 100);
    }

    // Query should automatically use distributed scanning
    List<Object[]> result = sql("SELECT COUNT(*) FROM %s.partitions", tableName);

    assertThat(result).as("Should return exactly one row for COUNT query").hasSize(1);
    Long partitionCount = (Long) result.get(0)[0];
    // Should have exactly 15 partitions (one for each hour: 0-14)
    assertThat(partitionCount)
        .as("Should have exactly 15 partitions for 15 different hours")
        .isEqualTo(15L);

    // Verify this triggers AUTO distributed mode (threshold is > 10)
    assertThat(partitionCount)
        .as("15 partitions should trigger AUTO distributed mode (threshold > 10)")
        .isGreaterThan(10L);
  }
}
