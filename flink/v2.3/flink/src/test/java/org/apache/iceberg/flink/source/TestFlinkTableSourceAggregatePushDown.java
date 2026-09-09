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

import java.io.IOException;
import java.util.List;
import org.apache.flink.types.Row;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.flink.FlinkConfigOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkTableSourceAggregatePushDown extends TableSourceTestBase {

  @Override
  protected FileFormat format() {
    return FileFormat.PARQUET;
  }

  @BeforeEach
  @Override
  public void before() throws IOException {
    super.before();
    getTableEnv()
        .getConfig()
        .getConfiguration()
        .removeConfig(FlinkConfigOptions.TABLE_EXEC_ICEBERG_AGGREGATE_PUSH_DOWN_ENABLED);
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
  public void maxOnStringIsNotPushedDown() {
    enableAggregatePushDown();

    String query = String.format("SELECT MAX(data) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("MIN/MAX on a string column must not be pushed into the scan")
        .doesNotContain("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of("iceberg"));
  }

  @TestTemplate
  public void maxOnDoublePushDown() {
    enableAggregatePushDown();

    String query = String.format("SELECT MAX(d) FROM %s", TABLE_NAME);
    assertThat(explain(query))
        .as("MIN/MAX on a non-integer column should be pushed into the scan")
        .contains("aggregates=[");

    List<Row> result = sql(query);
    assertThat(result).hasSize(1).containsExactly(Row.of(30.0));
  }

  @TestTemplate
  public void aggregatePushDownSkippedWithCountsMetricsMode() {
    enableAggregatePushDown();

    String countsTable = "counts_table";
    sql(
        "CREATE TABLE %s (id INT, data VARCHAR) "
            + "WITH ('write.format.default'='%s', 'write.metadata.metrics.default'='counts')",
        countsTable, format().name());
    try {
      sql("INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'c')", countsTable);

      String query = String.format("SELECT MAX(id) FROM %s", countsTable);
      assertThat(explain(query))
          .as("MIN/MAX cannot be answered from counts-only metrics")
          .doesNotContain("aggregates=[");

      assertThat(sql(query)).hasSize(1).containsExactly(Row.of(3));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, countsTable);
    }
  }

  @TestTemplate
  public void aggregatePushDownSkippedForAvroTable() {
    enableAggregatePushDown();

    String avroTable = "avro_table";
    sql("CREATE TABLE %s (id INT, data VARCHAR) WITH ('write.format.default'='avro')", avroTable);
    try {
      sql("INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'c')", avroTable);

      String query = String.format("SELECT MAX(id) FROM %s", avroTable);
      assertThat(explain(query))
          .as("Avro files carry no column metrics, so MIN/MAX cannot be pushed down")
          .doesNotContain("aggregates=[");

      assertThat(sql(query)).hasSize(1).containsExactly(Row.of(3));
    } finally {
      sql("DROP TABLE IF EXISTS %s.%s", DATABASE_NAME, avroTable);
    }
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
        partitionedTable, format().name());
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
        partitionedTable, format().name());
    try {
      sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20),(3,'b',30),(4,'b',40)", partitionedTable);

      String alignedQuery = String.format("SELECT id FROM %s WHERE data = 'a'", partitionedTable);
      assertThat(explain(alignedQuery))
          .as("A partition-aligned filter should be fully handled by the scan, not re-applied")
          .doesNotContain("Calc");
      assertThat(sql(alignedQuery)).containsExactlyInAnyOrder(Row.of(1), Row.of(2));

      String nonAligned = String.format("SELECT id FROM %s WHERE id > 2", partitionedTable);
      assertThat(explain(nonAligned))
          .as("A non-aligned filter should be re-applied above the scan")
          .contains("Calc");
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
