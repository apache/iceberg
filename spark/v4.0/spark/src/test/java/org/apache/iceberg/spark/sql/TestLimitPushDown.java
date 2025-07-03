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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.TableProperties;
import org.apache.iceberg.events.LimitAwareScanTaskEvent;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestLimitPushDown extends CatalogTestBase {
  private int limitAwareScanTaskEventCount = 0;
  private LimitAwareScanTaskEvent lastLimitAwareScanTaskEvent = null;

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @BeforeEach
  public void createTables() {
    enableLimitPushDown(tableName);
    disablePreserveDataGrouping(tableName);
    // register a scan event listener to validate limit push down
    Listeners.register(
        event -> {
          limitAwareScanTaskEventCount += 1;
          lastLimitAwareScanTaskEvent = event;
        },
        LimitAwareScanTaskEvent.class);

    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg PARTITIONED BY (id)",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0)", tableName);
    sql("INSERT INTO %s VALUES (2, 'b', 2.0)", tableName);
    sql("INSERT INTO %s VALUES (3, 'c', 3.0)", tableName);
    sql("INSERT INTO %s VALUES (4, 'd', 4.0)", tableName);
    sql("INSERT INTO %s VALUES (5, 'e', 5.0)", tableName);

    this.limitAwareScanTaskEventCount = 0;
    this.lastLimitAwareScanTaskEvent = null;
  }

  @TestTemplate
  public void testLimitPushDownWithSimpleSelect() {
    String query = String.format("SELECT * FROM %s limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    assertThat(limitAwareScanTaskEventCount)
        .as("Should only load two limit aware scan tasks")
        .isEqualTo(2);
    pushedLimitMustMatch(query, "2");
  }

  @TestTemplate
  public void testLimitPushDownWithSimpleSelectWhenEnablePreserveDataGrouping() {
    enablePreserveDataGrouping(tableName);
    String query = String.format("SELECT * FROM %s limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    // limit push down doesn't work with PreserveDataGrouping
    assertThat(limitAwareScanTaskEventCount)
        .as("Should only load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitMustMatch(query, "None");
    enablePreserveDataGrouping(tableName);
  }

  @TestTemplate
  public void testLimitPushDownZeroWithSimpleSelect() {
    String query = String.format("SELECT * FROM %s limit 0", tableName);

    assertThat(sql(query).size()).as("Should return 0 row").isEqualTo(0);

    // Spark will convert limit 0 to an empty table scan
    assertThat(limitAwareScanTaskEventCount)
        .as("Should only load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitZeroMustMatch(query);
  }

  @TestTemplate
  public void testLimitPushDownDisabledWithSimpleSelect() {
    disableLimitPushDown(tableName);
    String query = String.format("SELECT * FROM %s limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    assertThat(limitAwareScanTaskEventCount)
        .as("Should only load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitMustMatch(query, "None");
  }

  @TestTemplate
  public void testLimitPushDownWithSimpleSelectWithDeleteFiles() {
    enableMergeOnReadDelete(tableName);
    sql("DELETE FROM %s WHERE id = 2", tableName);

    String query = String.format("SELECT * FROM %s limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    assertThat(limitAwareScanTaskEventCount)
        .as("Should load three or less limit aware scan tasks")
        .isLessThanOrEqualTo(3);
    pushedLimitMustMatch(query, "2");
  }

  @TestTemplate
  public void testLimitPushDownWithPartitionPruning() {
    String query = String.format("SELECT * FROM %s where id != 4 limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    // Spark push down LIMIT when entire partition been pruned
    assertThat(limitAwareScanTaskEventCount)
        .as("Should only load two limit aware scan tasks")
        .isEqualTo(2);
    pushedLimitMustMatch(query, "2");
  }

  @TestTemplate
  public void testLimitPushDownWithFilter() {
    String query = String.format("SELECT * FROM %s where data != 'a' limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    // Spark does not push down LIMIT when filters are present in the query plan
    assertThat(limitAwareScanTaskEventCount)
        .as("Should load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitMustMatch(query, "None");
  }

  @TestTemplate
  public void testLimitPushDownWithAggregate() {
    String query = String.format("SELECT count(*) FROM %s group by id limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    // Spark doesn't support limit push down with aggregate
    assertThat(limitAwareScanTaskEventCount)
        .as("Should load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitMustMatch(query, "None");
  }

  @TestTemplate
  public void testLimitPushDownWithOrderBy() {
    String query = String.format("SELECT * FROM %s order by id limit 2", tableName);

    assertThat(sql(query).size()).as("Should return 2 rows").isEqualTo(2);

    // Spark doesn't support limit push down with order by clause
    assertThat(limitAwareScanTaskEventCount)
        .as("Should load 0 limit aware scan tasks")
        .isEqualTo(0);
    pushedLimitMustMatch(query, "None");
  }

  private void pushedLimitMustMatch(String query, String pushedLimit) {
    SparkPlan sparkPlan = executeAndKeepPlan(() -> sql(query));
    String planAsString = sparkPlan.toString().replaceAll("#(\\d+L?)", "");

    assertThat(planAsString).as("Pushed limit must match").contains("pushedLimit=" + pushedLimit);
  }

  private void pushedLimitZeroMustMatch(String query) {
    SparkPlan sparkPlan = executeAndKeepPlan(() -> sql(query));
    String planAsString = sparkPlan.toString().replaceAll("#(\\d+L?)", "");

    assertThat(planAsString).as("Pushed limit zero must match").contains("LocalTableScan <empty>");
  }

  private void disableLimitPushDown(String table) {
    spark.conf().set(SparkSQLProperties.LIMIT_PUSH_DOWN_ENABLED, "false");
  }

  private void enableLimitPushDown(String table) {
    spark.conf().set(SparkSQLProperties.LIMIT_PUSH_DOWN_ENABLED, "true");
  }

  private void enablePreserveDataGrouping(String table) {
    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_GROUPING, "true");
  }

  private void disablePreserveDataGrouping(String table) {
    spark.conf().set(SparkSQLProperties.PRESERVE_DATA_GROUPING, "false");
  }

  private void enableMergeOnReadDelete(String table) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        table, TableProperties.DELETE_MODE, MERGE_ON_READ.modeName());
  }
}
