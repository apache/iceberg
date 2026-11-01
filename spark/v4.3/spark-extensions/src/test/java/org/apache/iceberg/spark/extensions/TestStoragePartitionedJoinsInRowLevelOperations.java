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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.RowLevelOperationMode.COPY_ON_WRITE;
import static org.apache.iceberg.RowLevelOperationMode.MERGE_ON_READ;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestStoragePartitionedJoinsInRowLevelOperations extends ExtensionsTestBase {

  private static final String OTHER_TABLE_NAME = "other_table";

  // open file cost and split size are set as 16 MB to produce a split per file
  private static final Map<String, String> COMMON_TABLE_PROPERTIES =
      ImmutableMap.of(
          TableProperties.FORMAT_VERSION,
          "2",
          TableProperties.SPLIT_SIZE,
          "16777216",
          TableProperties.SPLIT_OPEN_FILE_COST,
          "16777216");

  // only v2 bucketing and preserve data grouping properties have to be enabled to trigger SPJ
  // other properties are only to simplify testing and validation
  private static final Map<String, String> ENABLED_SPJ_SQL_CONF =
      ImmutableMap.of(
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "true",
          SQLConf.V2_BUCKETING_PUSH_PART_VALUES_ENABLED().key(),
          "true",
          SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
          "false",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
          "-1",
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true");

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      }
    };
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName(OTHER_TABLE_NAME));
  }

  @TestTemplate
  public void testCopyOnWriteDeleteWithoutShuffles() {
    checkDelete(COPY_ON_WRITE);
  }

  @TestTemplate
  public void testMergeOnReadDeleteWithoutShuffles() {
    checkDelete(MERGE_ON_READ);
  }

  private void checkDelete(RowLevelOperationMode mode) {
    String createTableStmt =
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep) "
            + "TBLPROPERTIES (%s)";

    sql(createTableStmt, tableName, tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName, "{ \"id\": 1, \"salary\": 100, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 2, \"salary\": 200, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"salary\": 300, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 4, \"salary\": 400, \"dep\": \"hardware\" }");

    sql(createTableStmt, tableName(OTHER_TABLE_NAME), tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 1, \"salary\": 110, \"dep\": \"hr\" }");
    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 5, \"salary\": 500, \"dep\": \"hr\" }");

    Map<String, String> deleteTableProps =
        ImmutableMap.of(
            TableProperties.DELETE_MODE,
            mode.modeName(),
            TableProperties.DELETE_DISTRIBUTION_MODE,
            "none");

    sql("ALTER TABLE %s SET TBLPROPERTIES(%s)", tableName, tablePropsAsString(deleteTableProps));

    withSQLConf(
        ENABLED_SPJ_SQL_CONF,
        () -> {
          SparkPlan plan =
              executeAndKeepPlan(
                  "DELETE FROM %s t WHERE "
                      + "EXISTS (SELECT 1 FROM %s s WHERE t.id = s.id AND t.dep = s.dep)",
                  tableName, tableName(OTHER_TABLE_NAME));
          String planAsString = plan.toString();
          if (mode == COPY_ON_WRITE) {
            int actualNumShuffles = StringUtils.countMatches(planAsString, "Exchange");
            assertThat(actualNumShuffles).as("Should be 1 shuffle with SPJ").isEqualTo(1);
            assertThat(planAsString).contains("Exchange hashpartitioning(_file");
          } else {
            assertThat(planAsString).doesNotContain("Exchange");
          }
        });

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, 200, "hr"), // remaining
            row(3, 300, "hr"), // remaining
            row(4, 400, "hardware")); // remaining

    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id, salary", tableName));
  }

  @TestTemplate
  public void testCopyOnWriteUpdateWithoutShuffles() {
    checkUpdate(COPY_ON_WRITE);
  }

  @TestTemplate
  public void testMergeOnReadUpdateWithoutShuffles() {
    checkUpdate(MERGE_ON_READ);
  }

  private void checkUpdate(RowLevelOperationMode mode) {
    String createTableStmt =
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep) "
            + "TBLPROPERTIES (%s)";

    sql(createTableStmt, tableName, tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName, "{ \"id\": 1, \"salary\": 100, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 2, \"salary\": 200, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"salary\": 300, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 4, \"salary\": 400, \"dep\": \"hardware\" }");

    sql(createTableStmt, tableName(OTHER_TABLE_NAME), tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 1, \"salary\": 110, \"dep\": \"hr\" }");
    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 5, \"salary\": 500, \"dep\": \"hr\" }");

    Map<String, String> updateTableProps =
        ImmutableMap.of(
            TableProperties.UPDATE_MODE,
            mode.modeName(),
            TableProperties.UPDATE_DISTRIBUTION_MODE,
            "none");

    sql("ALTER TABLE %s SET TBLPROPERTIES(%s)", tableName, tablePropsAsString(updateTableProps));

    withSQLConf(
        ENABLED_SPJ_SQL_CONF,
        () -> {
          SparkPlan plan =
              executeAndKeepPlan(
                  "UPDATE %s t SET salary = -1 WHERE "
                      + "EXISTS (SELECT 1 FROM %s s WHERE t.id = s.id AND t.dep = s.dep)",
                  tableName, tableName(OTHER_TABLE_NAME));
          String planAsString = plan.toString();
          if (mode == COPY_ON_WRITE) {
            int actualNumShuffles = StringUtils.countMatches(planAsString, "Exchange");
            assertThat(actualNumShuffles).as("Should be 1 shuffle with SPJ").isEqualTo(1);
            assertThat(planAsString).contains("Exchange hashpartitioning(_file");
          } else {
            assertThat(planAsString).doesNotContain("Exchange");
          }
        });

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, -1, "hr"), // updated
            row(2, 200, "hr"), // existing
            row(3, 300, "hr"), // existing
            row(4, 400, "hardware")); // existing

    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id, salary", tableName));
  }

  @TestTemplate
  public void testCopyOnWriteMergeWithoutShuffles() {
    checkMerge(COPY_ON_WRITE, false /* with ON predicate */);
  }

  @TestTemplate
  public void testCopyOnWriteMergeWithoutShufflesWithPredicate() {
    checkMerge(COPY_ON_WRITE, true /* with ON predicate */);
  }

  @TestTemplate
  public void testMergeOnReadMergeWithoutShuffles() {
    checkMerge(MERGE_ON_READ, false /* with ON predicate */);
  }

  @TestTemplate
  public void testMergeOnReadMergeWithoutShufflesWithPredicate() {
    checkMerge(MERGE_ON_READ, true /* with ON predicate */);
  }

  private void checkMerge(RowLevelOperationMode mode, boolean withPredicate) {
    String createTableStmt =
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep) "
            + "TBLPROPERTIES (%s)";

    sql(createTableStmt, tableName, tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName, "{ \"id\": 1, \"salary\": 100, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 2, \"salary\": 200, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"salary\": 300, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 4, \"salary\": 400, \"dep\": \"hardware\" }");
    append(tableName, "{ \"id\": 6, \"salary\": 600, \"dep\": \"software\" }");

    sql(createTableStmt, tableName(OTHER_TABLE_NAME), tablePropsAsString(COMMON_TABLE_PROPERTIES));

    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 1, \"salary\": 110, \"dep\": \"hr\" }");
    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 5, \"salary\": 500, \"dep\": \"hr\" }");
    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 6, \"salary\": 300, \"dep\": \"software\" }");
    append(tableName(OTHER_TABLE_NAME), "{ \"id\": 10, \"salary\": 1000, \"dep\": \"ops\" }");

    Map<String, String> mergeTableProps =
        ImmutableMap.of(
            TableProperties.MERGE_MODE,
            mode.modeName(),
            TableProperties.MERGE_DISTRIBUTION_MODE,
            "none");

    sql("ALTER TABLE %s SET TBLPROPERTIES(%s)", tableName, tablePropsAsString(mergeTableProps));

    withSQLConf(
        ENABLED_SPJ_SQL_CONF,
        () -> {
          String predicate = withPredicate ? "AND t.dep IN ('hr', 'ops', 'software')" : "";
          SparkPlan plan =
              executeAndKeepPlan(
                  "MERGE INTO %s AS t USING %s AS s "
                      + "ON t.id = s.id AND t.dep = s.dep %s "
                      + "WHEN MATCHED THEN "
                      + "  UPDATE SET t.salary = s.salary "
                      + "WHEN NOT MATCHED THEN "
                      + "  INSERT *",
                  tableName, tableName(OTHER_TABLE_NAME), predicate);
          String planAsString = plan.toString();
          if (mode == COPY_ON_WRITE) {
            int actualNumShuffles = StringUtils.countMatches(planAsString, "Exchange");
            assertThat(actualNumShuffles).as("Should be 1 shuffle with SPJ").isEqualTo(1);
            assertThat(planAsString).contains("Exchange hashpartitioning(_file");
          } else {
            assertThat(planAsString).doesNotContain("Exchange");
          }
        });

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 110, "hr"), // updated
            row(2, 200, "hr"), // existing
            row(3, 300, "hr"), // existing
            row(4, 400, "hardware"), // existing
            row(5, 500, "hr"), // new
            row(6, 300, "software"), // updated
            row(10, 1000, "ops")); // new

    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT * FROM %s ORDER BY id, salary", tableName));
  }
}
