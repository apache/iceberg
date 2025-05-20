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

import java.util.List;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke;
import org.apache.spark.sql.execution.CommandResultExec;
import org.apache.spark.sql.execution.datasources.v2.V2TableWriteExec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSystemFunctionPushDownInRowLevelOperations extends ExtensionsTestBase {

  private static final String CHANGES_TABLE_NAME = "changes";

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

  @BeforeEach
  public void beforeEach() {
    sql("USE %s", catalogName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s PURGE", tableName);
    sql("DROP TABLE IF EXISTS %s PURGE", tableName(CHANGES_TABLE_NAME));
  }

  @TestTemplate
  public void testCopyOnWriteDeleteBucketTransformInPredicate() {
    initTable("bucket(4, dep)");
    checkDelete(COPY_ON_WRITE, "system.bucket(4, dep) IN (2, 3)");
  }

  @TestTemplate
  public void testMergeOnReadDeleteBucketTransformInPredicate() {
    initTable("bucket(4, dep)");
    checkDelete(MERGE_ON_READ, "system.bucket(4, dep) IN (2, 3)");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteBucketTransformEqPredicate() {
    initTable("bucket(4, dep)");
    checkDelete(COPY_ON_WRITE, "system.bucket(4, dep) = 2");
  }

  @TestTemplate
  public void testMergeOnReadDeleteBucketTransformEqPredicate() {
    initTable("bucket(4, dep)");
    checkDelete(MERGE_ON_READ, "system.bucket(4, dep) = 2");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteYearsTransform() {
    initTable("years(ts)");
    checkDelete(COPY_ON_WRITE, "system.years(ts) > 30");
  }

  @TestTemplate
  public void testMergeOnReadDeleteYearsTransform() {
    initTable("years(ts)");
    checkDelete(MERGE_ON_READ, "system.years(ts) <= 30");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteMonthsTransform() {
    initTable("months(ts)");
    checkDelete(COPY_ON_WRITE, "system.months(ts) <= 250");
  }

  @TestTemplate
  public void testMergeOnReadDeleteMonthsTransform() {
    initTable("months(ts)");
    checkDelete(MERGE_ON_READ, "system.months(ts) > 250");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteDaysTransform() {
    initTable("days(ts)");
    checkDelete(COPY_ON_WRITE, "system.days(ts) <= date('2000-01-03 00:00:00')");
  }

  @TestTemplate
  public void testMergeOnReadDeleteDaysTransform() {
    initTable("days(ts)");
    checkDelete(MERGE_ON_READ, "system.days(ts) > date('2000-01-03 00:00:00')");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteHoursTransform() {
    initTable("hours(ts)");
    checkDelete(COPY_ON_WRITE, "system.hours(ts) <= 100000");
  }

  @TestTemplate
  public void testMergeOnReadDeleteHoursTransform() {
    initTable("hours(ts)");
    checkDelete(MERGE_ON_READ, "system.hours(ts) > 100000");
  }

  @TestTemplate
  public void testCopyOnWriteDeleteTruncateTransform() {
    initTable("truncate(1, dep)");
    checkDelete(COPY_ON_WRITE, "system.truncate(1, dep) = 'i'");
  }

  @TestTemplate
  public void testMergeOnReadDeleteTruncateTransform() {
    initTable("truncate(1, dep)");
    checkDelete(MERGE_ON_READ, "system.truncate(1, dep) = 'i'");
  }

  @TestTemplate
  public void testCopyOnWriteUpdateBucketTransform() {
    initTable("bucket(4, dep)");
    checkUpdate(COPY_ON_WRITE, "system.bucket(4, dep) IN (2, 3)");
  }

  @TestTemplate
  public void testMergeOnReadUpdateBucketTransform() {
    initTable("bucket(4, dep)");
    checkUpdate(MERGE_ON_READ, "system.bucket(4, dep) = 2");
  }

  @TestTemplate
  public void testCopyOnWriteUpdateYearsTransform() {
    initTable("years(ts)");
    checkUpdate(COPY_ON_WRITE, "system.years(ts) > 30");
  }

  @TestTemplate
  public void testMergeOnReadUpdateYearsTransform() {
    initTable("years(ts)");
    checkUpdate(MERGE_ON_READ, "system.years(ts) <= 30");
  }

  @TestTemplate
  public void testCopyOnWriteMergeBucketTransform() {
    initTable("bucket(4, dep)");
    checkMerge(COPY_ON_WRITE, "system.bucket(4, dep) IN (2, 3)");
  }

  @TestTemplate
  public void testMergeOnReadMergeBucketTransform() {
    initTable("bucket(4, dep)");
    checkMerge(MERGE_ON_READ, "system.bucket(4, dep) = 2");
  }

  @TestTemplate
  public void testCopyOnWriteMergeYearsTransform() {
    initTable("years(ts)");
    checkMerge(COPY_ON_WRITE, "system.years(ts) > 30");
  }

  @TestTemplate
  public void testMergeOnReadMergeYearsTransform() {
    initTable("years(ts)");
    checkMerge(MERGE_ON_READ, "system.years(ts) <= 30");
  }

  @TestTemplate
  public void testCopyOnWriteMergeTruncateTransform() {
    initTable("truncate(1, dep)");
    checkMerge(COPY_ON_WRITE, "system.truncate(1, dep) = 'i'");
  }

  @TestTemplate
  public void testMergeOnReadMergeTruncateTransform() {
    initTable("truncate(1, dep)");
    checkMerge(MERGE_ON_READ, "system.truncate(1, dep) = 'i'");
  }

  private void checkDelete(RowLevelOperationMode mode, String cond) {
    withUnavailableLocations(
        findIrrelevantFileLocations(cond),
        () -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s', '%s' '%s')",
              tableName,
              TableProperties.DELETE_MODE,
              mode.modeName(),
              TableProperties.DELETE_DISTRIBUTION_MODE,
              DistributionMode.NONE.modeName());

          Dataset<Row> changeDF = spark.table(tableName).where(cond).limit(2).select("id");
          try {
            changeDF.coalesce(1).writeTo(tableName(CHANGES_TABLE_NAME)).create();
          } catch (TableAlreadyExistsException e) {
            throw new AlreadyExistsException(
                "Cannot create table %s as it already exists", CHANGES_TABLE_NAME);
          }

          List<Expression> calls =
              executeAndCollectFunctionCalls(
                  "DELETE FROM %s t WHERE %s AND t.id IN (SELECT id FROM %s)",
                  tableName, cond, tableName(CHANGES_TABLE_NAME));
          // CoW planning currently does not optimize post-scan filters in DELETE
          int expectedCallCount = mode == COPY_ON_WRITE ? 1 : 0;
          assertThat(calls).hasSize(expectedCallCount);

          assertEquals(
              "Should have no matching rows",
              ImmutableList.of(),
              sql(
                  "SELECT * FROM %s WHERE %s AND id IN (SELECT * FROM %s)",
                  tableName, cond, tableName(CHANGES_TABLE_NAME)));
        });
  }

  private void checkUpdate(RowLevelOperationMode mode, String cond) {
    withUnavailableLocations(
        findIrrelevantFileLocations(cond),
        () -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s', '%s' '%s')",
              tableName,
              TableProperties.UPDATE_MODE,
              mode.modeName(),
              TableProperties.UPDATE_DISTRIBUTION_MODE,
              DistributionMode.NONE.modeName());

          Dataset<Row> changeDF = spark.table(tableName).where(cond).limit(2).select("id");
          try {
            changeDF.coalesce(1).writeTo(tableName(CHANGES_TABLE_NAME)).create();
          } catch (TableAlreadyExistsException e) {
            throw new AlreadyExistsException(
                "Cannot create table %s as it already exists", CHANGES_TABLE_NAME);
          }

          List<Expression> calls =
              executeAndCollectFunctionCalls(
                  "UPDATE %s t SET t.salary = -1 WHERE %s AND t.id IN (SELECT id FROM %s)",
                  tableName, cond, tableName(CHANGES_TABLE_NAME));
          // CoW planning currently does not optimize post-scan filters in UPDATE
          int expectedCallCount = mode == COPY_ON_WRITE ? 2 : 0;
          assertThat(calls).hasSize(expectedCallCount);

          assertEquals(
              "Should have correct updates",
              sql("SELECT id FROM %s", tableName(CHANGES_TABLE_NAME)),
              sql("SELECT id FROM %s WHERE %s AND salary = -1", tableName, cond));
        });
  }

  private void checkMerge(RowLevelOperationMode mode, String cond) {
    withUnavailableLocations(
        findIrrelevantFileLocations(cond),
        () -> {
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s', '%s' '%s')",
              tableName,
              TableProperties.MERGE_MODE,
              mode.modeName(),
              TableProperties.MERGE_DISTRIBUTION_MODE,
              DistributionMode.NONE.modeName());

          Dataset<Row> changeDF =
              spark.table(tableName).where(cond).limit(2).selectExpr("id + 1 as id");
          try {
            changeDF.coalesce(1).writeTo(tableName(CHANGES_TABLE_NAME)).create();
          } catch (TableAlreadyExistsException e) {
            throw new AlreadyExistsException(
                "Cannot create table %s as it already exists", CHANGES_TABLE_NAME);
          }

          List<Expression> calls =
              executeAndCollectFunctionCalls(
                  "MERGE INTO %s t USING %s s "
                      + "ON t.id == s.id AND %s "
                      + "WHEN MATCHED THEN "
                      + "  UPDATE SET salary = -1 "
                      + "WHEN NOT MATCHED AND s.id = 2 THEN "
                      + "  INSERT (id, salary, dep, ts) VALUES (100, -1, 'hr', null)",
                  tableName, tableName(CHANGES_TABLE_NAME), cond);
          assertThat(calls).isEmpty();

          assertEquals(
              "Should have correct updates",
              sql("SELECT id FROM %s", tableName(CHANGES_TABLE_NAME)),
              sql("SELECT id FROM %s WHERE %s AND salary = -1", tableName, cond));
        });
  }

  private List<Expression> executeAndCollectFunctionCalls(String query, Object... args) {
    CommandResultExec command = (CommandResultExec) executeAndKeepPlan(query, args);
    V2TableWriteExec write = (V2TableWriteExec) command.commandPhysicalPlan();
    return SparkPlanUtil.collectExprs(
        write.query(),
        expr -> expr instanceof StaticInvoke || expr instanceof ApplyFunctionExpression);
  }

  private List<String> findIrrelevantFileLocations(String cond) {
    return spark
        .table(tableName)
        .where("NOT " + cond)
        .select(MetadataColumns.FILE_PATH.name())
        .distinct()
        .as(Encoders.STRING())
        .collectAsList();
  }

  private void initTable(String transform) {
    sql(
        "CREATE TABLE %s (id BIGINT, salary INT, dep STRING, ts TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (%s)",
        tableName, transform);

    append(
        tableName,
        "{ \"id\": 1, \"salary\": 100, \"dep\": \"hr\", \"ts\": \"1975-01-01 06:00:00\" }",
        "{ \"id\": 2, \"salary\": 200, \"dep\": \"hr\", \"ts\": \"1975-01-01 06:00:00\" }",
        "{ \"id\": 3, \"salary\": 300, \"dep\": \"hr\", \"ts\": \"1975-01-01 06:00:00\" }",
        "{ \"id\": 4, \"salary\": 400, \"dep\": \"it\", \"ts\": \"2020-01-01 10:00:00\" }",
        "{ \"id\": 5, \"salary\": 500, \"dep\": \"it\", \"ts\": \"2020-01-01 10:00:00\" }",
        "{ \"id\": 6, \"salary\": 600, \"dep\": \"it\", \"ts\": \"2020-01-01 10:00:00\" }");
  }
}
