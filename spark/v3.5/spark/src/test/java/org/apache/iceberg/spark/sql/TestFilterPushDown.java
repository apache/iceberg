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

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.execution.SparkPlan;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFilterPushDown extends SparkTestBaseWithCatalog {

  @Parameterized.Parameters(name = "planningMode = {0}")
  public static Object[] parameters() {
    return new Object[] {LOCAL, DISTRIBUTED};
  }

  private final PlanningMode planningMode;

  public TestFilterPushDown(PlanningMode planningMode) {
    this.planningMode = planningMode;
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS tmp_view");
  }

  @Test
  public void testFilterPushdownWithDecimalValues() {
    sql(
        "CREATE TABLE %s (id INT, salary DECIMAL(10, 2), dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100.01, 'd1')", tableName);
    sql("INSERT INTO %s VALUES (2, 100.05, 'd1')", tableName);

    checkFilters(
        "dep = 'd1' AND salary > 100.03" /* query predicate */,
        "isnotnull(salary) AND (salary > 100.03)" /* Spark post scan filter */,
        "dep IS NOT NULL, salary IS NOT NULL, dep = 'd1', salary > 100.03" /* Iceberg scan filters */,
        ImmutableList.of(row(2, new BigDecimal("100.05"), "d1")));
  }

  @Test
  public void testFilterPushdownWithIdentityTransform() {
    sql(
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, 'd1')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, 'd2')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, 'd3')", tableName);
    sql("INSERT INTO %s VALUES (4, 400, 'd4')", tableName);
    sql("INSERT INTO %s VALUES (5, 500, 'd5')", tableName);
    sql("INSERT INTO %s VALUES (6, 600, null)", tableName);

    checkOnlyIcebergFilters(
        "dep IS NULL" /* query predicate */,
        "dep IS NULL" /* Iceberg scan filters */,
        ImmutableList.of(row(6, 600, null)));

    checkOnlyIcebergFilters(
        "dep IS NOT NULL" /* query predicate */,
        "dep IS NOT NULL" /* Iceberg scan filters */,
        ImmutableList.of(
            row(1, 100, "d1"),
            row(2, 200, "d2"),
            row(3, 300, "d3"),
            row(4, 400, "d4"),
            row(5, 500, "d5")));

    checkOnlyIcebergFilters(
        "dep = 'd3'" /* query predicate */,
        "dep IS NOT NULL, dep = 'd3'" /* Iceberg scan filters */,
        ImmutableList.of(row(3, 300, "d3")));

    checkOnlyIcebergFilters(
        "dep > 'd3'" /* query predicate */,
        "dep IS NOT NULL, dep > 'd3'" /* Iceberg scan filters */,
        ImmutableList.of(row(4, 400, "d4"), row(5, 500, "d5")));

    checkOnlyIcebergFilters(
        "dep >= 'd5'" /* query predicate */,
        "dep IS NOT NULL, dep >= 'd5'" /* Iceberg scan filters */,
        ImmutableList.of(row(5, 500, "d5")));

    checkOnlyIcebergFilters(
        "dep < 'd2'" /* query predicate */,
        "dep IS NOT NULL, dep < 'd2'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkOnlyIcebergFilters(
        "dep <= 'd2'" /* query predicate */,
        "dep IS NOT NULL, dep <= 'd2'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(2, 200, "d2")));

    checkOnlyIcebergFilters(
        "dep <=> 'd3'" /* query predicate */,
        "dep = 'd3'" /* Iceberg scan filters */,
        ImmutableList.of(row(3, 300, "d3")));

    checkOnlyIcebergFilters(
        "dep IN (null, 'd1')" /* query predicate */,
        "dep IN ('d1')" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkOnlyIcebergFilters(
        "dep NOT IN ('d2', 'd4')" /* query predicate */,
        "(dep IS NOT NULL AND dep NOT IN ('d2', 'd4'))" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(3, 300, "d3"), row(5, 500, "d5")));

    checkOnlyIcebergFilters(
        "dep = 'd1' AND dep IS NOT NULL" /* query predicate */,
        "dep = 'd1', dep IS NOT NULL" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkOnlyIcebergFilters(
        "dep = 'd1' OR dep = 'd2' OR dep = 'd3'" /* query predicate */,
        "((dep = 'd1' OR dep = 'd2') OR dep = 'd3')" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(2, 200, "d2"), row(3, 300, "d3")));

    checkFilters(
        "dep = 'd1' AND id = 1" /* query predicate */,
        "isnotnull(id) AND (id = 1)" /* Spark post scan filter */,
        "dep IS NOT NULL, id IS NOT NULL, dep = 'd1', id = 1" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkFilters(
        "dep = 'd2' OR id = 1" /* query predicate */,
        "(dep = d2) OR (id = 1)" /* Spark post scan filter */,
        "(dep = 'd2' OR id = 1)" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(2, 200, "d2")));

    checkFilters(
        "dep LIKE 'd1%' AND id = 1" /* query predicate */,
        "isnotnull(id) AND (id = 1)" /* Spark post scan filter */,
        "dep IS NOT NULL, id IS NOT NULL, dep LIKE 'd1%', id = 1" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkFilters(
        "dep NOT LIKE 'd5%' AND (id = 1 OR id = 5)" /* query predicate */,
        "(id = 1) OR (id = 5)" /* Spark post scan filter */,
        "dep IS NOT NULL, NOT (dep LIKE 'd5%'), (id = 1 OR id = 5)" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    checkFilters(
        "dep LIKE '%d5' AND id IN (1, 5)" /* query predicate */,
        "EndsWith(dep, d5) AND id IN (1,5)" /* Spark post scan filter */,
        "dep IS NOT NULL, id IN (1, 5)" /* Iceberg scan filters */,
        ImmutableList.of(row(5, 500, "d5")));
  }

  @Test
  public void testFilterPushdownWithHoursTransform() {
    sql(
        "CREATE TABLE %s (id INT, price INT, t TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (hours(t))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, TIMESTAMP '2021-06-30T01:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2021-06-30T02:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, null)", tableName);

    withDefaultTimeZone(
        "UTC",
        () -> {
          checkOnlyIcebergFilters(
              "t IS NULL" /* query predicate */,
              "t IS NULL" /* Iceberg scan filters */,
              ImmutableList.of(row(3, 300, null)));

          // strict/inclusive projections for t < TIMESTAMP '2021-06-30T02:00:00.000Z' are equal,
          // so this filter selects entire partitions and can be pushed down completely
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2021-06-30T02:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1625018400000000" /* Iceberg scan filters */,
              ImmutableList.of(row(1, 100, timestamp("2021-06-30T01:00:00.0Z"))));

          // strict/inclusive projections for t < TIMESTAMP '2021-06-30T01:00:00.001Z' differ,
          // so this filter does NOT select entire partitions and can't be pushed down completely
          checkFilters(
              "t < TIMESTAMP '2021-06-30T01:00:00.001Z'" /* query predicate */,
              "t < 2021-06-30 01:00:00.001" /* Spark post scan filter */,
              "t IS NOT NULL, t < 1625014800001000" /* Iceberg scan filters */,
              ImmutableList.of(row(1, 100, timestamp("2021-06-30T01:00:00.0Z"))));

          // strict/inclusive projections for t <= TIMESTAMP '2021-06-30T01:00:00.000Z' differ,
          // so this filter does NOT select entire partitions and can't be pushed down completely
          checkFilters(
              "t <= TIMESTAMP '2021-06-30T01:00:00.000Z'" /* query predicate */,
              "t <= 2021-06-30 01:00:00" /* Spark post scan filter */,
              "t IS NOT NULL, t <= 1625014800000000" /* Iceberg scan filters */,
              ImmutableList.of(row(1, 100, timestamp("2021-06-30T01:00:00.0Z"))));
        });
  }

  @Test
  public void testFilterPushdownWithDaysTransform() {
    sql(
        "CREATE TABLE %s (id INT, price INT, t TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (days(t))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, TIMESTAMP '2021-06-15T01:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2021-06-30T02:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, TIMESTAMP '2021-07-15T10:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (4, 400, null)", tableName);

    withDefaultTimeZone(
        "UTC",
        () -> {
          checkOnlyIcebergFilters(
              "t IS NULL" /* query predicate */,
              "t IS NULL" /* Iceberg scan filters */,
              ImmutableList.of(row(4, 400, null)));

          // strict/inclusive projections for t < TIMESTAMP '2021-07-05T00:00:00.000Z' are equal,
          // so this filter selects entire partitions and can be pushed down completely
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2021-07-05T00:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1625443200000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-15T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));

          // strict/inclusive projections for t < TIMESTAMP '2021-06-30T03:00:00.000Z' differ,
          // so this filter does NOT select entire partitions and can't be pushed down completely
          checkFilters(
              "t < TIMESTAMP '2021-06-30T03:00:00.000Z'" /* query predicate */,
              "t < 2021-06-30 03:00:00" /* Spark post scan filter */,
              "t IS NOT NULL, t < 1625022000000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-15T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));
        });
  }

  @Test
  public void testFilterPushdownWithMonthsTransform() {
    sql(
        "CREATE TABLE %s (id INT, price INT, t TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (months(t))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, TIMESTAMP '2021-06-30T01:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2021-06-30T02:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, TIMESTAMP '2021-07-15T10:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (4, 400, null)", tableName);

    withDefaultTimeZone(
        "UTC",
        () -> {
          checkOnlyIcebergFilters(
              "t IS NULL" /* query predicate */,
              "t IS NULL" /* Iceberg scan filters */,
              ImmutableList.of(row(4, 400, null)));

          // strict/inclusive projections for t < TIMESTAMP '2021-07-01T00:00:00.000Z' are equal,
          // so this filter selects entire partitions and can be pushed down completely
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2021-07-01T00:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1625097600000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-30T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));

          // strict/inclusive projections for t < TIMESTAMP '2021-06-30T03:00:00.000Z' differ,
          // so this filter does NOT select entire partitions and can't be pushed down completely
          checkFilters(
              "t < TIMESTAMP '2021-06-30T03:00:00.000Z'" /* query predicate */,
              "t < 2021-06-30 03:00:00" /* Spark post scan filter */,
              "t IS NOT NULL, t < 1625022000000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-30T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));
        });
  }

  @Test
  public void testFilterPushdownWithYearsTransform() {
    sql(
        "CREATE TABLE %s (id INT, price INT, t TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (years(t))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, TIMESTAMP '2021-06-30T01:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2021-06-30T02:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2022-09-25T02:00:00.000Z')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, null)", tableName);

    withDefaultTimeZone(
        "UTC",
        () -> {
          checkOnlyIcebergFilters(
              "t IS NULL" /* query predicate */,
              "t IS NULL" /* Iceberg scan filters */,
              ImmutableList.of(row(3, 300, null)));

          // strict/inclusive projections for t < TIMESTAMP '2022-01-01T00:00:00.000Z' are equal,
          // so this filter selects entire partitions and can be pushed down completely
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2022-01-01T00:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1640995200000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-30T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));

          // strict/inclusive projections for t < TIMESTAMP '2021-06-30T03:00:00.000Z' differ,
          // so this filter does NOT select entire partitions and can't be pushed down completely
          checkFilters(
              "t < TIMESTAMP '2021-06-30T03:00:00.000Z'" /* query predicate */,
              "t < 2021-06-30 03:00:00" /* Spark post scan filter */,
              "t IS NOT NULL, t < 1625022000000000" /* Iceberg scan filters */,
              ImmutableList.of(
                  row(1, 100, timestamp("2021-06-30T01:00:00.000Z")),
                  row(2, 200, timestamp("2021-06-30T02:00:00.000Z"))));
        });
  }

  @Test
  public void testFilterPushdownWithBucketTransform() {
    sql(
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep, bucket(8, id))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, 'd1')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, 'd2')", tableName);

    checkFilters(
        "dep = 'd1' AND id = 1" /* query predicate */,
        "id = 1" /* Spark post scan filter */,
        "dep IS NOT NULL, id IS NOT NULL, dep = 'd1'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));
  }

  @Test
  public void testFilterPushdownWithTruncateTransform() {
    sql(
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (truncate(1, dep))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, 'd1')", tableName);
    sql("INSERT INTO %s VALUES (2, 200, 'd2')", tableName);
    sql("INSERT INTO %s VALUES (3, 300, 'a3')", tableName);

    checkOnlyIcebergFilters(
        "dep LIKE 'd%'" /* query predicate */,
        "dep IS NOT NULL, dep LIKE 'd%'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(2, 200, "d2")));

    checkFilters(
        "dep = 'd1'" /* query predicate */,
        "dep = d1" /* Spark post scan filter */,
        "dep IS NOT NULL" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));
  }

  @Test
  public void testFilterPushdownWithSpecEvolutionAndIdentityTransforms() {
    sql(
        "CREATE TABLE %s (id INT, salary INT, dep STRING, sub_dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (dep)",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, 'd1', 'sd1')", tableName);

    // the filter can be pushed completely because all specs include identity(dep)
    checkOnlyIcebergFilters(
        "dep = 'd1'" /* query predicate */,
        "dep IS NOT NULL, dep = 'd1'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1", "sd1")));

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("sub_dep").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s VALUES (2, 200, 'd2', 'sd2')", tableName);

    // the filter can be pushed completely because all specs include identity(dep)
    checkOnlyIcebergFilters(
        "dep = 'd1'" /* query predicate */,
        "dep IS NOT NULL, dep = 'd1'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1", "sd1")));

    table.updateSpec().removeField("sub_dep").removeField("dep").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s VALUES (3, 300, 'd3', 'sd3')", tableName);

    // the filter can't be pushed completely because not all specs include identity(dep)
    checkFilters(
        "dep = 'd1'" /* query predicate */,
        "isnotnull(dep) AND (dep = d1)" /* Spark post scan filter */,
        "dep IS NOT NULL, dep = 'd1'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1", "sd1")));
  }

  @Test
  public void testFilterPushdownWithSpecEvolutionAndTruncateTransform() {
    sql(
        "CREATE TABLE %s (id INT, salary INT, dep STRING)"
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, dep))",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100, 'd1')", tableName);

    // the filter can be pushed completely because the current spec supports it
    checkOnlyIcebergFilters(
        "dep LIKE 'd1%'" /* query predicate */,
        "dep IS NOT NULL, dep LIKE 'd1%'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));

    Table table = validationCatalog.loadTable(tableIdent);
    table
        .updateSpec()
        .removeField(Expressions.truncate("dep", 2))
        .addField(Expressions.truncate("dep", 1))
        .commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO %s VALUES (2, 200, 'd2')", tableName);

    // the filter can be pushed completely because both specs support it
    checkOnlyIcebergFilters(
        "dep LIKE 'd%'" /* query predicate */,
        "dep IS NOT NULL, dep LIKE 'd%'" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1"), row(2, 200, "d2")));

    // the filter can't be pushed completely because the second spec is truncate(dep, 1) and
    // the predicate literal is d1, which is two chars
    checkFilters(
        "dep LIKE 'd1%' AND id = 1" /* query predicate */,
        "(isnotnull(id) AND StartsWith(dep, d1)) AND (id = 1)" /* Spark post scan filter */,
        "dep IS NOT NULL, id IS NOT NULL, dep LIKE 'd1%', id = 1" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100, "d1")));
  }

  @Test
  public void testFilterPushdownWithSpecEvolutionAndTimeTransforms() {
    sql(
        "CREATE TABLE %s (id INT, price INT, t TIMESTAMP)"
            + "USING iceberg "
            + "PARTITIONED BY (hours(t))",
        tableName);
    configurePlanningMode(planningMode);

    withDefaultTimeZone(
        "UTC",
        () -> {
          sql("INSERT INTO %s VALUES (1, 100, TIMESTAMP '2021-06-30T01:00:00.000Z')", tableName);

          // the filter can be pushed completely because the current spec supports it
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2021-07-01T00:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1625097600000000" /* Iceberg scan filters */,
              ImmutableList.of(row(1, 100, timestamp("2021-06-30T01:00:00.000Z"))));

          Table table = validationCatalog.loadTable(tableIdent);
          table
              .updateSpec()
              .removeField(Expressions.hour("t"))
              .addField(Expressions.month("t"))
              .commit();
          sql("REFRESH TABLE %s", tableName);
          sql("INSERT INTO %s VALUES (2, 200, TIMESTAMP '2021-05-30T01:00:00.000Z')", tableName);

          // the filter can be pushed completely because both specs support it
          checkOnlyIcebergFilters(
              "t < TIMESTAMP '2021-06-01T00:00:00.000Z'" /* query predicate */,
              "t IS NOT NULL, t < 1622505600000000" /* Iceberg scan filters */,
              ImmutableList.of(row(2, 200, timestamp("2021-05-30T01:00:00.000Z"))));
        });
  }

  @Test
  public void testFilterPushdownWithSpecialFloatingPointPartitionValues() {
    sql(
        "CREATE TABLE %s (id INT, salary DOUBLE)" + "USING iceberg " + "PARTITIONED BY (salary)",
        tableName);
    configurePlanningMode(planningMode);

    sql("INSERT INTO %s VALUES (1, 100.5)", tableName);
    sql("INSERT INTO %s VALUES (2, double('NaN'))", tableName);
    sql("INSERT INTO %s VALUES (3, double('infinity'))", tableName);
    sql("INSERT INTO %s VALUES (4, double('-infinity'))", tableName);

    checkOnlyIcebergFilters(
        "salary = 100.5" /* query predicate */,
        "salary IS NOT NULL, salary = 100.5" /* Iceberg scan filters */,
        ImmutableList.of(row(1, 100.5)));

    checkOnlyIcebergFilters(
        "salary = double('NaN')" /* query predicate */,
        "salary IS NOT NULL, is_nan(salary)" /* Iceberg scan filters */,
        ImmutableList.of(row(2, Double.NaN)));

    checkOnlyIcebergFilters(
        "salary != double('NaN')" /* query predicate */,
        "salary IS NOT NULL, NOT (is_nan(salary))" /* Iceberg scan filters */,
        ImmutableList.of(
            row(1, 100.5), row(3, Double.POSITIVE_INFINITY), row(4, Double.NEGATIVE_INFINITY)));

    checkOnlyIcebergFilters(
        "salary = double('infinity')" /* query predicate */,
        "salary IS NOT NULL, salary = Infinity" /* Iceberg scan filters */,
        ImmutableList.of(row(3, Double.POSITIVE_INFINITY)));

    checkOnlyIcebergFilters(
        "salary = double('-infinity')" /* query predicate */,
        "salary IS NOT NULL, salary = -Infinity" /* Iceberg scan filters */,
        ImmutableList.of(row(4, Double.NEGATIVE_INFINITY)));
  }

  private void checkOnlyIcebergFilters(
      String predicate, String icebergFilters, List<Object[]> expectedRows) {

    checkFilters(predicate, null, icebergFilters, expectedRows);
  }

  private void checkFilters(
      String predicate, String sparkFilter, String icebergFilters, List<Object[]> expectedRows) {

    Action check =
        () -> {
          assertEquals(
              "Rows must match",
              expectedRows,
              sql("SELECT * FROM %s WHERE %s ORDER BY id", tableName, predicate));
        };
    SparkPlan sparkPlan = executeAndKeepPlan(check);
    String planAsString = sparkPlan.toString().replaceAll("#(\\d+L?)", "");

    if (sparkFilter != null) {
      Assertions.assertThat(planAsString)
          .as("Post scan filter should match")
          .contains("Filter (" + sparkFilter + ")");
    } else {
      Assertions.assertThat(planAsString)
          .as("Should be no post scan filter")
          .doesNotContain("Filter (");
    }

    Assertions.assertThat(planAsString)
        .as("Pushed filters must match")
        .contains("[filters=" + icebergFilters + ",");
  }

  private Timestamp timestamp(String timestampAsString) {
    return Timestamp.from(Instant.parse(timestampAsString));
  }
}
