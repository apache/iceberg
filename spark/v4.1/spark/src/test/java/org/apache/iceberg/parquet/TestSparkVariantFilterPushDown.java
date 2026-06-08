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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for Spark SQL variant predicate pushdown via the Iceberg data source. After
 * creating a table with variants (parameterization includes shredded/unshredded), queries are
 * issued against the table.
 *
 * <p>Each test verifies both the rows returned (correctness) and what pushed Iceberg scan filters
 * were present in the physical plan.
 *
 * <p>They also make assertions on how many times {@code
 * ParquetMetricsRowGroupFilter.compareVariant()} has been invoked on shredded data, which is why it
 * needs to be in the same package as that class.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkVariantFilterPushDown extends TestBaseWithCatalog {

  public static final String SPARK_VARIANT_GET =
      "variant_get(nested, $.varcategory, IntegerType, true, Some(UTC))";
  public static final String ICEBERG_VARCAT = "variant_get(nested, '$.varcategory', 'int')";
  public static final String SPARK_ISNOTNULL_NESTED = "isnotnull(nested)";
  public static final String ICEBERG_NESTED_ISNOTNULL = "nested IS NOT NULL";

  @Parameters(
      name =
          "catalogName = {0}, implementation = {1}, config = {2}, planningMode = {3} shredded= {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      // unshredded: the reference
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        LOCAL,
        false
      },
      // local planning and shredded
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        LOCAL,
        true
      },
      // distributed planning and shredded
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties(),
        DISTRIBUTED,
        true
      },
    };
  }

  public static final Logger LOG = LoggerFactory.getLogger(TestSparkVariantFilterPushDown.class);

  @Parameter(index = 3)
  private PlanningMode planningMode;

  /** Should the variant be shredded? */
  @Parameter(index = 4)
  private boolean shredded;

  /** Number of categories; each row has {@code category = id}. */
  private static final int NUM_CATEGORIES = 20;

  public TestSparkVariantFilterPushDown() {}

  @BeforeEach
  public void createTable() {
    LOG.info("Creating Spark Table with shredding {}", shredded);
    if (shredded) {
      spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
    }
    sql(
        """
        CREATE TABLE %s (id BIGINT, category INT, nested VARIANT, arr VARIANT)
          USING iceberg
          TBLPROPERTIES ('format-version'='3',
           'read.parquet.vectorization.enabled'='false',
           'write.parquet.shred-variants'='%s')""",
        selectTarget(), shredded);
    configurePlanningMode(planningMode);
    buildDataset();
  }

  @AfterEach
  public void dropTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  /**
   * Build a dataset in sql, using parse_json to create the variant data. Records are {@code (id,
   * category, parse_json(nested_json), parse_json(arr_json))}
   */
  private void buildDataset() {

    String values =
        IntStream.range(0, NUM_CATEGORIES)
            .mapToObj(
                n ->
                    String.format(
                        "(%d, %d, parse_json('{\"varid\": %d, \"varcategory\": %d}'), parse_json('[%d]'))",
                        (long) n, n, n, n, n))
            .collect(Collectors.joining(", "));

    sql("INSERT INTO %s VALUES %s", selectTarget(), values);
  }

  /**
   * Baseline: filter on a plain INT column, project the id. Iceberg pushes the predicate fully to
   * the scan; Spark still evaluates the predicate post-scan as a safety check.
   */
  @TestTemplate
  public void filterCategoryProjectId() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                "category = 5",
                "isnotnull(category) AND (category = 5)",
                "category IS NOT NULL, category = 5",
                0,
                0,
                ImmutableList.of(row(5L))));
  }

  /**
   * Filter using variant field extraction (equality). Iceberg pushes both a null check and the
   * equality predicate on the variant column; Spark also evaluates the filter post-scan.
   */
  @TestTemplate
  public void filterVariantCategoryProjectId() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " = 5",
                SPARK_ISNOTNULL_NESTED + " AND (" + SPARK_VARIANT_GET + " = 5)",
                ICEBERG_NESTED_ISNOTNULL + ", " + ICEBERG_VARCAT + " = 5",
                2,
                2,
                ImmutableList.of(row(5L))));
  }

  /** Use the greater than and less than predicates in a query. Doubles the number of scans. */
  @TestTemplate
  public void filterVariantCategoryInRange() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " > 4 AND " + ICEBERG_VARCAT + " < 7",
                "("
                    + SPARK_ISNOTNULL_NESTED
                    + " AND ("
                    + SPARK_VARIANT_GET
                    + " > 4))"
                    + " AND ("
                    + SPARK_VARIANT_GET
                    + " < 7)",
                ICEBERG_NESTED_ISNOTNULL
                    + ", "
                    + ICEBERG_VARCAT
                    + " > 4, "
                    + ICEBERG_VARCAT
                    + " < 7",
                4,
                4,
                ImmutableList.of(row(5L), row(6L))));
  }

  /** Use the greater than and less than predicates in a query. Doubles the number of scans. */
  @TestTemplate
  public void filterVariantCategoryGreateThanEquals() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " >= 4 AND " + ICEBERG_VARCAT + " <= 7",
                "("
                    + SPARK_ISNOTNULL_NESTED
                    + " AND ("
                    + SPARK_VARIANT_GET
                    + " >= 4))"
                    + " AND ("
                    + SPARK_VARIANT_GET
                    + " <= 7)",
                ICEBERG_NESTED_ISNOTNULL
                    + ", "
                    + ICEBERG_VARCAT
                    + " >= 4, "
                    + ICEBERG_VARCAT
                    + " <= 7",
                4,
                4,
                ImmutableList.of(row(4L), row(5L), row(6L), row(7L))));
  }

  /**
   * Project a variant field and filter on a different variant field. Iceberg pushes both a null
   * check and the equality predicate; the projection is evaluated post-scan.
   */
  @TestTemplate
  public void filterVariantCategoryProjectVariantId() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "variant_get(nested, '$.varid', 'int')",
                ICEBERG_VARCAT + " = 5",
                SPARK_ISNOTNULL_NESTED + " AND (" + SPARK_VARIANT_GET + " = 5)",
                ICEBERG_NESTED_ISNOTNULL + ", " + ICEBERG_VARCAT + " = 5",
                2,
                2,
                ImmutableList.of(row(5))));
  }

  /** IN predicate on a variant field using {@code variant_get}. */
  @TestTemplate
  public void filterVariantCategorySetMembership() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IN (5, 10)",
                SPARK_VARIANT_GET + " IN (5,10)",
                ICEBERG_VARCAT + " IN (5, 10)", // no null check
                4,
                4,
                ImmutableList.of(row(5L), row(10L))));
  }

  /**
   * Set members are all above or below the categories. The filter string this produces is slightly
   * different from that of {@link #filterVariantCategorySetMembership()}.
   */
  @TestTemplate
  public void filterVariantCategorySetMembership2() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IN (100, 400)",
                SPARK_VARIANT_GET + " IN (100,400)",
                "",
                0,
                0,
                ImmutableList.of()));
  }

  /**
   * Set membership of a single element is remapped to equality, filtering takes place in planning.
   */
  @TestTemplate
  public void filterVariantCategorySetMembership3() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IN (100)",
                SPARK_ISNOTNULL_NESTED + " AND (" + SPARK_VARIANT_GET + " = 100)",
                "",
                0,
                0,
                ImmutableList.of()));
  }

  /**
   * Set membership of a single element is remapped to equality, and if that element is in range, a
   * pushed down predicate is evaluated.
   */
  @TestTemplate
  public void filterVariantCategorySetMembership4() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IN (4)",
                SPARK_ISNOTNULL_NESTED + " AND (" + SPARK_VARIANT_GET + " = 4)",
                ICEBERG_NESTED_ISNOTNULL + ", " + ICEBERG_VARCAT + " = 4",
                2,
                2,
                ImmutableList.of(row(4L))));
  }

  /** Evaluation of the IS NULL predicate. */
  @TestTemplate
  public void filterVariantCategoryIsNull() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IS NULL",
                "isnull(" + SPARK_VARIANT_GET + ")",
                "",
                4,
                4,
                ImmutableList.of()));
  }

  /** Evaluation of the IS NOT NULL predicate; this finds everything. */
  @TestTemplate
  public void filterVariantCategoryIsNotNull() {
    List<Object[]> rows = LongStream.rangeClosed(0, 19).mapToObj(this::row).toList();
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                ICEBERG_VARCAT + " IS NOT NULL",
                SPARK_ISNOTNULL_NESTED + " AND isnotnull(" + SPARK_VARIANT_GET + ")",
                "nested IS NOT NULL, variant_get(nested, '$.varcategory', 'int') IS NOT NULL",
                4,
                4,
                rows));
  }

  /**
   * Filter on element 0 of an array variant. Iceberg pushes a null check on the array column; the
   * actual element comparison is done post-scan.
   */
  @TestTemplate
  public void filterArrayElementProjectId() {
    withDefaultTimeZone(
        "UTC",
        () ->
            checkFilters(
                "id",
                "variant_get(arr, '$[0]', 'int') = 5",
                "isnotnull(arr) AND (variant_get(arr, $[0], IntegerType, true, Some(UTC)) = 5)",
                "arr IS NOT NULL",
                0,
                0,
                ImmutableList.of(row(5L))));
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Run {@code SELECT <projection> FROM <table> WHERE <predicate> ORDER BY id}, assert the returned
   * rows, and verify that the physical plan contains the expected Spark post-scan filter and
   * Iceberg scan filters.
   *
   * @param projection column expression(s) to select
   * @param predicate SQL WHERE clause (no "WHERE" keyword)
   * @param sparkFilter expected post-scan Spark Filter node text
   * @param icebergFilters expected {@code filters=...} value from the Iceberg scan node; empty
   *     string means no Iceberg pushdown shall take places.
   * @param expectedPlanningEvaluations expected number of evaluations during planning
   * @param expectedExecutionEvaluations number of evaluations of a rowgroup filter predicate on
   *     shredded column.
   * @param expectedRows expected result rows in id order
   */
  private void checkFilters(
      String projection,
      String predicate,
      String sparkFilter,
      String icebergFilters,
      int expectedPlanningEvaluations,
      int expectedExecutionEvaluations,
      List<Object[]> expectedRows) {

    ParquetMetricsRowGroupFilter.resetShreddedMetricsCounters();
    String query =
        String.format("SELECT %s FROM %s WHERE %s ORDER BY id", projection, tableName, predicate);

    SparkPlan plan = executeAndKeepPlan(query);
    long planShredCount = ParquetMetricsRowGroupFilter.variantPredicatesShreddedMetricsEvaluated();
    String planString = plan.toString().replaceAll("#\\d+L?", "");
    String summary = String.format("%s with plan shred count %d", query, planShredCount);

    assertThat(planString)
        .as("Post-scan Spark filter of %s", summary)
        .containsAnyOf("Filter (" + sparkFilter + ")", "Filter " + sparkFilter);

    if (!icebergFilters.isEmpty()) {
      assertThat(planString)
          .as("No iceberg scan generated from %s", summary)
          .contains("IcebergScan");
      assertThat(planString)
          .as("Iceberg pushed filters of must match from %s", summary)
          .contains(", filters=" + icebergFilters + ",");
    } else {
      assertThat(planString)
          .as("No iceberg scan generated from %s", summary)
          .doesNotContain("IcebergScan");
    }

    if (shredded) {
      assertThat(ParquetMetricsRowGroupFilter.variantPredicatesShreddedMetricsEvaluated())
          .describedAs(
              "Count of shredded metrics filtered during planning of of %s to plan %s",
              summary, planString)
          .isEqualTo(expectedPlanningEvaluations);
    }
    ParquetMetricsRowGroupFilter.resetShreddedMetricsCounters();
    final List<Object[]> rows =
        sql("SELECT %s FROM %s WHERE %s ORDER BY id", projection, selectTarget(), predicate);
    assertEquals("Execution of " + summary + " to plan " + planString, expectedRows, rows);
    if (shredded) {
      assertThat(ParquetMetricsRowGroupFilter.variantPredicatesShreddedMetricsEvaluated())
          .describedAs(
              "Count of shredded metrics filtered during execution of of %s to plan %s",
              summary, planString)
          .isEqualTo(expectedExecutionEvaluations);
    }
  }
}
