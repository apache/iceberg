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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.VariantType;
import org.apache.spark.sql.types.VariantType$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Tests that {@code SparkScanBuilder} implements {@code SupportsPushDownVariantExtractions} so
 * Spark's {@code V2ScanRelationPushDown} rule rewrites {@code variant_get} expressions into struct
 * field accesses and prunes the scan output schema to an annotated struct rather than the full
 * {@code VariantType}.
 *
 * <p>These tests cover the DSv2 contract (plan shape + query correctness). Actual Parquet I/O
 * column pruning is a follow-on change.
 *
 * <p>Each test sets {@link SparkSQLProperties#VARIANT_EXTRACTION_PUSH_DOWN_ENABLED} explicitly so
 * behavior does not depend on the product default.
 */
public class TestVariantShreddingPushdown extends CatalogTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        SparkCatalogConfig.HADOOP.properties()
      },
    };
  }

  @BeforeEach
  public void createTable() {
    // Use a schema with a non-variant column + a variant column, matching the GHA table shape
    // (type STRING, payload VARIANT) to catch any plan-shape issue with multi-column tables.
    sql(
        "CREATE TABLE %s (id INT, type STRING, v VARIANT) USING iceberg TBLPROPERTIES ('%s' = '3')",
        tableName, TableProperties.FORMAT_VERSION);
    spark.conf().set(SparkSQLProperties.SHRED_VARIANTS, "true");
  }

  private void withExtractionPushdown(boolean enabled, Runnable action) {
    spark
        .conf()
        .set(SparkSQLProperties.VARIANT_EXTRACTION_PUSH_DOWN_ENABLED, String.valueOf(enabled));
    try {
      action.run();
    } finally {
      spark.conf().unset(SparkSQLProperties.VARIANT_EXTRACTION_PUSH_DOWN_ENABLED);
    }
  }

  private void withVariantPushIntoScan(Runnable action) {
    spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "true");
    try {
      action.run();
    } finally {
      spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
    }
  }

  @AfterEach
  public void dropTable() {
    spark.conf().unset(SparkSQLProperties.SHRED_VARIANTS);
    spark.conf().unset(SparkSQLProperties.VARIANT_EXTRACTION_PUSH_DOWN_ENABLED);
    spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private List<StructType> scanReadSchemas(LogicalPlan optimized) {
    return scala.collection.JavaConverters.seqAsJavaListConverter(optimized.collectLeaves())
        .asJava()
        .stream()
        .filter(node -> node instanceof DataSourceV2ScanRelation)
        .map(node -> ((DataSourceV2ScanRelation) node).scan().readSchema())
        .collect(Collectors.toList());
  }

  private StructField variantField(StructType scanSchema, String colName) {
    return scanSchema.apply(colName);
  }

  private void assertVariantColumnIsAnnotatedStruct(
      StructType scanSchema, String colName, int expectedOrdinalFields) {
    StructField field = variantField(scanSchema, colName);
    assertThat(field.dataType())
        .as("%s should be rewritten to an annotated struct, not VariantType", colName)
        .isInstanceOf(StructType.class)
        .isNotInstanceOf(VariantType.class);

    StructType vStruct = (StructType) field.dataType();
    assertThat(vStruct.fields())
        .as("struct for %s should have %s ordinal field(s)", colName, expectedOrdinalFields)
        .hasSize(expectedOrdinalFields);
  }

  @TestTemplate
  public void testVariantExtractionPlanShape() {
    sql(
        "INSERT INTO %s VALUES (1, 'A', parse_json('{\"city\": \"Austin\", \"zip\": 78701}')), "
            + "(2, 'B', parse_json('{\"city\": \"Boston\", \"zip\": 2108}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(v, '$.city', 'string') FROM %s", tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());

                  assertThat(scanSchemas).hasSize(1);
                  assertVariantColumnIsAnnotatedStruct(scanSchemas.get(0), "v", 1);

                  StructType vStruct = (StructType) scanSchemas.get(0).apply("v").dataType();
                  StructField extracted = vStruct.fields()[0];
                  assertThat(extracted.name()).as("ordinal name must be '0'").isEqualTo("0");
                  assertThat(extracted.dataType())
                      .as("extracted type must be StringType")
                      .isEqualTo(DataTypes.StringType);
                }));
  }

  @TestTemplate
  public void testFilterPlanShape() {
    sql(
        "INSERT INTO %s VALUES (1, 'PushEvent', parse_json('{\"size\": 10}')), "
            + "(2, 'PushEvent', parse_json('{\"size\": 3}')), "
            + "(3, 'IssueEvent', parse_json('{\"size\": 8}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT count(*) AS n FROM %s WHERE type = 'PushEvent'"
                                  + " AND variant_get(v, '$.size', 'long') > 5",
                              tableName));
                  LogicalPlan optimized = df.queryExecution().optimizedPlan();

                  assertThat(optimized).isNotNull();
                  assertThat(optimized.toString())
                      .as("optimized plan should not retain variant_get after pushdown")
                      .doesNotContain("variant_get");

                  List<StructType> scanSchemas = scanReadSchemas(optimized);
                  assertThat(scanSchemas).hasSize(1);
                  assertVariantColumnIsAnnotatedStruct(scanSchemas.get(0), "v", 1);

                  StructType vStruct = (StructType) scanSchemas.get(0).apply("v").dataType();
                  assertThat(vStruct.fields()[0].name()).isEqualTo("0");
                  assertThat(vStruct.fields()[0].dataType()).isEqualTo(DataTypes.LongType);
                }));
  }

  @TestTemplate
  public void testPushdownDisabledWhenConfigOff() {
    withExtractionPushdown(
        false,
        () -> {
          sql("INSERT INTO %s VALUES (1, 'A', parse_json('{\"city\": \"Austin\"}'))", tableName);

          Dataset<Row> df =
              spark.sql(
                  String.format("SELECT variant_get(v, '$.city', 'string') FROM %s", tableName));
          List<StructType> scanSchemas = scanReadSchemas(df.queryExecution().optimizedPlan());

          assertThat(scanSchemas).hasSize(1);
          assertThat(variantField(scanSchemas.get(0), "v").dataType())
              .as("v stays VariantType when iceberg extraction pushdown is disabled")
              .isEqualTo(VariantType$.MODULE$);
        });
  }

  @TestTemplate
  public void testIsNotNullOnlyFallsBackWithoutError() {
    sql(
        "INSERT INTO %s VALUES (1, 'A', parse_json('{\"city\": \"Austin\"}')), " + "(2, 'B', null)",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format("SELECT count(*) FROM %s WHERE v IS NOT NULL", tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());

                  assertThat(scanSchemas).hasSize(1);
                  assertThat(scanSchemas.get(0).apply("v").dataType())
                      .as("IsNotNull-only queries fall back to full variant reads for now")
                      .isInstanceOf(VariantType.class);
                  assertThat(df.collectAsList().get(0).getLong(0)).isEqualTo(1L);
                }));
  }

  @TestTemplate
  public void testVariantExtractionCorrectResults() {
    sql(
        "INSERT INTO %s VALUES (1, 'X', parse_json('{\"city\": \"Austin\"}')), "
            + "(2, 'X', parse_json('{\"city\": \"Boston\"}')), "
            + "(3, 'Y', null)",
        tableName);

    // Execute without variant extraction pushdown so the Parquet reader returns full VariantVal.
    withExtractionPushdown(
        false,
        () -> {
          List<Row> results;
          spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "false");
          try {
            results =
                spark
                    .sql(
                        String.format(
                            "SELECT id, variant_get(v, '$.city', 'string') AS city FROM %s ORDER BY id",
                            tableName))
                    .collectAsList();
          } finally {
            spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
          }

          assertThat(results).hasSize(3);
          assertThat(results.get(0).getString(1)).isEqualTo("Austin");
          assertThat(results.get(1).getString(1)).isEqualTo("Boston");
          assertThat(results.get(2).isNullAt(1)).isTrue();
        });
  }

  @TestTemplate
  public void testVariantExtractionWithFilterCorrectResults() {
    sql(
        "INSERT INTO %s VALUES (1, 'X', parse_json('{\"size\": 10}')), "
            + "(2, 'X', parse_json('{\"size\": 3}')), "
            + "(3, 'X', parse_json('{\"size\": 7}'))",
        tableName);

    // Execute without variant extraction pushdown. Rows with size > 5: id=1 (size=10) and id=3
    // (size=7).
    withExtractionPushdown(
        false,
        () -> {
          List<Row> results;
          spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "false");
          try {
            results =
                spark
                    .sql(
                        String.format(
                            "SELECT id FROM %s WHERE variant_get(v, '$.size', 'int') > 5 ORDER BY id",
                            tableName))
                    .collectAsList();
          } finally {
            spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
          }

          assertThat(results).hasSize(2);
          assertThat(results.get(0).getInt(0)).isEqualTo(1);
          assertThat(results.get(1).getInt(0)).isEqualTo(3);
        });
  }

  @TestTemplate
  public void testMultipleExtractionsFromSameColumnPlanShape() {
    sql(
        "INSERT INTO %s VALUES (1, 'A', parse_json('{\"city\": \"Austin\", \"zip\": \"78701\"}')), "
            + "(2, 'B', parse_json('{\"city\": \"Boston\", \"zip\": \"02108\"}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(v, '$.city', 'string') AS city, "
                                  + "variant_get(v, '$.zip', 'string') AS zip FROM %s ORDER BY city",
                              tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());

                  assertThat(scanSchemas).hasSize(1);
                  StructType vStruct = (StructType) scanSchemas.get(0).apply("v").dataType();

                  assertThat(vStruct.fields())
                      .as("struct should have two ordinal fields for two extractions")
                      .hasSize(2);
                  assertThat(vStruct.fields()[0].name()).isEqualTo("0");
                  assertThat(vStruct.fields()[0].dataType()).isEqualTo(DataTypes.StringType);
                  assertThat(vStruct.fields()[1].name()).isEqualTo("1");
                  assertThat(vStruct.fields()[1].dataType()).isEqualTo(DataTypes.StringType);
                }));
  }

  @TestTemplate
  public void testAggregateWithSamePathTwoTypesIsCorrect() {
    // c-q05 pattern: avg(size as double) and max(size as long) in the same query.
    // Spark's PhysicalOperation does not collect variant_get expressions from inside aggregates,
    // so the scan falls back to full variant evaluation — but the result must still be correct.
    sql(
        "INSERT INTO %s VALUES (1, 'PushEvent', parse_json('{\"size\": 10}')), "
            + "(2, 'PushEvent', parse_json('{\"size\": 3}')), "
            + "(3, 'IssueEvent', parse_json('{\"size\": 8}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Row row =
                      spark
                          .sql(
                              String.format(
                                  "SELECT round(avg(variant_get(v, '$.size', 'double')), 6) AS avg_size, "
                                      + "max(variant_get(v, '$.size', 'long')) AS max_size "
                                      + "FROM %s WHERE type = 'PushEvent' "
                                      + "AND variant_get(v, '$.size', 'long') IS NOT NULL",
                                  tableName))
                          .collectAsList()
                          .get(0);
                  assertThat(row.getDouble(0)).isEqualTo(6.5D);
                  assertThat(row.getLong(1)).isEqualTo(10L);
                }));
  }

  /**
   * GHA query shape: table with multiple variant columns (actor, repo, payload), query touches only
   * one column. Spark only sends extractions for {@code payload} — {@code actor} and {@code repo}
   * are never in {@code variants.mapping} — so only {@code payload} gets rewritten to a struct.
   *
   * <p>Plan-shape only (no collect): correctness requires the Parquet extraction reader from the
   * {@code variant-extraction-parquet-io} branch; verified on the integration branch.
   */
  @TestTemplate
  public void testMultipleVariantColumnsSingleColumnQueryPushesDown() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, type STRING, actor VARIANT, repo VARIANT, payload VARIANT)"
            + " USING iceberg TBLPROPERTIES ('%s' = '3')",
        tableName, TableProperties.FORMAT_VERSION);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'PushEvent', parse_json('{\"login\": \"alice\"}'),"
            + " parse_json('{\"name\": \"repo1\"}'), parse_json('{\"size\": 10}')), "
            + "(2, 'PushEvent', parse_json('{\"login\": \"bob\"}'),"
            + " parse_json('{\"name\": \"repo2\"}'), parse_json('{\"size\": 3}')), "
            + "(3, 'IssueEvent', parse_json('{\"login\": \"carol\"}'),"
            + " parse_json('{\"name\": \"repo3\"}'), parse_json('{\"size\": 8}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT count(*) AS n FROM %s WHERE type = 'PushEvent'"
                                  + " AND variant_get(payload, '$.size', 'long') > 5",
                              tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());

                  assertThat(scanSchemas).hasSize(1);
                  StructType scanSchema = scanSchemas.get(0);

                  assertVariantColumnIsAnnotatedStruct(scanSchema, "payload", 1);
                  assertThat(
                          ((StructType) scanSchema.apply("payload").dataType())
                              .fields()[0].dataType())
                      .as("payload extraction slot must be LongType")
                      .isEqualTo(DataTypes.LongType);

                  // actor and repo are not referenced in this query — they must stay VariantType.
                  assertThat(variantField(scanSchema, "actor").dataType())
                      .as("actor stays VariantType when not referenced")
                      .isEqualTo(VariantType$.MODULE$);
                  assertThat(variantField(scanSchema, "repo").dataType())
                      .as("repo stays VariantType when not referenced")
                      .isEqualTo(VariantType$.MODULE$);
                }));
  }

  /**
   * Two variant columns referenced in the same query (SELECT + WHERE). Spark sends extractions for
   * both; the all-or-nothing policy accepts both because all paths are supported, and both columns
   * are rewritten to annotated structs.
   *
   * <p>Plan-shape only (no collect): correctness requires the Parquet extraction reader from the
   * {@code variant-extraction-parquet-io} branch; verified on the integration branch.
   */
  @TestTemplate
  public void testMultipleVariantColumnsTwoColumnQueryPushesDown() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, type STRING, actor VARIANT, repo VARIANT, payload VARIANT)"
            + " USING iceberg TBLPROPERTIES ('%s' = '3')",
        tableName, TableProperties.FORMAT_VERSION);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'PushEvent', parse_json('{\"login\": \"alice\"}'),"
            + " parse_json('{\"name\": \"repo1\"}'), parse_json('{\"size\": 10}')), "
            + "(2, 'PushEvent', parse_json('{\"login\": \"bob\"}'),"
            + " parse_json('{\"name\": \"repo2\"}'), parse_json('{\"size\": 3}')), "
            + "(3, 'IssueEvent', parse_json('{\"login\": \"carol\"}'),"
            + " parse_json('{\"name\": \"repo3\"}'), parse_json('{\"size\": 8}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  // Query references both repo and payload in the same Project/Filter visible to
                  // the scan — Spark sends both sets of extractions in one batch.
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(repo, '$.name', 'string') AS rn,"
                                  + " variant_get(payload, '$.size', 'long') AS sz"
                                  + " FROM %s WHERE type = 'PushEvent'",
                              tableName));

                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());
                  assertThat(scanSchemas).hasSize(1);
                  StructType scanSchema = scanSchemas.get(0);

                  // Both referenced columns must be rewritten to annotated structs.
                  assertVariantColumnIsAnnotatedStruct(scanSchema, "repo", 1);
                  assertVariantColumnIsAnnotatedStruct(scanSchema, "payload", 1);

                  // actor is not referenced — must stay VariantType.
                  assertThat(variantField(scanSchema, "actor").dataType())
                      .as("actor stays VariantType when not referenced")
                      .isEqualTo(VariantType$.MODULE$);
                }));
  }

  @TestTemplate
  public void testVariantExtractionWithPushdownCorrectResults() {
    sql(
        "INSERT INTO %s VALUES (1, 'X', parse_json('{\"city\": \"Austin\"}')), "
            + "(2, 'X', parse_json('{\"city\": \"Boston\"}')), "
            + "(3, 'Y', null)",
        tableName);

    withExtractionPushdown(
        true,
        () -> {
          // Plan-shape: v must be rewritten to an annotated struct.
          withVariantPushIntoScan(
              () -> {
                Dataset<Row> df =
                    spark.sql(
                        String.format(
                            "SELECT id, variant_get(v, '$.city', 'string') AS city FROM %s ORDER BY id",
                            tableName));
                List<StructType> scanSchemas = scanReadSchemas(df.queryExecution().optimizedPlan());
                assertThat(scanSchemas).hasSize(1);
                assertVariantColumnIsAnnotatedStruct(scanSchemas.get(0), "v", 1);
              });

          // Correctness: disable Spark-level variant pushdown so collect() works on this branch
          // without the Parquet extraction reader. End-to-end correctness with pushdown active is
          // verified on the integration branch.
          spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "false");
          try {
            List<Row> results =
                spark
                    .sql(
                        String.format(
                            "SELECT id, variant_get(v, '$.city', 'string') AS city FROM %s ORDER BY id",
                            tableName))
                    .collectAsList();
            assertThat(results).hasSize(3);
            assertThat(results.get(0).getString(1)).isEqualTo("Austin");
            assertThat(results.get(1).getString(1)).isEqualTo("Boston");
            assertThat(results.get(2).isNullAt(1)).isTrue();
          } finally {
            spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
          }
        });
  }

  @TestTemplate
  public void testArrayPathExtractionViaFieldLevelFallback() {
    // Insert rows where the variant column contains a "commits" array. Shredding writes
    // v.typed_value.commits with a serialized value column. The extraction pushdown reads
    // that smaller blob and navigates [0].author.name in-memory rather than reading the full root.
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'PushEvent', parse_json('{\"commits\": [{\"author\": {\"name\": \"Alice\"}},"
            + " {\"author\": {\"name\": \"Bob\"}}]}')), "
            + "(2, 'PushEvent', parse_json('{\"commits\": []}')), "
            + "(3, 'IssueEvent', parse_json('{\"commits\": null}'))",
        tableName);

    withExtractionPushdown(
        true,
        () -> {
          // Plan-shape: v must be rewritten to an annotated struct (array-path extraction).
          withVariantPushIntoScan(
              () -> {
                Dataset<Row> df =
                    spark.sql(
                        String.format(
                            "SELECT id, variant_get(v, '$.commits[0].author.name', 'string') AS first_author"
                                + " FROM %s ORDER BY id",
                            tableName));
                List<StructType> scanSchemas = scanReadSchemas(df.queryExecution().optimizedPlan());
                assertThat(scanSchemas).hasSize(1);
                assertVariantColumnIsAnnotatedStruct(scanSchemas.get(0), "v", 1);
              });

          // Correctness: disable Spark-level variant pushdown so collect() works on this branch
          // without the Parquet extraction reader.
          spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "false");
          try {
            List<Row> results =
                spark
                    .sql(
                        String.format(
                            "SELECT id, variant_get(v, '$.commits[0].author.name', 'string') AS first_author"
                                + " FROM %s ORDER BY id",
                            tableName))
                    .collectAsList();
            assertThat(results).hasSize(3);
            assertThat(results.get(0).getString(1)).isEqualTo("Alice");
            assertThat(results.get(1).isNullAt(1)).isTrue();
            assertThat(results.get(2).isNullAt(1)).isTrue();
          } finally {
            spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
          }
        });
  }

  @TestTemplate
  public void testSamePathDifferentTypesAcceptedInProjectPushdown() {
    // When the same path appears with two different target types in the SELECT list (not
    // inside an aggregate), both appear within PhysicalOperation and get pushed to the scan.
    // The scan builder must accept both; the Parquet reader shares one physical column.
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'PushEvent', parse_json('{\"size\": 10}')), "
            + "(2, 'PushEvent', parse_json('{\"size\": 3}')), "
            + "(3, 'IssueEvent', parse_json('{\"size\": 8}'))",
        tableName);

    withExtractionPushdown(
        true,
        () -> {
          // Plan-shape: both extractions must be accepted; v becomes a struct with 2 typed fields.
          withVariantPushIntoScan(
              () -> {
                Dataset<Row> df =
                    spark.sql(
                        String.format(
                            "SELECT"
                                + " variant_get(v, '$.size', 'double') AS size_d,"
                                + " variant_get(v, '$.size', 'long') AS size_l"
                                + " FROM %s"
                                + " WHERE variant_get(v, '$.size', 'long') IS NOT NULL",
                            tableName));

                List<StructType> scanSchemas = scanReadSchemas(df.queryExecution().optimizedPlan());
                assertThat(scanSchemas).hasSize(1);
                StructType vStruct = (StructType) scanSchemas.get(0).apply("v").dataType();
                assertThat(vStruct.fields())
                    .as("same path with two types must produce two struct slots")
                    .hasSize(2);
                assertThat(vStruct.fields()[0].dataType()).isEqualTo(DataTypes.DoubleType);
                assertThat(vStruct.fields()[1].dataType()).isEqualTo(DataTypes.LongType);
              });

          // Correctness: disable Spark-level variant pushdown so collect() works on this branch
          // without the Parquet extraction reader.
          spark.conf().set(SQLConf.PUSH_VARIANT_INTO_SCAN().key(), "false");
          try {
            List<Row> results =
                spark
                    .sql(
                        String.format(
                            "SELECT"
                                + " variant_get(v, '$.size', 'double') AS size_d,"
                                + " variant_get(v, '$.size', 'long') AS size_l"
                                + " FROM %s"
                                + " WHERE variant_get(v, '$.size', 'long') IS NOT NULL"
                                + " ORDER BY size_l",
                            tableName))
                    .collectAsList();
            assertThat(results).hasSize(3);
            // All three rows, ordered by size_l: 3, 8, 10
            assertThat(results.get(0).getDouble(0)).isEqualTo(3.0D);
            assertThat(results.get(0).getLong(1)).isEqualTo(3L);
            assertThat(results.get(2).getDouble(0)).isEqualTo(10.0D);
            assertThat(results.get(2).getLong(1)).isEqualTo(10L);
          } finally {
            spark.conf().unset(SQLConf.PUSH_VARIANT_INTO_SCAN().key());
          }
        });
  }

  @TestTemplate
  public void testDeclineUnsupportedStructTarget() {
    sql(
        "INSERT INTO %s VALUES (1, 'X', parse_json('{\"size\": 10}')), "
            + "(2, 'X', parse_json('{\"size\": 3}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(v, '$', 'struct<size: int>') AS s FROM %s",
                              tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());
                  assertThat(scanSchemas).hasSize(1);
                  assertThat(variantField(scanSchemas.get(0), "v").dataType())
                      .isInstanceOf(VariantType.class);
                }));
  }

  @TestTemplate
  public void testDeclineUnsupportedArrayTarget() {
    sql("INSERT INTO %s VALUES (1, 'X', parse_json('{\"tags\": [\"a\", \"b\"]}'))", tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(v, '$.tags', 'array<string>') AS tags FROM %s",
                              tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());
                  assertThat(scanSchemas).hasSize(1);
                  assertThat(variantField(scanSchemas.get(0), "v").dataType())
                      .isInstanceOf(VariantType.class);
                }));
  }

  @TestTemplate
  public void testDateExtractionPlanShape() {
    sql(
        "INSERT INTO %s VALUES (1, 'X', parse_json('{\"created\": \"1970-03-01\"}')), "
            + "(2, 'X', parse_json('{\"created\": \"1970-01-01\"}'))",
        tableName);

    withExtractionPushdown(
        true,
        () ->
            withVariantPushIntoScan(
                () -> {
                  Dataset<Row> df =
                      spark.sql(
                          String.format(
                              "SELECT variant_get(v, '$.created', 'date') AS created FROM %s"
                                  + " WHERE variant_get(v, '$.created', 'date') IS NOT NULL",
                              tableName));
                  List<StructType> scanSchemas =
                      scanReadSchemas(df.queryExecution().optimizedPlan());
                  assertThat(scanSchemas).hasSize(1);
                  assertVariantColumnIsAnnotatedStruct(scanSchemas.get(0), "v", 1);
                }));
  }

  @TestTemplate
  public void testFallbackForNonVariantColumn() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql(
        "CREATE TABLE %s (id INT, x INT) USING iceberg TBLPROPERTIES ('%s' = '3')",
        tableName, TableProperties.FORMAT_VERSION);
    sql("INSERT INTO %s VALUES (1, 42), (2, 99)", tableName);

    List<Row> results =
        spark.sql(String.format("SELECT id, x FROM %s ORDER BY id", tableName)).collectAsList();

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getInt(1)).isEqualTo(42);
    assertThat(results.get(1).getInt(1)).isEqualTo(99);
  }
}
