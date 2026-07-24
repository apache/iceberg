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

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestReplaceScopedDataDataFrame extends SparkRowLevelOperationsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS parquet_replace_scoped_data");
  }

  @TestTemplate
  public void testScopedReplace() throws Exception {
    createAndInitTable(
        "id INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"it\" }\n"
            + "{ \"id\": 4, \"dep\": \"sales\" }");

    source(
            "id INT, dep STRING",
            "{ \"id\": 10, \"dep\": \"hr\" }\n{ \"id\": 11, \"dep\": \"sales\" }")
        .writeTo(commitTarget())
        .option("replace-using", "dep")
        .overwrite(lit(true));

    assertEquals(
        "Should replace rows with matching scope values and retain the rest",
        Arrays.asList(row(3, "it"), row(10, "hr"), row(11, "sales")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testDataFrameColumnsAreAlignedByName() throws Exception {
    createAndInitTable(
        "id INT, dep STRING, note STRING",
        "{ \"id\": 1, \"dep\": \"hr\", \"note\": \"old\" }\n"
            + "{ \"id\": 2, \"dep\": \"it\", \"note\": \"keep\" }");

    Dataset<Row> reorderedSource = spark.sql("SELECT 'new' AS note, 'hr' AS dep, 10 AS id");
    reorderedSource.writeTo(commitTarget()).option("replace-using", "dep").overwrite(lit(true));

    assertEquals(
        "Should preserve Spark DataFrameWriterV2 by-name column alignment",
        Arrays.asList(row(2, "it", "keep"), row(10, "hr", "new")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testNormalWriteOptionsArePreserved() throws Exception {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
        .writeTo(commitTarget())
        .option("Replace-Using", "dep")
        .option(SparkWriteOptions.SNAPSHOT_PROPERTY_PREFIX + ".df-scoped-replace", "passed")
        .overwrite(lit(true));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    assertThat(snapshot.summary()).containsEntry("df-scoped-replace", "passed");
  }

  @TestTemplate
  public void testBranchWriteOptionIsHonored() throws Exception {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");
    sql("ALTER TABLE %s CREATE BRANCH df_branch", tableName);

    source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
        .writeTo(tableName)
        .option(SparkWriteOptions.BRANCH, "df_branch")
        .option("replace-using", "dep")
        .overwrite(lit(true));

    assertEquals(
        "Main branch should be unchanged",
        ImmutableList.of(row(1, "hr")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
    assertEquals(
        "Configured write branch should receive the scoped replace",
        ImmutableList.of(row(10, "hr")),
        sql("SELECT * FROM %s VERSION AS OF 'df_branch' ORDER BY id", tableName));
  }

  @TestTemplate
  public void testDuplicateScopeColumnsAreAccepted() throws Exception {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
        .writeTo(commitTarget())
        .option("replace-using", "dep, dep")
        .overwrite(lit(true));

    assertEquals(
        "Duplicate scope columns should behave as redundant predicates",
        Arrays.asList(row(2, "it"), row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testEmptyScopeTokenIsRejected() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    assertThatThrownBy(
            () ->
                source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
                    .writeTo(commitTarget())
                    .option("replace-using", "dep,")
                    .overwrite(lit(true)))
        .isInstanceOf(ParseException.class)
        .hasMessageContaining("Syntax error");
  }

  @TestTemplate
  public void testNonTrueOverwritePredicateIsRejected() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    assertThatThrownBy(
            () ->
                source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
                    .writeTo(commitTarget())
                    .option("replace-using", "dep")
                    .overwrite(expr("id = 1")))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("overwrite(true)");
  }

  @TestTemplate
  public void testAppendWithReplaceUsingIsRejectedForIceberg() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    assertThatThrownBy(
            () ->
                source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
                    .writeTo(commitTarget())
                    .option("replace-using", "dep")
                    .append())
        .isInstanceOf(Exception.class)
        .hasMessageContaining("not append");
  }

  @TestTemplate
  public void testDynamicOverwriteWithReplaceUsingIsRejectedForIceberg() {
    createAndInitTable(
        "id INT, dep STRING", "PARTITIONED BY (dep)", "{ \"id\": 1, \"dep\": \"hr\" }");

    assertThatThrownBy(
            () ->
                source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
                    .writeTo(commitTarget())
                    .option("replace-using", "dep")
                    .overwritePartitions())
        .isInstanceOf(Exception.class)
        .hasMessageContaining("not overwritePartitions");
  }

  @TestTemplate
  public void testPlainOverwriteIsUnaffected() throws Exception {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    source("id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }")
        .writeTo(commitTarget())
        .overwrite(lit(true));

    assertEquals(
        "Plain overwrite should keep existing DataFrameWriterV2 semantics",
        ImmutableList.of(row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testNonIcebergAppendIsUnchangedByIcebergRule() throws Exception {
    sql("CREATE TABLE parquet_replace_scoped_data (id INT) USING parquet");
    sql("INSERT INTO parquet_replace_scoped_data VALUES (1)");

    assertThatThrownBy(
            () ->
                spark
                    .sql("SELECT 2 AS id")
                    .writeTo("parquet_replace_scoped_data")
                    .option("replace-using", "id")
                    .append())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot write into v1 table")
        .hasMessageNotContaining("not append");
  }

  @TestTemplate
  public void testNonIcebergOverwriteIsUnchangedByIcebergRule() throws Exception {
    sql("CREATE TABLE parquet_replace_scoped_data (id INT) USING parquet");
    sql("INSERT INTO parquet_replace_scoped_data VALUES (1)");

    // The rewrite is gated to Iceberg SparkTable targets, so a non-Iceberg overwrite carrying
    // replace-using must fall through to Spark's standard write path rather than being intercepted
    // and lowered into a scoped replace.
    assertThatThrownBy(
            () ->
                spark
                    .sql("SELECT 2 AS id")
                    .writeTo("parquet_replace_scoped_data")
                    .option("replace-using", "id")
                    .overwrite(lit(true)))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot write into v1 table")
        .hasMessageNotContaining("overwrite(true)");
  }

  private Dataset<Row> source(String schema, String jsonData) {
    List<String> jsonRows =
        Arrays.stream(jsonData.split("\n"))
            .filter(str -> !str.trim().isEmpty())
            .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, org.apache.spark.sql.Encoders.STRING());
    return spark.read().schema(schema).json(jsonDS);
  }
}
