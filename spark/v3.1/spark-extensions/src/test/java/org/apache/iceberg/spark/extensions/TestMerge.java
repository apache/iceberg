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

import static org.apache.iceberg.TableProperties.MERGE_CARDINALITY_CHECK_ENABLED;
import static org.apache.iceberg.TableProperties.MERGE_ISOLATION_LEVEL;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.SPLIT_SIZE;
import static org.apache.iceberg.TableProperties.WRITE_DISTRIBUTION_MODE;
import static org.apache.spark.sql.functions.lit;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMerge extends SparkRowLevelOperationsTestBase {

  public TestMerge(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode) {
    super(catalogName, implementation, config, fileFormat, vectorized, distributionMode);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  // TODO: add tests for multiple NOT MATCHED clauses when we move to Spark 3.1

  @Test
  public void testMergeIntoEmptyTargetInsertAllNonMatchingRows() {
    createAndInitTable("id INT, dep STRING");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // new
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeIntoEmptyTargetInsertOnlyMatchingRows() {
    createAndInitTable("id INT, dep STRING");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 3, \"dep\": \"emp-id-3\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND (s.id >=2) THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2"), // new
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithOnlyUpdateClause() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-six\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(6, "emp-id-six") // kept
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithOnlyDeleteClause() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-one") // kept
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithAllCauses() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithAllCausesWithExplicitColumnSpecification() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET t.id = s.id, t.dep = s.dep "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT (t.id, t.dep) VALUES (s.id, s.dep)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithSourceCTE() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-two\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-3\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 5, \"dep\": \"emp-id-6\" }");

    sql(
        "WITH cte1 AS (SELECT id + 1 AS id, dep FROM source) "
            + "MERGE INTO %s AS t USING cte1 AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 2 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 3 THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2"), // updated
            row(3, "emp-id-3") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithSourceFromSetOps() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String derivedSource =
        "SELECT * FROM source WHERE id = 2 "
            + "UNION ALL "
            + "SELECT * FROM source WHERE id = 1 OR id = 6";

    sql(
        "MERGE INTO %s AS t USING (%s) AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET * "
            + "WHEN MATCHED AND t.id = 6 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        tableName, derivedSource);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "emp-id-1"), // updated
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithMultipleUpdatesForTargetRow() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg = "a single row from the target table with multiple rows of the source table";
    AssertHelpers.assertThrows(
        "Should complain non iceberg target table",
        SparkException.class,
        errorMsg,
        () -> {
          sql(
              "MERGE INTO %s AS t USING source AS s "
                  + "ON t.id == s.id "
                  + "WHEN MATCHED AND t.id = 1 THEN "
                  + "  UPDATE SET * "
                  + "WHEN MATCHED AND t.id = 6 THEN "
                  + "  DELETE "
                  + "WHEN NOT MATCHED AND s.id = 2 THEN "
                  + "  INSERT *",
              tableName);
        });

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testMergeWithDisabledCardinalityCheck() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    try {
      // disable the cardinality check
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
          tableName, MERGE_CARDINALITY_CHECK_ENABLED, false);

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);
    } finally {
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%b')",
          tableName, MERGE_CARDINALITY_CHECK_ENABLED, true);
    }

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "emp-id-1"), row(1, "emp-id-1"), row(2, "emp-id-2")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testMergeWithUnconditionalDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND s.id = 2 THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(2, "emp-id-2") // new
            );
    assertEquals(
        "Should have expected rows", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithSingleConditionalDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
            + "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
            + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    String errorMsg = "a single row from the target table with multiple rows of the source table";
    AssertHelpers.assertThrows(
        "Should complain non iceberg target table",
        SparkException.class,
        errorMsg,
        () -> {
          sql(
              "MERGE INTO %s AS t USING source AS s "
                  + "ON t.id == s.id "
                  + "WHEN MATCHED AND t.id = 1 THEN "
                  + "  DELETE "
                  + "WHEN NOT MATCHED AND s.id = 2 THEN "
                  + "  INSERT *",
              tableName);
        });

    assertEquals(
        "Target should be unchanged",
        ImmutableList.of(row(1, "emp-id-one"), row(6, "emp-id-6")),
        sql("SELECT * FROM %s ORDER BY id ASC NULLS LAST", tableName));
  }

  @Test
  public void testMergeWithIdentityTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD identity(dep)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", tableName));

      removeTables();
    }
  }

  @Test
  public void testMergeWithDaysTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, ts TIMESTAMP");
      sql("ALTER TABLE %s ADD PARTITION FIELD days(ts)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "id INT, ts TIMESTAMP",
          "{ \"id\": 1, \"ts\": \"2000-01-01 00:00:00\" }\n"
              + "{ \"id\": 6, \"ts\": \"2000-01-06 00:00:00\" }");

      createOrReplaceView(
          "source",
          "id INT, ts TIMESTAMP",
          "{ \"id\": 2, \"ts\": \"2001-01-02 00:00:00\" }\n"
              + "{ \"id\": 1, \"ts\": \"2001-01-01 00:00:00\" }\n"
              + "{ \"id\": 6, \"ts\": \"2001-01-06 00:00:00\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "2001-01-01 00:00:00"), // updated
              row(2, "2001-01-02 00:00:00") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT id, CAST(ts AS STRING) FROM %s ORDER BY id", tableName));

      removeTables();
    }
  }

  @Test
  public void testMergeWithBucketTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD bucket(2, dep)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", tableName));

      removeTables();
    }
  }

  @Test
  public void testMergeWithTruncateTransform() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD truncate(dep, 2)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", tableName));

      removeTables();
    }
  }

  @Test
  public void testMergeIntoPartitionedAndOrderedTable() {
    for (DistributionMode mode : DistributionMode.values()) {
      createAndInitTable("id INT, dep STRING");
      sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);
      sql("ALTER TABLE %s WRITE ORDERED BY (id)", tableName);
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
          tableName, WRITE_DISTRIBUTION_MODE, mode.modeName());

      append(
          tableName,
          "{ \"id\": 1, \"dep\": \"emp-id-one\" }\n" + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      createOrReplaceView(
          "source",
          "id INT, dep STRING",
          "{ \"id\": 2, \"dep\": \"emp-id-2\" }\n"
              + "{ \"id\": 1, \"dep\": \"emp-id-1\" }\n"
              + "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

      sql(
          "MERGE INTO %s AS t USING source AS s "
              + "ON t.id == s.id "
              + "WHEN MATCHED AND t.id = 1 THEN "
              + "  UPDATE SET * "
              + "WHEN MATCHED AND t.id = 6 THEN "
              + "  DELETE "
              + "WHEN NOT MATCHED AND s.id = 2 THEN "
              + "  INSERT *",
          tableName);

      ImmutableList<Object[]> expectedRows =
          ImmutableList.of(
              row(1, "emp-id-1"), // updated
              row(2, "emp-id-2") // new
              );
      assertEquals(
          "Should have expected rows",
          expectedRows,
          sql("SELECT * FROM %s ORDER BY id", tableName));

      removeTables();
    }
  }

  @Test
  public void testSelfMerge() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    sql(
        "MERGE INTO %s t USING %s s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET v = 'x' "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        tableName, tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "x"), // updated
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithSourceAsSelfSubquery() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView("source", Arrays.asList(1, null), Encoders.INT());

    sql(
        "MERGE INTO %s t USING (SELECT id AS value FROM %s r JOIN source ON r.id = source.value) s "
            + "ON t.id == s.value "
            + "WHEN MATCHED AND t.id = 1 THEN "
            + "  UPDATE SET v = 'x' "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES ('invalid', -1) ",
        tableName, tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "x"), // updated
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public synchronized void testMergeWithSerializableIsolation() throws InterruptedException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitTable("id INT, dep STRING");
    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, MERGE_ISOLATION_LEVEL, "serializable");

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);

    // merge thread
    Future<?> mergeFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET dep = 'x'",
                    tableName);
                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < Integer.MAX_VALUE; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
                barrier.incrementAndGet();
              }
            });

    try {
      Assertions.assertThatThrownBy(mergeFuture::get)
          .isInstanceOf(ExecutionException.class)
          .cause()
          .isInstanceOf(SparkException.class)
          .cause()
          .isInstanceOf(ValidationException.class)
          .hasMessageContaining("Found conflicting files that can contain");
    } finally {
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public synchronized void testMergeWithSnapshotIsolation()
      throws InterruptedException, ExecutionException {
    // cannot run tests with concurrency for Hadoop tables without atomic renames
    Assume.assumeFalse(catalogName.equalsIgnoreCase("testhadoop"));

    createAndInitTable("id INT, dep STRING");
    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, MERGE_ISOLATION_LEVEL, "snapshot");

    ExecutorService executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor) Executors.newFixedThreadPool(2));

    AtomicInteger barrier = new AtomicInteger(0);

    // merge thread
    Future<?> mergeFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < 20; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql(
                    "MERGE INTO %s t USING source s "
                        + "ON t.id == s.value "
                        + "WHEN MATCHED THEN "
                        + "  UPDATE SET dep = 'x'",
                    tableName);
                barrier.incrementAndGet();
              }
            });

    // append thread
    Future<?> appendFuture =
        executorService.submit(
            () -> {
              for (int numOperations = 0; numOperations < 20; numOperations++) {
                while (barrier.get() < numOperations * 2) {
                  sleep(10);
                }
                sql("INSERT INTO TABLE %s VALUES (1, 'hr')", tableName);
                barrier.incrementAndGet();
              }
            });

    try {
      mergeFuture.get();
    } finally {
      appendFuture.cancel(true);
    }

    executorService.shutdown();
    Assert.assertTrue("Timeout", executorService.awaitTermination(2, TimeUnit.MINUTES));
  }

  @Test
  public void testMergeWithExtraColumnsInSource() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"extra_col\": -1, \"v\": \"v1_1\" }\n"
            + "{ \"id\": 3, \"extra_col\": -1, \"v\": \"v3\" }\n"
            + "{ \"id\": 4, \"extra_col\": -1, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "v1_1"), // new
            row(2, "v2"), // kept
            row(3, "v3"), // new
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithNullsInTargetAndSource() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1"), // kept
            row(null, "v1_1"), // new
            row(2, "v2"), // kept
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", tableName));
  }

  @Test
  public void testMergeWithNullSafeEquals() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 4, \"v\": \"v4\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id <=> source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1_1"), // updated
            row(2, "v2"), // kept
            row(4, "v4") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", tableName));
  }

  @Test
  public void testMergeWithNullCondition() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": null, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": null, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id AND NULL "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(null, "v1"), // kept
            row(null, "v1_1"), // new
            row(2, "v2"), // kept
            row(2, "v2_2") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", tableName));
  }

  @Test
  public void testMergeWithNullActionConditions() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"v\": \"v1_1\" }\n"
            + "{ \"id\": 2, \"v\": \"v2_2\" }\n"
            + "{ \"id\": 3, \"v\": \"v3_3\" }");

    // all conditions are NULL and will never match any rows
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 AND NULL THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' AND NULL THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND source.id = 3 AND NULL THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows1 =
        ImmutableList.of(
            row(1, "v1"), // kept
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows1, sql("SELECT * FROM %s ORDER BY v", tableName));

    // only the update and insert conditions are NULL
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 AND NULL THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED AND source.id = 3 AND NULL THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows2 =
        ImmutableList.of(
            row(2, "v2") // kept
            );
    assertEquals(
        "Output should match", expectedRows2, sql("SELECT * FROM %s ORDER BY v", tableName));
  }

  @Test
  public void testMergeWithMultipleMatchingActions() {
    createAndInitTable(
        "id INT, v STRING", "{ \"id\": 1, \"v\": \"v1\" }\n" + "{ \"id\": 2, \"v\": \"v2\" }");

    createOrReplaceView(
        "source", "{ \"id\": 1, \"v\": \"v1_1\" }\n" + "{ \"id\": 2, \"v\": \"v2_2\" }");

    // the order of match actions is important in this case
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED AND source.id = 1 THEN "
            + "  UPDATE SET v = source.v "
            + "WHEN MATCHED AND source.v = 'v1_1' THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (v, id) VALUES (source.v, source.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "v1_1"), // updated (also matches the delete cond but update is first)
            row(2, "v2") // kept (matches neither the update nor the delete cond)
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY v", tableName));
  }

  @Test
  public void testMergeWithMultipleRowGroupsParquet() throws NoSuchTableException {
    Assume.assumeTrue(fileFormat.equalsIgnoreCase("parquet"));

    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')",
        tableName, PARQUET_ROW_GROUP_SIZE_BYTES, 100);
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')", tableName, SPLIT_SIZE, 100);

    createOrReplaceView("source", Collections.singletonList(1), Encoders.INT());

    List<Integer> ids = Lists.newArrayListWithCapacity(200);
    for (int id = 1; id <= 200; id++) {
      ids.add(id);
    }
    Dataset<Row> df =
        spark
            .createDataset(ids, Encoders.INT())
            .withColumnRenamed("value", "id")
            .withColumn("dep", lit("hr"));
    df.coalesce(1).writeTo(tableName).append();

    Assert.assertEquals(200, spark.table(tableName).count());

    // update a record from one of two row groups and copy over the second one
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.value "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = 'x'",
        tableName);

    Assert.assertEquals(200, spark.table(tableName).count());
  }

  @Test
  public void testMergeInsertOnly() {
    createAndInitTable(
        "id STRING, v STRING",
        "{ \"id\": \"a\", \"v\": \"v1\" }\n" + "{ \"id\": \"b\", \"v\": \"v2\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": \"a\", \"v\": \"v1_1\" }\n"
            + "{ \"id\": \"a\", \"v\": \"v1_2\" }\n"
            + "{ \"id\": \"c\", \"v\": \"v3\" }\n"
            + "{ \"id\": \"d\", \"v\": \"v4_1\" }\n"
            + "{ \"id\": \"d\", \"v\": \"v4_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row("a", "v1"), // kept
            row("b", "v2"), // kept
            row("c", "v3"), // new
            row("d", "v4_1"), // new
            row("d", "v4_2") // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeInsertOnlyWithCondition() {
    createAndInitTable("id INTEGER, v INTEGER", "{ \"id\": 1, \"v\": 1 }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"v\": 11, \"is_new\": true }\n"
            + "{ \"id\": 2, \"v\": 21, \"is_new\": true }\n"
            + "{ \"id\": 2, \"v\": 22, \"is_new\": false }");

    // validate assignments are reordered to match the table attrs
    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN NOT MATCHED AND is_new = TRUE THEN "
            + "  INSERT (v, id) VALUES (s.v + 100, s.id)",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 1), // kept
            row(2, 121) // new
            );
    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeAlignsUpdateAndInsertActions() {
    createAndInitTable("id INT, a INT, b STRING", "{ \"id\": 1, \"a\": 2, \"b\": \"str\" }");
    createOrReplaceView(
        "source",
        "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n"
            + "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET b = c2, a = c1, t.id = source.id "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (b, a, id) VALUES (c2, c1, id)",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, -2, "new_str_1"), row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeUpdatesNestedStructFields() {
    createAndInitTable(
        "id INT, s STRUCT<c1:INT,c2:STRUCT<a:ARRAY<INT>,m:MAP<STRING, STRING>>>",
        "{ \"id\": 1, \"s\": { \"c1\": 2, \"c2\": { \"a\": [1,2], \"m\": { \"a\": \"b\"} } } } }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2 }");

    // update primitive, array, map columns inside a struct
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.c1 = source.c1, t.s.c2.a = array(-1, -2), t.s.c2.m = map('k', 'v')",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-2, row(ImmutableList.of(-1, -2), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // set primitive, array, map columns to NULL (proper casts should be in place)
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.c1 = NULL, t.s.c2 = NULL",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // update all fields in a struct
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s = named_struct('c1', 100, 'c2', named_struct('a', array(1), 'm', map('x', 'y')))",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(100, row(ImmutableList.of(1), ImmutableMap.of("x", "y"))))),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithInferredCasts() {
    createAndInitTable("id INT, s STRING", "{ \"id\": 1, \"s\": \"value\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2}");

    // -2 in source should be casted to "-2" in target
    sql(
        "MERGE INTO %s t USING source "
            + "ON t.id == source.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s = source.c1",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, "-2")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");
    createOrReplaceView("source", "{ \"id\": 1, \"n1\": -10 }");

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.s.n1 = s.n1",
        tableName);

    assertEquals(
        "Output should match",
        ImmutableList.of(row(1, row(-10, null))),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    createAndInitTable("id INT, name STRING", "{ \"id\": 1, \"name\": \"n1\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"name\": \"n2\" }");

    Dataset<Row> query = spark.sql("SELECT name FROM " + tableName);
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n1")), sql("SELECT * FROM tmp"));

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.name = s.name",
        tableName);

    assertEquals(
        "View should have correct data", ImmutableList.of(row("n2")), sql("SELECT * FROM tmp"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testMergeWithNonExistingColumns() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about the invalid top-level column",
        AnalysisException.class,
        "cannot resolve '`t.invalid_col`'",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.invalid_col = s.c2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about the invalid nested column",
        AnalysisException.class,
        "No such struct field invalid_col",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n2.invalid_col = s.c2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about the invalid top-level column",
        AnalysisException.class,
        "cannot resolve '`invalid_col`'",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                  + "WHEN NOT MATCHED THEN "
                  + "  INSERT (id, invalid_col) VALUES (s.c1, null)",
              tableName);
        });
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about the nested column",
        AnalysisException.class,
        "Nested fields are not supported inside INSERT clauses",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                  + "WHEN NOT MATCHED THEN "
                  + "  INSERT (id, c.n2) VALUES (s.c1, null)",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about duplicate columns",
        AnalysisException.class,
        "Duplicate column names inside INSERT clause",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n2.dn1 = s.c2 "
                  + "WHEN NOT MATCHED THEN "
                  + "  INSERT (id, id) VALUES (s.c1, null)",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about missing columns",
        AnalysisException.class,
        "must provide values for all columns of the target table",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN NOT MATCHED THEN "
                  + "  INSERT (id) VALUES (s.c1)",
              tableName);
        });
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    createAndInitTable("id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about updating an array column",
        AnalysisException.class,
        "Updating nested fields is only supported for structs",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.a.c1 = s.c2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about updating a map column",
        AnalysisException.class,
        "Updating nested fields is only supported for structs",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.m.key = 'new_key'",
              tableName);
        });
  }

  @Test
  public void testMergeWithConflictingUpdates() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a top-level column",
        AnalysisException.class,
        "Updates are in conflict",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.id = 1, t.c.n1 = 2, t.id = 2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a nested column",
        AnalysisException.class,
        "Updates are in conflict for these columns",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n1 = 1, t.id = 2, t.c.n1 = 2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about conflicting updates to a nested column",
        AnalysisException.class,
        "Updates are in conflict",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET c.n1 = 1, c = named_struct('n1', 1, 'n2', named_struct('dn1', 1, 'dn2', 2))",
              tableName);
        });
  }

  @Test
  public void testMergeWithInvalidAssignments() {
    createAndInitTable(
        "id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL");
    createOrReplaceView(
        "source",
        "c1 INT, c2 STRUCT<n1:INT NOT NULL> NOT NULL, c3 STRING NOT NULL, c4 STRUCT<dn2:INT,dn1:INT>",
        "{ \"c1\": -100, \"c2\": { \"n1\" : 1 }, \"c3\" : 'str', \"c4\": { \"dn2\": 1, \"dn2\": 2 } }");

    for (String policy : new String[] {"ansi", "strict"}) {
      withSQLConf(
          ImmutableMap.of("spark.sql.storeAssignmentPolicy", policy),
          () -> {
            AssertHelpers.assertThrows(
                "Should complain about writing nulls to a top-level column",
                AnalysisException.class,
                "Cannot write nullable values to non-null column",
                () -> {
                  sql(
                      "MERGE INTO %s t USING source s "
                          + "ON t.id == s.c1 "
                          + "WHEN MATCHED THEN "
                          + "  UPDATE SET t.id = NULL",
                      tableName);
                });

            AssertHelpers.assertThrows(
                "Should complain about writing nulls to a nested column",
                AnalysisException.class,
                "Cannot write nullable values to non-null column",
                () -> {
                  sql(
                      "MERGE INTO %s t USING source s "
                          + "ON t.id == s.c1 "
                          + "WHEN MATCHED THEN "
                          + "  UPDATE SET t.s.n1 = NULL",
                      tableName);
                });

            AssertHelpers.assertThrows(
                "Should complain about writing missing fields in structs",
                AnalysisException.class,
                "missing fields",
                () -> {
                  sql(
                      "MERGE INTO %s t USING source s "
                          + "ON t.id == s.c1 "
                          + "WHEN MATCHED THEN "
                          + "  UPDATE SET t.s = s.c2",
                      tableName);
                });

            AssertHelpers.assertThrows(
                "Should complain about writing invalid data types",
                AnalysisException.class,
                "Cannot safely cast",
                () -> {
                  sql(
                      "MERGE INTO %s t USING source s "
                          + "ON t.id == s.c1 "
                          + "WHEN MATCHED THEN "
                          + "  UPDATE SET t.s.n1 = s.c3",
                      tableName);
                });

            AssertHelpers.assertThrows(
                "Should complain about writing incompatible structs",
                AnalysisException.class,
                "field name does not match",
                () -> {
                  sql(
                      "MERGE INTO %s t USING source s "
                          + "ON t.id == s.c1 "
                          + "WHEN MATCHED THEN "
                          + "  UPDATE SET t.s.n2 = s.c4",
                      tableName);
                });
          });
    }
  }

  @Test
  public void testMergeWithNonDeterministicConditions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic search conditions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed in",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 AND rand() > t.id "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n1 = -1",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic update conditions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed in",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND rand() > t.id THEN "
                  + "  UPDATE SET t.c.n1 = -1",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic delete conditions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed in",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND rand() > t.id THEN "
                  + "  DELETE",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic insert conditions",
        AnalysisException.class,
        "nondeterministic expressions are only allowed in",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN NOT MATCHED AND rand() > c1 THEN "
                  + "  INSERT (id, c) VALUES (1, null)",
              tableName);
        });
  }

  @Test
  public void testMergeWithAggregateExpressions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about agg expressions in search conditions",
        AnalysisException.class,
        "contains one or more unsupported",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 AND max(t.id) == 1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n1 = -1",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about agg expressions in update conditions",
        AnalysisException.class,
        "contains one or more unsupported",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND sum(t.id) < 1 THEN "
                  + "  UPDATE SET t.c.n1 = -1",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic delete conditions",
        AnalysisException.class,
        "contains one or more unsupported",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND sum(t.id) THEN "
                  + "  DELETE",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about non-deterministic insert conditions",
        AnalysisException.class,
        "contains one or more unsupported",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN NOT MATCHED AND sum(c1) < 1 THEN "
                  + "  INSERT (id, c) VALUES (1, null)",
              tableName);
        });
  }

  @Test
  public void testMergeWithSubqueriesInConditions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain about subquery expressions",
        AnalysisException.class,
        "Subqueries are not supported in conditions",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 AND t.id < (SELECT max(c2) FROM source) "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET t.c.n1 = s.c2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about subquery expressions",
        AnalysisException.class,
        "Subqueries are not supported in conditions",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND t.id < (SELECT max(c2) FROM source) THEN "
                  + "  UPDATE SET t.c.n1 = s.c2",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about subquery expressions",
        AnalysisException.class,
        "Subqueries are not supported in conditions",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN MATCHED AND t.id NOT IN (SELECT c2 FROM source) THEN "
                  + "  DELETE",
              tableName);
        });

    AssertHelpers.assertThrows(
        "Should complain about subquery expressions",
        AnalysisException.class,
        "Subqueries are not supported in conditions",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.c1 "
                  + "WHEN NOT MATCHED AND s.c1 IN (SELECT c2 FROM source) THEN "
                  + "  INSERT (id, c) VALUES (1, null)",
              tableName);
        });
  }

  @Test
  public void testMergeWithTargetColumnsInInsertCondtions() {
    createAndInitTable("id INT, c2 INT");
    createOrReplaceView("source", "{ \"id\": 1, \"value\": 11 }");

    AssertHelpers.assertThrows(
        "Should complain about the target column",
        AnalysisException.class,
        "cannot resolve '`c2`'",
        () -> {
          sql(
              "MERGE INTO %s t USING source s "
                  + "ON t.id == s.id "
                  + "WHEN NOT MATCHED AND c2 = 1 THEN "
                  + "  INSERT (id, c2) VALUES (s.id, null)",
              tableName);
        });
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    createOrReplaceView("target", "{ \"c1\": -100, \"c2\": -200 }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows(
        "Should complain non iceberg target table",
        UnsupportedOperationException.class,
        "MERGE INTO TABLE is not supported temporarily.",
        () -> {
          sql(
              "MERGE INTO target t USING source s "
                  + "ON t.c1 == s.c1 "
                  + "WHEN MATCHED THEN "
                  + "  UPDATE SET *");
        });
  }

  /**
   * Tests a merge where both the source and target are evaluated to be partitioned by
   * SingePartition at planning time but DynamicFileFilterExec will return an empty target.
   */
  @Test
  public void testMergeSinglePartitionPartitioning() {
    // This table will only have a single file and a single partition
    createAndInitTable("id INT", "{\"id\": -1}");

    // Coalesce forces our source into a SinglePartition distribution
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET *"
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(row(-1), row(0), row(1), row(2), row(3), row(4));

    List<Object[]> result = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertEquals("Should correctly add the non-matching rows", expectedRows, result);
  }

  @Test
  public void testMergeEmptyTable() {
    // This table will only have a single file and a single partition
    createAndInitTable("id INT", null);

    // Coalesce forces our source into a SinglePartition distribution
    spark.range(0, 5).coalesce(1).createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET *"
            + "WHEN NOT MATCHED THEN INSERT *",
        tableName);

    ImmutableList<Object[]> expectedRows = ImmutableList.of(row(0), row(1), row(2), row(3), row(4));

    List<Object[]> result = sql("SELECT * FROM %s ORDER BY id", tableName);
    assertEquals("Should correctly add the non-matching rows", expectedRows, result);
  }

  @Test
  public void testFileFilterMetric() throws Exception {
    createAndInitTable("id INT, dep STRING");
    spark.sql(String.format("INSERT INTO %s VALUES (1, 'emp-id-one')", tableName));
    spark.sql(String.format("INSERT INTO %s VALUES (6, 'emp-id-six')", tableName));

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 6, \"dep\": \"emp-id-6\" }");

    Map<String, String> expectedMetrics = Maps.newHashMap();
    expectedMetrics.put("candidate files", "2");
    expectedMetrics.put("matching files", "1");

    checkMetrics(
        () ->
            spark.sql(
                String.format(
                    "MERGE INTO %s AS t USING source AS s "
                        + "ON t.id == s.id "
                        + "WHEN MATCHED THEN UPDATE SET * ",
                    tableName)),
        expectedMetrics);
  }
}
