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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Encoders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for verifying that merge metrics from Spark's MergeSummary are persisted in Iceberg's
 * snapshot summary.
 *
 * <p>Note: The numTargetRowsCopied metric behaves differently between copy-on-write and
 * merge-on-read modes:
 *
 * <ul>
 *   <li>Copy-on-write: Unchanged rows in modified files are rewritten, so numTargetRowsCopied > 0
 *   <li>Merge-on-read: Uses delete files, so unchanged rows aren't copied, numTargetRowsCopied = 0
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestMergeMetrics extends SparkRowLevelOperationsTestBase {

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  /**
   * Returns the expected number of rows copied for the given number of unchanged rows. In
   * copy-on-write mode, unchanged rows in modified files are rewritten. In merge-on-read mode,
   * unchanged rows aren't copied.
   */
  protected abstract long expectedRowsCopied(long unchangedRowsInModifiedFiles);

  @TestTemplate
  public void testMergeMetricsWithMatchedUpdate() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"software\" }");

    createOrReplaceView("source", ImmutableList.of(1, 3), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = 'updated'",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // One row matched and was updated, one row unchanged
    // In CoW mode, the unchanged row is copied; in MoR mode, it's not
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(1));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 0);
  }

  @TestTemplate
  public void testMergeMetricsWithMatchedDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"software\" }\n"
            + "{ \"id\": 3, \"dep\": \"finance\" }");

    createOrReplaceView("source", ImmutableList.of(1, 2), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value "
            + "WHEN MATCHED THEN "
            + "  DELETE",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // Two rows matched and were deleted, one row unchanged
    // In CoW mode, the unchanged row is copied; in MoR mode, it's not
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(1));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 2);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 2);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 0);
  }

  @TestTemplate
  public void testMergeMetricsWithAllClauses() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"software\" }\n"
            + "{ \"id\": 3, \"dep\": \"finance\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr-updated\" }\n" // will update id=1
            + "{ \"id\": 2, \"dep\": \"software-delete\" }\n" // will delete id=2
            + "{ \"id\": 4, \"dep\": \"new-dept\" }"); // will insert id=4

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND s.dep LIKE '%%delete%%' THEN "
            + "  DELETE "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = s.dep "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // 1 update, 1 delete, 1 insert, 1 unchanged row (id=3)
    // In CoW mode, the unchanged row is copied; in MoR mode, it's not
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(1));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 0);
  }

  @TestTemplate
  public void testMergeMetricsWithNotMatchedBySourceUpdate() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"software\" }\n"
            + "{ \"id\": 3, \"dep\": \"finance\" }");

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr-updated\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = s.dep "
            + "WHEN NOT MATCHED BY SOURCE THEN "
            + "  UPDATE SET dep = 'orphaned'",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // 1 matched update, 2 not matched by source updates, 0 unchanged rows
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(0));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 3);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 2);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 0);
  }

  @TestTemplate
  public void testMergeMetricsWithNotMatchedBySourceDelete() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"software\" }\n"
            + "{ \"id\": 3, \"dep\": \"finance\" }");

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr-updated\" }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET dep = s.dep "
            + "WHEN NOT MATCHED BY SOURCE THEN "
            + "  DELETE",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // 1 matched update, 2 not matched by source deletes, 0 unchanged rows
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(0));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 2);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 1);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 0);
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 2);
  }

  @TestTemplate
  public void testMergeMetricsWithMultipleUpdatesAndDeletes() {
    createAndInitTable(
        "id INT, dep STRING, salary INT",
        "{ \"id\": 1, \"dep\": \"hr\", \"salary\": 100 }\n"
            + "{ \"id\": 2, \"dep\": \"software\", \"salary\": 200 }\n"
            + "{ \"id\": 3, \"dep\": \"finance\", \"salary\": 300 }\n"
            + "{ \"id\": 4, \"dep\": \"marketing\", \"salary\": 400 }\n"
            + "{ \"id\": 5, \"dep\": \"sales\", \"salary\": 500 }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, salary INT",
        "{ \"id\": 1, \"dep\": \"hr\", \"salary\": 150 }\n"
            + "{ \"id\": 2, \"dep\": \"software\", \"salary\": 250 }\n"
            + "{ \"id\": 3, \"dep\": \"finance\", \"salary\": 350 }\n"
            + "{ \"id\": 6, \"dep\": \"new\", \"salary\": 600 }\n"
            + "{ \"id\": 7, \"dep\": \"newer\", \"salary\": 700 }");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED AND t.salary < 200 THEN "
            + "  DELETE "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET salary = s.salary "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT * "
            + "WHEN NOT MATCHED BY SOURCE AND t.salary > 400 THEN "
            + "  DELETE "
            + "WHEN NOT MATCHED BY SOURCE THEN "
            + "  UPDATE SET dep = 'orphaned'",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    Map<String, String> summary = currentSnapshot.summary();

    // id=1: matched delete
    // id=2,3: matched updates
    // id=4: not matched by source update
    // id=5: not matched by source delete
    // id=6,7: inserts
    // All 5 target rows are either updated or deleted, so 0 unchanged rows
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-copied", expectedRowsCopied(0));
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-updated", 3); // id=2,3,4
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-deleted", 2); // id=1 and id=5
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-inserted", 2); // id=6,7
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-updated", 2); // id=2,3
    assertMergeMetric(summary, "spark.merge-into.num-target-rows-matched-deleted", 1); // id=1
    assertMergeMetric(
        summary, "spark.merge-into.num-target-rows-not-matched-by-source-updated", 1); // id=4
    assertMergeMetric(
        summary, "spark.merge-into.num-target-rows-not-matched-by-source-deleted", 1); // id=5
  }

  private void assertMergeMetric(Map<String, String> summary, String key, long expectedValue) {
    assertThat(summary).as("Snapshot summary should contain merge metric: " + key).containsKey(key);
    assertThat(summary.get(key))
        .as("Merge metric " + key + " should have expected value")
        .isEqualTo(String.valueOf(expectedValue));
  }
}
