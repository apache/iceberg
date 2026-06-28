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

import static org.apache.spark.sql.functions.udf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestReplaceScopedData extends SparkRowLevelOperationsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
    sql("DROP VIEW IF EXISTS left_src");
    sql("DROP VIEW IF EXISTS right_src");
  }

  @TestTemplate
  public void testScopedReplace() {
    createAndInitTable(
        "id INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"it\" }\n"
            + "{ \"id\": 4, \"dep\": \"sales\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 10, \"dep\": \"hr\" }\n" + "{ \"id\": 11, \"dep\": \"sales\" }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should replace rows with matching scope values and retain the rest",
        ImmutableList.of(row(3, "it"), row(10, "hr"), row(11, "sales")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testDuplicateSourceScopesDeleteTargetRowsOnce() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"it\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 10, \"dep\": \"hr\" }\n" + "{ \"id\": 11, \"dep\": \"hr\" }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should delete the target scope once and append all source rows",
        ImmutableList.of(row(3, "it"), row(10, "hr"), row(11, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testNullScopeMatchesNullScope() {
    createAndInitTable("id INT, dep STRING");
    sql("INSERT INTO %s VALUES (1, NULL), (2, 'hr'), (3, NULL)", tableName);
    createBranchIfNeeded();

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 10, \"dep\": null }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should use null-safe equality for replace-scope values",
        ImmutableList.of(row(2, "hr"), row(10, null)),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testNondeterministicSourceIsEvaluatedOnce() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    AtomicInteger depIndex = new AtomicInteger();
    spark
        .udf()
        .register(
            "replace_scoped_data_dep",
            udf(() -> depIndex.getAndIncrement() == 0 ? "hr" : "it", DataTypes.StringType)
                .asNondeterministic()
                .asNonNullable());

    sql(
        "INSERT INTO %s REPLACE USING (dep) " + "SELECT 10 AS id, replace_scoped_data_dep() AS dep",
        commitTarget());

    assertEquals(
        "Should evaluate the source once for deletes and inserts",
        ImmutableList.of(row(2, "it"), row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testNewSourceScopesAppendWithoutDeletingTargetRows() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 10, \"dep\": \"sales\" }\n" + "{ \"id\": 11, \"dep\": \"eng\" }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should append new scopes and keep every existing row when no scope matches",
        ImmutableList.of(row(1, "hr"), row(2, "it"), row(10, "sales"), row(11, "eng")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMixedMatchingAndNewSourceScopes() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"it\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 10, \"dep\": \"hr\" }\n" + "{ \"id\": 11, \"dep\": \"sales\" }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should replace matching scopes, append new scopes, and retain untouched scopes",
        ImmutableList.of(row(3, "it"), row(10, "hr"), row(11, "sales")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testEmptySourceLeavesTargetUnchanged() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }");

    // An empty source has no scope values to match and no rows to append, so the target must be
    // left exactly as it was.
    sql(
        "INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source WHERE id < 0",
        commitTarget());

    assertEquals(
        "Empty source should delete nothing and append nothing",
        ImmutableList.of(row(1, "hr"), row(2, "it")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMultiColumnScopeMatchesOnTheFullTuple() {
    createAndInitTable(
        "id INT, dep STRING, subdep STRING",
        "{ \"id\": 1, \"dep\": \"hr\", \"subdep\": \"a\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\", \"subdep\": \"b\" }\n"
            + "{ \"id\": 3, \"dep\": \"it\", \"subdep\": \"a\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, subdep STRING",
        "{ \"id\": 10, \"dep\": \"hr\", \"subdep\": \"a\" }\n"
            + "{ \"id\": 11, \"dep\": \"it\", \"subdep\": \"a\" }");

    sql(
        "INSERT INTO %s REPLACE USING (dep, subdep) SELECT id, dep, subdep FROM source",
        commitTarget());

    // Only rows whose (dep, subdep) tuple appears in the source are replaced; (hr, b) is retained
    // even though its dep matches a replaced scope.
    assertEquals(
        "Should match the full scope tuple instead of any single scope column",
        ImmutableList.of(row(2, "hr", "b"), row(10, "hr", "a"), row(11, "it", "a")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testReplaceUsingTextInRegularInsertQueryDoesNotTriggerScopedReplaceParser() {
    createAndInitTable("id INT, dep STRING");

    sql("INSERT INTO %s SELECT 1, 'replace using ('", tableName);

    assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(row(1, "replace using ("));
  }

  @TestTemplate
  public void testJoinAliasNamedReplaceDoesNotTriggerScopedReplaceParser() {
    createAndInitTable("id INT, dep STRING");
    createOrReplaceView("left_src", "id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");
    createOrReplaceView("right_src", "id INT", "{ \"id\": 1 }");

    // Scoped-replace detection must stay at the command head. Valid Spark query bodies can contain
    // the same token sequence, for example a join alias named `replace` followed by a USING join.
    sql(
        "INSERT INTO %s SELECT id, left_src.dep FROM left_src JOIN right_src replace USING (id)",
        tableName);

    assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(row(1, "hr"));
  }

  @TestTemplate
  public void testNestedBlockCommentDoesNotTriggerScopedReplaceParser() {
    createAndInitTable("id INT, dep STRING");

    // Spark treats block comments as nesting, so the inner `*/` does not close the comment and the
    // whole `REPLACE USING (dep)` text stays commented out. The router must mask it the same way
    // and leave this as an ordinary insert for Spark to parse.
    sql("INSERT INTO %s /* outer /* inner */ REPLACE USING (dep) */ SELECT 1, 'hr'", tableName);

    assertThat(sql("SELECT * FROM %s", tableName)).containsExactly(row(1, "hr"));
  }

  @TestTemplate
  public void testCommentInCommandHeadStillRoutesToScopedReplace() {
    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");

    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }");

    // A (terminated) comment in the command head is masked for detection and skipped by the head
    // grammar, so this must still be recognized and executed as a scoped replace.
    sql(
        "INSERT INTO %s /* reload hr */ REPLACE USING (dep) SELECT id, dep FROM source",
        commitTarget());

    assertEquals(
        "A comment in the head should not prevent scoped replace detection",
        ImmutableList.of(row(2, "it"), row(10, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testStoreAssignmentPolicyRejectsUnsafeSourceCast() {
    createAndInitTable("id INT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");

    // Scoped replace uses the same store-assignment rules as INSERT. Under ANSI, unsafe source
    // values must fail during analysis instead of slipping through the row-level rewrite.
    withSQLConf(
        ImmutableMap.of(SQLConf.STORE_ASSIGNMENT_POLICY().key(), "ansi"),
        () ->
            assertThatThrownBy(
                    () ->
                        sql(
                            "INSERT INTO %s REPLACE USING (dep) SELECT 'x' AS id, 'hr' AS dep",
                            commitTarget()))
                .isInstanceOf(AnalysisException.class)
                .hasMessageContaining("INCOMPATIBLE_DATA_FOR_TABLE"));
  }

  @TestTemplate
  public void testSafeSourceCastIsAligned() {
    createAndInitTable("id BIGINT, dep STRING", "{ \"id\": 1, \"dep\": \"hr\" }");
    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }");

    // Valid store-assignment casts still need to be preserved when the rewrite aligns the source to
    // the target output.
    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    assertEquals(
        "Should align the source column to the target type",
        ImmutableList.of(row(10L, "hr")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }
}
