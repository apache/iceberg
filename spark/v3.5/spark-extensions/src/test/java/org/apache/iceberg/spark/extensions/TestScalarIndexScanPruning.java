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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Verifies the read side of the SCALAR index at the SQL level: once {@code build_scalar_index}
 * has populated an index, a subsequent equality query on the indexed column returns the correct
 * row -- functional correctness, which never depends on pruning actually kicking in. Whether
 * {@code FileScanTaskFilteringScan} (the mechanism that actually enforces pruning) narrows the
 * planned file set correctly is verified separately and in isolation by {@code
 * TestFileScanTaskFilteringScan}: {@code Dataset#inputFiles()} does not reflect Iceberg's
 * DataSourceV2 scans in this Spark version, so it is not a usable signal at this level.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestScalarIndexScanPruning extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testEqualityQueryReturnsCorrectRowAfterIndexBuild() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    // Separate INSERTs so the rows land in separate data files -- otherwise there is nothing for
    // the index to prune between.
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'ccc')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE data = 'bbb'", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(2L);
    assertThat(result.get(0)[1]).isEqualTo("bbb");
  }

  @TestTemplate
  public void testEqualityQueryOnAbsentValueReturnsNoRows() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id FROM %s WHERE data = 'does-not-exist'", tableName);
    assertThat(result).isEmpty();
  }

  @TestTemplate
  public void testQueryStillCorrectWithoutAnyIndex() {
    // Sanity check that nothing about tryPruneUsingScalarIndex breaks a table with no index at
    // all -- this is the common case (most tables), so it must be a complete no-op.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa'), (2, 'bbb')", tableName);

    List<Object[]> result = sql("SELECT id FROM %s WHERE data = 'bbb'", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(2L);
  }

  @TestTemplate
  public void testQueryCorrectAfterTableChangesSinceIndexBuild() {
    // Exercises the covered/uncovered-files staleness path: a commit lands after the index is
    // built, so the index's snapshot no longer matches table.currentSnapshot() exactly.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    // New commit after the index was built -- this row lives only in an "uncovered" file.
    sql("INSERT INTO TABLE %s VALUES (3, 'ccc')", tableName);

    // Covered file, resolvable via the (now-stale) index.
    List<Object[]> covered = sql("SELECT id, data FROM %s WHERE data = 'aaa'", tableName);
    assertThat(covered).hasSize(1);
    assertThat(covered.get(0)[0]).isEqualTo(1L);

    // Uncovered file, added after the index's snapshot -- must still be found even though the
    // index knows nothing about it.
    List<Object[]> uncovered = sql("SELECT id, data FROM %s WHERE data = 'ccc'", tableName);
    assertThat(uncovered).hasSize(1);
    assertThat(uncovered.get(0)[0]).isEqualTo(3L);

    // Absent from both covered and uncovered files -- must still return empty.
    List<Object[]> absent = sql("SELECT id FROM %s WHERE data = 'does-not-exist'", tableName);
    assertThat(absent).isEmpty();
  }

  @TestTemplate
  public void testQueryCorrectAfterCompactionSinceIndexBuild() {
    // Compaction is a non-append snapshot between the index's snapshot and the current one --
    // uncoveredFilePathsSince must detect this and force a full fallback (not prune at all),
    // rather than resolve matches against file paths compaction may have rewritten away. Without
    // that safeguard, a row physically moved during compaction into a file outside both the
    // index's covered matches and the (incomplete) uncovered set would silently never be scanned.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    sql("CALL %s.system.rewrite_data_files(table => '%s')", catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE data = 'aaa'", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(1L);
    assertThat(result.get(0)[1]).isEqualTo("aaa");
  }

  @TestTemplate
  public void testRangeQueryResolvesViaIdentityIndex() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (5, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (10, 'c')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
            + " transform => 'IDENTITY')",
        catalogName, tableIdent);

    // A two-sided range -- Spark decomposes this into two range predicates on the same column,
    // which tryPruneUsingScalarIndex must combine into a single [lower, upper] bound.
    List<Object[]> between = sql("SELECT id, data FROM %s WHERE id > 3 AND id < 8", tableName);
    assertThat(between).hasSize(1);
    assertThat(between.get(0)[0]).isEqualTo(5L);
    assertThat(between.get(0)[1]).isEqualTo("b");

    // A one-sided range.
    List<Object[]> atLeast = sql("SELECT id FROM %s WHERE id >= 5", tableName);
    assertThat(atLeast).extracting(r -> r[0]).containsExactlyInAnyOrder(5L, 10L);

    // A range matching nothing.
    List<Object[]> none = sql("SELECT id FROM %s WHERE id > 100", tableName);
    assertThat(none).isEmpty();
  }

  @TestTemplate
  public void testRangeQueryFallsBackSafelyForHashIndex() {
    // HASH scatters values across buckets, so a range predicate cannot be resolved via a
    // HASH-transform index -- must still return correct results via the normal (residual-filter)
    // path rather than attempt to use the index for something it cannot answer.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (5, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (10, 'c')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE id > 3 AND id < 8", tableName);
    assertThat(result).hasSize(1);
    assertThat(result.get(0)[0]).isEqualTo(5L);
    assertThat(result.get(0)[1]).isEqualTo("b");
  }

  @TestTemplate
  public void testInPredicateResolvesViaHashIndex() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'aaa')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'bbb')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'ccc')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('data'),"
            + " transform => 'HASH')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE data IN ('aaa', 'ccc')", tableName);
    assertThat(result).extracting(r -> r[0]).containsExactlyInAnyOrder(1L, 3L);

    // A mix of present and absent values -- must return only the present one.
    List<Object[]> mixed =
        sql("SELECT id FROM %s WHERE data IN ('bbb', 'does-not-exist')", tableName);
    assertThat(mixed).extracting(r -> r[0]).containsExactly(2L);

    // No values present at all.
    List<Object[]> none =
        sql("SELECT id FROM %s WHERE data IN ('does-not-exist-1', 'does-not-exist-2')", tableName);
    assertThat(none).isEmpty();
  }

  @TestTemplate
  public void testInPredicateResolvesViaIdentityIndex() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (5, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (10, 'c')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
            + " transform => 'IDENTITY')",
        catalogName, tableIdent);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE id IN (1, 10)", tableName);
    assertThat(result).extracting(r -> r[0]).containsExactlyInAnyOrder(1L, 10L);
  }

  @TestTemplate
  public void testPlanningCostBoundFallsBackSafely() {
    // Forces a query to resolve to more candidate leaf files than an aggressively low
    // scalar-index.max-candidate-leaf-files bound allows, and confirms correctness is preserved
    // via the normal fallback path regardless -- this doesn't prove the bound is what caused the
    // fallback (a correct result looks the same either way from SQL), only that setting one
    // doesn't break anything.
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (300, 'z')", tableName);

    sql(
        "CALL %s.system.build_scalar_index(table => '%s', columns => array('id'),"
            + " transform => 'IDENTITY', options => map('target-leaf-files', '2'))",
        catalogName, tableIdent);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('scalar-index.max-candidate-leaf-files' = '1')",
        tableName);

    List<Object[]> result = sql("SELECT id, data FROM %s WHERE id IN (1, 300)", tableName);
    assertThat(result).extracting(r -> r[0]).containsExactlyInAnyOrder(1L, 300L);
  }
}
