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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Test;

public class TestFastForwardBranchProcedure extends SparkExtensionsTestBase {
  public TestFastForwardBranchProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testFastForwardBranchUsingPositionalArgs() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    table.refresh();

    Snapshot currSnapshot = table.currentSnapshot();
    long sourceRef = currSnapshot.snapshotId();

    String newBranch = "testBranch";
    String tableNameWithBranch = String.format("%s.branch_%s", tableName, newBranch);

    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, newBranch);
    sql("INSERT INTO TABLE %s VALUES(3,'c')", tableNameWithBranch);

    table.refresh();
    long updatedRef = table.snapshot(newBranch).snapshotId();

    assertEquals(
        "Main branch should not have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));

    assertEquals(
        "Test branch should have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c")),
        sql("SELECT * FROM %s order by id", tableNameWithBranch));

    List<Object[]> output =
        sql(
            "CALL %s.system.fast_forward('%s', '%s', '%s')",
            catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch);

    assertThat(Arrays.stream(output.get(0)).collect(Collectors.toList()).get(0))
        .isEqualTo(SnapshotRef.MAIN_BRANCH);

    assertThat(Arrays.stream(output.get(0)).collect(Collectors.toList()).get(1))
        .isEqualTo(sourceRef);

    assertThat(Arrays.stream(output.get(0)).collect(Collectors.toList()).get(2))
        .isEqualTo(updatedRef);

    assertEquals(
        "Main branch should have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c")),
        sql("SELECT * FROM %s order by id", tableName));
  }

  @Test
  public void testFastForwardBranchUsingNamedArgs() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    String newBranch = "testBranch";
    String tableNameWithBranch = String.format("%s.branch_%s", tableName, newBranch);

    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, newBranch);
    sql("INSERT INTO TABLE %s VALUES(3,'c')", tableNameWithBranch);

    assertEquals(
        "Main branch should not have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));

    assertEquals(
        "Test branch should have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c")),
        sql("SELECT * FROM %s order by id", tableNameWithBranch));

    List<Object[]> output =
        sql(
            "CALL %s.system.fast_forward(table => '%s', branch => '%s', to => '%s')",
            catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch);

    assertEquals(
        "Main branch should now have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c")),
        sql("SELECT * FROM %s order by id", tableName));
  }

  @Test
  public void testFastForwardWhenTargetIsNotAncestorFails() {
    sql("CREATE TABLE %s (id int NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    String newBranch = "testBranch";
    String tableNameWithBranch = String.format("%s.branch_%s", tableName, newBranch);

    sql("ALTER TABLE %s CREATE BRANCH %s", tableName, newBranch);
    sql("INSERT INTO TABLE %s VALUES(3,'c')", tableNameWithBranch);

    assertEquals(
        "Main branch should not have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));

    assertEquals(
        "Test branch should have the newly inserted record.",
        ImmutableList.of(row(1, "a"), row(2, "b"), row(3, "c")),
        sql("SELECT * FROM %s order by id", tableNameWithBranch));

    // Commit a snapshot on main to deviate the branches
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.fast_forward(table => '%s', branch => '%s', to => '%s')",
                    catalogName, tableIdent, SnapshotRef.MAIN_BRANCH, newBranch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot fast-forward: main is not an ancestor of testBranch");
  }

  @Test
  public void testInvalidFastForwardBranchCases() {
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.fast_forward('test_table', branch => 'main', to => 'newBranch')",
                    catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    assertThatThrownBy(
            () ->
                sql("CALL %s.custom.fast_forward('test_table', 'main', 'newBranch')", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.fast_forward not found");

    assertThatThrownBy(() -> sql("CALL %s.system.fast_forward('test_table', 'main')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [to]");

    assertThatThrownBy(
            () -> sql("CALL %s.system.fast_forward('', 'main', 'newBranch')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }
}
