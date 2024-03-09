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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Test;

public class TestAncestorsOfProcedure extends SparkExtensionsTestBase {

  public TestAncestorsOfProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testAncestorOfUsingEmptyArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Long currentSnapshotId = table.currentSnapshot().snapshotId();
    Long currentTimestamp = table.currentSnapshot().timestampMillis();
    Long preSnapshotId = table.currentSnapshot().parentId();
    Long preTimeStamp = table.snapshot(table.currentSnapshot().parentId()).timestampMillis();

    List<Object[]> output = sql("CALL %s.system.ancestors_of('%s')", catalogName, tableIdent);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(
            row(currentSnapshotId, currentTimestamp), row(preSnapshotId, preTimeStamp)),
        output);
  }

  @Test
  public void testAncestorOfUsingSnapshotId() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Long currentSnapshotId = table.currentSnapshot().snapshotId();
    Long currentTimestamp = table.currentSnapshot().timestampMillis();
    Long preSnapshotId = table.currentSnapshot().parentId();
    Long preTimeStamp = table.snapshot(table.currentSnapshot().parentId()).timestampMillis();

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(
            row(currentSnapshotId, currentTimestamp), row(preSnapshotId, preTimeStamp)),
        sql("CALL %s.system.ancestors_of('%s', %dL)", catalogName, tableIdent, currentSnapshotId));

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(preSnapshotId, preTimeStamp)),
        sql("CALL %s.system.ancestors_of('%s', %dL)", catalogName, tableIdent, preSnapshotId));
  }

  @Test
  public void testAncestorOfWithRollBack() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    table.refresh();
    Long firstSnapshotId = table.currentSnapshot().snapshotId();
    Long firstTimestamp = table.currentSnapshot().timestampMillis();
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    table.refresh();
    Long secondSnapshotId = table.currentSnapshot().snapshotId();
    Long secondTimestamp = table.currentSnapshot().timestampMillis();
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    table.refresh();
    Long thirdSnapshotId = table.currentSnapshot().snapshotId();
    Long thirdTimestamp = table.currentSnapshot().timestampMillis();

    // roll back
    sql(
        "CALL %s.system.rollback_to_snapshot('%s', %dL)",
        catalogName, tableIdent, secondSnapshotId);

    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);
    table.refresh();
    Long fourthSnapshotId = table.currentSnapshot().snapshotId();
    Long fourthTimestamp = table.currentSnapshot().timestampMillis();

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(
            row(fourthSnapshotId, fourthTimestamp),
            row(secondSnapshotId, secondTimestamp),
            row(firstSnapshotId, firstTimestamp)),
        sql("CALL %s.system.ancestors_of('%s', %dL)", catalogName, tableIdent, fourthSnapshotId));

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(
            row(thirdSnapshotId, thirdTimestamp),
            row(secondSnapshotId, secondTimestamp),
            row(firstSnapshotId, firstTimestamp)),
        sql("CALL %s.system.ancestors_of('%s', %dL)", catalogName, tableIdent, thirdSnapshotId));
  }

  @Test
  public void testAncestorOfUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Long firstSnapshotId = table.currentSnapshot().snapshotId();
    Long firstTimestamp = table.currentSnapshot().timestampMillis();

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(firstSnapshotId, firstTimestamp)),
        sql(
            "CALL %s.system.ancestors_of(snapshot_id => %dL, table => '%s')",
            catalogName, firstSnapshotId, tableIdent));
  }

  @Test
  public void testInvalidAncestorOfCases() {
    AssertHelpers.assertThrows(
        "Should reject calls without all required args",
        AnalysisException.class,
        "Missing required parameters",
        () -> sql("CALL %s.system.ancestors_of()", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with empty table identifier",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier for parameter 'table'",
        () -> sql("CALL %s.system.ancestors_of('')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with invalid arg types",
        AnalysisException.class,
        "Wrong arg type for snapshot_id: cannot cast",
        () -> sql("CALL %s.system.ancestors_of('%s', 1.1)", catalogName, tableIdent));
  }
}
