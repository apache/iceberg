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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotRefType;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestCreateTagProcedure extends SparkExtensionsTestBase {

  public TestCreateTagProcedure(String catalogName, String implementation,
      Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }


  @Test
  public void testCreateTagUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    String tagName = "customTag";

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    List<Object[]> output = sql(
        "CALL %s.system.create_tag('%s', %dL, '%s')",
        catalogName, tableIdent, snapshot.snapshotId(), tagName);

    Map<String, SnapshotRef> refs = ((BaseTable) table).operations().refresh().refs();
    Assert.assertTrue("Table metadata should contain customTag", refs.containsKey(tagName));
    SnapshotRef snapshotRef = refs.get(tagName);

    assertEquals("Procedure output must match",
        ImmutableList.of(row(snapshotRef.snapshotId(), SnapshotRefType.TAG.name())),
        output);
    Assert.assertEquals("Snapshot ref type must be TAG", SnapshotRefType.TAG, snapshotRef.type());
  }

  @Test
  public void testCreateTagUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    String tagName = "customTag";

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    List<Object[]> output = sql(
        "CALL %s.system.create_tag(table => '%s', snapshot_id => %dL, tag_name => '%s')",
        catalogName, tableIdent, snapshot.snapshotId(), tagName);

    Map<String, SnapshotRef> refs = ((BaseTable) table).operations().refresh().refs();
    Assert.assertTrue("Table metadata should contain customTag", refs.containsKey(tagName));
    SnapshotRef snapshotRef = refs.get(tagName);

    assertEquals("Procedure output must match",
        ImmutableList.of(row(snapshotRef.snapshotId(), SnapshotRefType.TAG.name())),
        output);
    Assert.assertEquals("Snapshot ref type must be TAG", SnapshotRefType.TAG, snapshotRef.type());
  }

  @Test
  public void testCreateTagWithoutExplicitCatalog() {
    Assume.assumeTrue("Working only with the session catalog", "spark_catalog".equals(catalogName));

    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = table.currentSnapshot();
    String tagName = "customTag";

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // use camel case intentionally to test case sensitivity
    List<Object[]> output = sql(
        "CALL SyStEm.cReATe_TaG('%s', %dL, '%s')",
        tableIdent, snapshot.snapshotId(), tagName);

    Map<String, SnapshotRef> refs = ((BaseTable) table).operations().refresh().refs();
    Assert.assertTrue("Table metadata should contain customTag", refs.containsKey(tagName));
    SnapshotRef snapshotRef = refs.get(tagName);

    assertEquals("Procedure output must match",
        ImmutableList.of(row(snapshotRef.snapshotId(), SnapshotRefType.TAG.name())),
        output);
    Assert.assertEquals("Snapshot ref type must be TAG", SnapshotRefType.TAG, snapshotRef.type());
  }

  @Test
  public void testCreateTagToInvalidSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    String tagName = "customTag";

    AssertHelpers.assertThrows("Should reject invalid snapshot id",
        ValidationException.class, "Cannot set customTag to unknown snapshot: -1",
        () -> sql("CALL %s.system.create_tag('%s', -1L, '%s')", catalogName, tableIdent, tagName));
  }

  @Test
  public void testInvalidCreateTagCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.create_tag(namespace => 'n1', table => 't', 1L, 'tag')",
            catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.create_tag('n', 1L, 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.create_tag('t')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.create_tag(1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.create_tag(snapshot_id => 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.create_tag(table => 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.create_tag(tag_name => 'tag')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type for snapshot_id: cannot cast",
        () -> sql("CALL %s.system.create_tag('t', 2.2, 'tag')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.create_tag('', 1L, 'tag')", catalogName));
  }
}
