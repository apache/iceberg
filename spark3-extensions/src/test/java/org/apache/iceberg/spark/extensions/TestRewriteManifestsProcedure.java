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
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED;

public class TestRewriteManifestsProcedure extends SparkExtensionsTestBase {

  public TestRewriteManifestsProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testRewriteManifestsInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    List<Object[]> output = sql(
        "CALL %s.system.rewrite_manifests('%s', '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name());
    assertEquals("Procedure output must match",
        ImmutableList.of(row(0, 0)),
        output);
  }

  @Test
  public void testRewriteLargeManifests() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Must have 1 manifest", 1, table.currentSnapshot().allManifests().size());

    sql("ALTER TABLE %s SET TBLPROPERTIES ('commit.manifest.target-size-bytes' '1')", tableName);

    List<Object[]> output = sql(
        "CALL %s.system.rewrite_manifests('%s', '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name());
    assertEquals("Procedure output must match",
        ImmutableList.of(row(1, 4)),
        output);

    table.refresh();

    Assert.assertEquals("Must have 4 manifests", 4, table.currentSnapshot().allManifests().size());
  }

  @Test
  public void testRewriteSmallManifestsWithSnapshotIdInheritance() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')", tableName, SNAPSHOT_ID_INHERITANCE_ENABLED, "true");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Must have 4 manifest", 4, table.currentSnapshot().allManifests().size());

    List<Object[]> output = sql(
        "CALL %s.system.rewrite_manifests(table => '%s', namespace => '%s')",
        catalogName, tableIdent.name(), tableIdent.namespace());
    assertEquals("Procedure output must match",
        ImmutableList.of(row(4, 1)),
        output);

    table.refresh();

    Assert.assertEquals("Must have 1 manifests", 1, table.currentSnapshot().allManifests().size());
  }

  @Test
  public void testRewriteSmallManifestsWithoutCaching() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Must have 2 manifest", 2, table.currentSnapshot().allManifests().size());

    List<Object[]> output = sql(
        "CALL %s.system.rewrite_manifests(use_caching => false, namespace => '%s', table => '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name());
    assertEquals("Procedure output must match",
        ImmutableList.of(row(2, 1)),
        output);

    table.refresh();

    Assert.assertEquals("Must have 1 manifests", 1, table.currentSnapshot().allManifests().size());
  }

  @Test
  public void testRewriteManifestsCaseInsensitiveArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg PARTITIONED BY (data)", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Must have 2 manifest", 2, table.currentSnapshot().allManifests().size());

    List<Object[]> output = sql(
        "CALL %s.system.rewrite_manifests(usE_cAcHiNg => false, nAmeSpaCe => '%s', tAbLe => '%s')",
        catalogName, tableIdent.namespace(), tableIdent.name());
    assertEquals("Procedure output must match",
        ImmutableList.of(row(2, 1)),
        output);

    table.refresh();

    Assert.assertEquals("Must have 1 manifests", 1, table.currentSnapshot().allManifests().size());
  }

  @Test
  public void testInvalidRewriteManifestsCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.rewrite_manifests('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.rewrite_manifests('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.rewrite_manifests('n')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        RuntimeException.class, "Couldn't parse identifier",
        () -> sql("CALL %s.system.rewrite_manifests('n', 2.2)", catalogName));

    AssertHelpers.assertThrows("Should reject empty namespace",
        IllegalArgumentException.class, "Namespace cannot be empty",
        () -> sql("CALL %s.system.rewrite_manifests('', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject empty table name",
        IllegalArgumentException.class, "Table name cannot be empty",
        () -> sql("CALL %s.system.rewrite_manifests('n', '')", catalogName));

    AssertHelpers.assertThrows("Should reject duplicate arg names name",
        AnalysisException.class, "Duplicate procedure argument: table",
        () -> sql("CALL %s.system.rewrite_manifests(table => 't', tAbLe => 't')", catalogName));
  }
}
