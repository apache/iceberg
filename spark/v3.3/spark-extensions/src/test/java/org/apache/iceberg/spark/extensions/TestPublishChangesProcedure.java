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

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Test;

public class TestPublishChangesProcedure extends SparkExtensionsTestBase {

  public TestPublishChangesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testApplyWapChangesUsingPositionalArgs() {
    String wapId = "wap_id_1";
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", wapId);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output =
        sql("CALL %s.system.publish_changes('%s', '%s')", catalogName, tableIdent, wapId);

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(wapSnapshot.snapshotId(), currentSnapshot.snapshotId())),
        output);

    assertEquals(
        "Apply of WAP changes must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testApplyWapChangesUsingNamedArgs() {
    String wapId = "wap_id_1";
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", wapId);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output =
        sql(
            "CALL %s.system.publish_changes(wap_id => '%s', table => '%s')",
            catalogName, wapId, tableIdent);

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(wapSnapshot.snapshotId(), currentSnapshot.snapshotId())),
        output);

    assertEquals(
        "Apply of WAP changes must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testApplyWapChangesRefreshesRelationCache() {
    String wapId = "wap_id_1";
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals("View should not produce rows", ImmutableList.of(), sql("SELECT * FROM tmp"));

    spark.conf().set("spark.wap.id", wapId);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    sql("CALL %s.system.publish_changes('%s', '%s')", catalogName, tableIdent, wapId);

    assertEquals(
        "Apply of WAP changes should be visible",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM tmp"));

    sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testApplyInvalidWapId() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    assertThatThrownBy(
            () -> sql("CALL %s.system.publish_changes('%s', 'not_valid')", catalogName, tableIdent))
        .as("Should reject invalid wap id")
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot apply unknown WAP ID");
  }

  @Test
  public void testInvalidApplyWapChangesCases() {
    assertThatThrownBy(
            () ->
                sql("CALL %s.system.publish_changes('n', table => 't', 'not_valid')", catalogName))
        .as("Should not allow mixed args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Named and positional arguments cannot be mixed");

    assertThatThrownBy(
            () -> sql("CALL %s.custom.publish_changes('n', 't', 'not_valid')", catalogName))
        .as("Should not resolve procedures in arbitrary namespaces")
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessageContaining("not found");

    assertThatThrownBy(() -> sql("CALL %s.system.publish_changes('t')", catalogName))
        .as("Should reject calls without all required args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters");

    assertThatThrownBy(() -> sql("CALL %s.system.publish_changes('', 'not_valid')", catalogName))
        .as("Should reject calls with empty table identifier")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot handle an empty identifier");
  }
}
