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

import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestUndeleteProcedure extends ExtensionsTestBase {
  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testUndeleteColumnWithNoWritesDuringWindow() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int originalFieldId = table.schema().findField("data").fieldId();

    sql("ALTER TABLE %s DROP COLUMN data", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'data')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().findField("data").fieldId()).isEqualTo(originalFieldId);
    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(originalFieldId, updated.schema().schemaId(), false, false)),
        output);
    assertEquals(
        "Historical rows must be readable through the restored column",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testUndeleteColumnWithWritesDuringWindow() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int originalFieldId = table.schema().findField("data").fieldId();

    sql("ALTER TABLE %s DROP COLUMN data", tableName);
    sql("INSERT INTO TABLE %s VALUES (2)", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'data')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().findField("data").fieldId()).isEqualTo(originalFieldId);
    assertEquals(
        "Procedure output must report writes during the window",
        ImmutableList.of(row(originalFieldId, updated.schema().schemaId(), true, false)),
        output);
    assertEquals(
        "Rows written after the drop must have null in the restored column",
        ImmutableList.of(row(1L, "a"), row(2L, null)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testUndeleteRequiredColumnWithWritesDuringWindowFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, req string NOT NULL) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    long lastContainingSnapshotId =
        validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();

    sql("ALTER TABLE %s DROP COLUMN req", tableName);
    sql("INSERT INTO TABLE %s VALUES (2)", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int schemaIdAtCall = table.schema().schemaId();
    long snapshotIdAtCall = table.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () -> sql("CALL %s.system.undelete_column('%s', 'req')", catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot undelete required column req: snapshots newer than snapshot "
                + lastContainingSnapshotId
                + ", which was the last to contain the column, may contain rows without values."
                + " Only nullable columns or tables unchanged since the drop can be undeleted");

    Table after = validationCatalog.loadTable(tableIdent);
    assertThat(after.schema().findField("req")).isNull();
    assertThat(after.schema().schemaId()).isEqualTo(schemaIdAtCall);
    assertThat(after.currentSnapshot().snapshotId()).isEqualTo(snapshotIdAtCall);
  }

  @TestTemplate
  public void testUndeleteRequiredColumnWithoutWritesRestoresRequiredness() {
    sql("CREATE TABLE %s (id bigint NOT NULL, req string NOT NULL) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int originalFieldId = table.schema().findField("req").fieldId();

    sql("ALTER TABLE %s DROP COLUMN req", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'req')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().findField("req").isRequired()).isTrue();
    assertThat(updated.schema().findField("req").fieldId()).isEqualTo(originalFieldId);
    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(originalFieldId, updated.schema().schemaId(), false, false)),
        output);
    assertEquals(
        "Historical rows must be readable through the restored column",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testUndeleteUnknownColumnFails() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    assertThatThrownBy(
            () -> sql("CALL %s.system.undelete_column('%s', 'ghost')", catalogName, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot undelete column: no deleted column with that name: ghost");

    Table after = validationCatalog.loadTable(tableIdent);
    assertThat(after.schema().columns()).hasSize(2);
  }

  @TestTemplate
  public void testUndeleteRestoresLatestIncarnation() {
    sql("CREATE TABLE %s (id bigint NOT NULL, x string) USING iceberg", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int stringIncarnationId = table.schema().findField("x").fieldId();

    sql("ALTER TABLE %s DROP COLUMN x", tableName);
    sql("ALTER TABLE %s ADD COLUMN x int", tableName);

    int intIncarnationId =
        validationCatalog.loadTable(tableIdent).schema().findField("x").fieldId();
    assertThat(intIncarnationId).isNotEqualTo(stringIncarnationId);

    sql("ALTER TABLE %s DROP COLUMN x", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'x')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().findField("x").type()).isEqualTo(Types.IntegerType.get());
    assertThat(updated.schema().findField("x").fieldId()).isEqualTo(intIncarnationId);
    assertEquals(
        "Procedure output must restore the latest incarnation's field id",
        ImmutableList.of(row(intIncarnationId, updated.schema().schemaId(), false, false)),
        output);
  }

  @TestTemplate
  public void testUndeleteColumnReportsWasIdentifierTrue() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int originalFieldId = table.schema().findField("id").fieldId();
    table.updateSchema().setIdentifierFields("id").commit();
    // the pre-drop schema keeps the identifier mark, so the restore reports it
    table.updateSchema().setIdentifierFields().deleteColumn("id").commit();
    // java-side commits bypass the spark catalog cache, sync it before the procedure runs
    spark.catalog().refreshTable(tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'id')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().findField("id").fieldId()).isEqualTo(originalFieldId);
    assertThat(updated.schema().findField("id").isRequired()).isTrue();
    assertThat(updated.schema().identifierFieldIds()).isEmpty();
    assertEquals(
        "Procedure output must report that the restored column was an identifier",
        ImmutableList.of(row(originalFieldId, updated.schema().schemaId(), false, true)),
        output);
  }

  @TestTemplate
  public void testUndeleteColumnReportsWasIdentifierFalseAfterRemoval() {
    sql(
        "CREATE TABLE %s (pkey bigint NOT NULL, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("ALTER TABLE %s SET IDENTIFIER FIELDS pkey", tableName);
    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS pkey", tableName);
    // an intervening change keeps the unmarked schema distinct once identifiers are dropped
    sql("ALTER TABLE %s ADD COLUMN filler string", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int originalFieldId = table.schema().findField("pkey").fieldId();

    sql("ALTER TABLE %s DROP COLUMN pkey", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'pkey')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    assertThat(updated.schema().identifierFieldIds()).isEmpty();
    assertEquals(
        "Identifier status removed before the drop must not be reported",
        ImmutableList.of(row(originalFieldId, updated.schema().schemaId(), false, false)),
        output);
  }

  @TestTemplate
  public void testUndeleteColumnWasIdentifierTiesToWinningIncarnation() {
    sql(
        "CREATE TABLE %s (x bigint NOT NULL, data string) USING iceberg "
            + "TBLPROPERTIES ('format-version'='2')",
        tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("ALTER TABLE %s SET IDENTIFIER FIELDS x", tableName);
    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS x", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    int stringIncarnationId = table.schema().findField("x").fieldId();

    sql("ALTER TABLE %s DROP COLUMN x", tableName);
    sql("ALTER TABLE %s ADD COLUMN x int", tableName);
    sql("ALTER TABLE %s DROP COLUMN x", tableName);

    List<Object[]> output =
        sql("CALL %s.system.undelete_column('%s', 'x')", catalogName, tableIdent);

    Table updated = validationCatalog.loadTable(tableIdent);
    int intIncarnationId = updated.schema().findField("x").fieldId();
    assertThat(intIncarnationId).isNotEqualTo(stringIncarnationId);
    // only the winning incarnation counts: an identifier mark on the older one is irrelevant
    assertEquals(
        "was_identifier must follow the restored incarnation's field id",
        ImmutableList.of(row(intIncarnationId, updated.schema().schemaId(), true, false)),
        output);
  }
}
