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
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadOptions;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestAlterTableSchema extends SparkExtensionsTestBase {
  public TestAlterTableSchema(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue(
        "Table should start without identifier", table.schema().identifierFieldIds().isEmpty());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have new identifier field",
        Sets.newHashSet(table.schema().findField("id").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have new identifier field",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS location.lon", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have new identifier field",
        Sets.newHashSet(table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());
  }

  @Test
  public void testSetInvalidIdentifierFields() {
    sql("CREATE TABLE %s (id bigint NOT NULL, id2 bigint) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue(
        "Table should start without identifier", table.schema().identifierFieldIds().isEmpty());
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s SET IDENTIFIER FIELDS unknown", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("not found in current schema or added columns");

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s SET IDENTIFIER FIELDS id2", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("not a required field");
  }

  @Test
  public void testDropIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue(
        "Table should start without identifier", table.schema().identifierFieldIds().isEmpty());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have new identifier fields",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS id", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should removed identifier field",
        Sets.newHashSet(table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have new identifier fields",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(),
            table.schema().findField("location.lon").fieldId()),
        table.schema().identifierFieldIds());

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    Assert.assertEquals(
        "Should have no identifier field", Sets.newHashSet(), table.schema().identifierFieldIds());
  }

  @Test
  public void testDropInvalidIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertTrue(
        "Table should start without identifier", table.schema().identifierFieldIds().isEmpty());
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP IDENTIFIER FIELDS unknown", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot complete drop identifier fields operation: field unknown not found");

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id", tableName);
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP IDENTIFIER FIELDS data", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot complete drop identifier fields operation: data is not an identifier field");

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP IDENTIFIER FIELDS location.lon", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot complete drop identifier fields operation: location.lon is not an identifier field");
  }

  @Test
  public void testMakeColumnRequiredAndRead() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES(1, null), (2, '-')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot s1 = table.currentSnapshot();

    sql("DELETE FROM %s", tableName);

    table.refresh();
    table.updateSchema().requireColumn("id").commit();
    Assertions.assertThat(table.schema().findField("id").isRequired()).isTrue();

    Throwable thrown =
        Assertions.catchThrowable(() -> table.updateSchema().requireColumn("data").commit());
    Assertions.assertThat(thrown)
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot set a column to required: data, as it cannot be determined from "
                + "existing metadata metrics that it must not contain null values");

    Snapshot s2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view("
                + "table => '%s',"
                + "options => map('%s','%s','%s','%s'))",
            catalogName,
            tableName,
            SparkReadOptions.START_SNAPSHOT_ID,
            s1.snapshotId(),
            SparkReadOptions.END_SNAPSHOT_ID,
            s2.snapshotId());

    String viewName = (String) returns.get(0)[0];

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1L, null, ChangelogOperation.DELETE.name(), 0, s2.snapshotId()),
            row(2L, "-", ChangelogOperation.DELETE.name(), 0, s2.snapshotId())),
        sql("SELECT * FROM %s order by _change_ordinal, id", viewName));
  }
}
