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

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestAlterTableSchema extends ExtensionsTestBase {

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSetIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().identifierFieldIds())
        .as("Table should start without identifier")
        .isEmpty();

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have new identifier field")
        .isEqualTo(Sets.newHashSet(table.schema().findField("id").fieldId()));

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have new identifier field")
        .isEqualTo(
            Sets.newHashSet(
                table.schema().findField("id").fieldId(),
                table.schema().findField("location.lon").fieldId()));

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS location.lon", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have new identifier field")
        .isEqualTo(Sets.newHashSet(table.schema().findField("location.lon").fieldId()));
  }

  @TestTemplate
  public void testSetInvalidIdentifierFields() {
    sql("CREATE TABLE %s (id bigint NOT NULL, id2 bigint) USING iceberg", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().identifierFieldIds())
        .as("Table should start without identifier")
        .isEmpty();
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s SET IDENTIFIER FIELDS unknown", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("not found in current schema or added columns");

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s SET IDENTIFIER FIELDS id2", tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageEndingWith("not a required field");
  }

  @TestTemplate
  public void testDropIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().identifierFieldIds())
        .as("Table should start without identifier")
        .isEmpty();

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have new identifier fields")
        .isEqualTo(
            Sets.newHashSet(
                table.schema().findField("id").fieldId(),
                table.schema().findField("location.lon").fieldId()));

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS id", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should removed identifier field")
        .isEqualTo(Sets.newHashSet(table.schema().findField("location.lon").fieldId()));

    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have new identifier fields")
        .isEqualTo(
            Sets.newHashSet(
                table.schema().findField("id").fieldId(),
                table.schema().findField("location.lon").fieldId()));

    sql("ALTER TABLE %s DROP IDENTIFIER FIELDS id, location.lon", tableName);
    table.refresh();
    assertThat(table.schema().identifierFieldIds())
        .as("Should have no identifier field")
        .isEqualTo(Sets.newHashSet());
  }

  @TestTemplate
  public void testDropInvalidIdentifierFields() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string NOT NULL, "
            + "location struct<lon:bigint NOT NULL,lat:bigint NOT NULL> NOT NULL) USING iceberg",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().identifierFieldIds())
        .as("Table should start without identifier")
        .isEmpty();
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
}
