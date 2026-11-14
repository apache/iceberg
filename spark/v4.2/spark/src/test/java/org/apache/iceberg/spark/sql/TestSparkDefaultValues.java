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
package org.apache.iceberg.spark.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HexFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/** Tests for Spark SQL Default values integration with Iceberg default values. */
public class TestSparkDefaultValues extends CatalogTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", tableName("dest"));
  }

  @TestTemplate
  public void testWriteDefaultWithSparkDefaultKeyword() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional("bool_col")
                .withId(2)
                .ofType(Types.BooleanType.get())
                .withWriteDefault(Literal.of(true))
                .build(),
            Types.NestedField.optional("int_col")
                .withId(3)
                .ofType(Types.IntegerType.get())
                .withWriteDefault(Literal.of(42))
                .build(),
            Types.NestedField.optional("long_col")
                .withId(4)
                .ofType(Types.LongType.get())
                .withWriteDefault(Literal.of(100L))
                .build());

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    sql("INSERT INTO %s VALUES (1, DEFAULT, DEFAULT, DEFAULT)", commitTarget());

    assertEquals(
        "Should have expected default values",
        ImmutableList.of(row(1, true, 42, 100L)),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testWriteDefaultWithDefaultKeywordAndReorderedSchema() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional("int_col")
                .withId(2)
                .ofType(Types.IntegerType.get())
                .withWriteDefault(Literal.of(123))
                .build(),
            Types.NestedField.optional("string_col")
                .withId(3)
                .ofType(Types.StringType.get())
                .withWriteDefault(Literal.of("doom"))
                .build());

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    // Insert with columns in different order than table schema
    sql("INSERT INTO %s (int_col, id, string_col) VALUES (DEFAULT, 1, DEFAULT)", commitTarget());

    assertEquals(
        "Should apply correct defaults regardless of column order",
        ImmutableList.of(row(1, 123, "doom")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testBulkInsertWithDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withWriteDefault(Literal.of("default_data"))
                .build());

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    sql("INSERT INTO %s VALUES (1, DEFAULT), (2, DEFAULT), (3, DEFAULT)", commitTarget());

    assertEquals(
        "Should insert multiple rows with default values",
        ImmutableList.of(row(1, "default_data"), row(2, "default_data"), row(3, "default_data")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testCreateTableWithDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (id INT DEFAULT -1, data STRING DEFAULT '-') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Types.NestedField idField = validationCatalog.loadTable(tableIdent).schema().findField("id");
    assertThat(idField.initialDefault()).isEqualTo(-1);
    assertThat(idField.writeDefault()).isEqualTo(-1);

    Types.NestedField dataField =
        validationCatalog.loadTable(tableIdent).schema().findField("data");
    assertThat(dataField.initialDefault()).isEqualTo("-");
    assertThat(dataField.writeDefault()).isEqualTo("-");
  }

  @TestTemplate
  public void testCreateTableWithBoxedDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (ti TINYINT DEFAULT -1, s SHORT DEFAULT '-2') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Types.NestedField tinyField = validationCatalog.loadTable(tableIdent).schema().findField("ti");
    assertThat(tinyField.initialDefault()).isEqualTo(-1);
    assertThat(tinyField.writeDefault()).isEqualTo(-1);

    Types.NestedField shortField = validationCatalog.loadTable(tableIdent).schema().findField("s");
    assertThat(shortField.initialDefault()).isEqualTo(-2);
    assertThat(shortField.writeDefault()).isEqualTo(-2);
  }

  @TestTemplate
  public void testCreateTableWithBinaryDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (b BINARY DEFAULT X'1ABF') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    ByteBuffer expected = ByteBuffer.wrap(HexFormat.of().parseHex("1ABF"));

    Types.NestedField binaryField = validationCatalog.loadTable(tableIdent).schema().findField("b");
    assertThat(binaryField.initialDefault()).isEqualTo(expected);
    assertThat(binaryField.writeDefault()).isEqualTo(expected);
  }

  @TestTemplate
  public void testCreateTableWithTimestampDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (ts TIMESTAMP DEFAULT timestamp '2026-09-24 21:26:02.98269') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    long expectedMicros =
        DateTimeUtil.microsFromInstant(
            LocalDateTime.parse("2026-09-24T21:26:02.98269")
                .atZone(ZoneId.of(spark.sessionState().conf().sessionLocalTimeZone()))
                .toInstant());

    Types.NestedField tsField = validationCatalog.loadTable(tableIdent).schema().findField("ts");
    assertThat(tsField.initialDefault()).isEqualTo(expectedMicros);
    assertThat(tsField.writeDefault()).isEqualTo(expectedMicros);
  }

  @TestTemplate
  public void testCreateTableWithNullDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (s String DEFAULT null) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Types.NestedField stringField = validationCatalog.loadTable(tableIdent).schema().findField("s");
    assertThat(stringField.initialDefault()).isNull();
    assertThat(stringField.writeDefault()).isNull();
  }

  @TestTemplate
  public void testCreateTableWithDefaultsExpressionsUnsupported() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s (id INT, data STRING DEFAULT CURRENT_USER) USING iceberg",
                    tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Unsupported default value expression: CURRENT_USER");
  }

  @TestTemplate
  public void testDefaultValuesAfterCoercionAndFolding() {
    sql(
        "CREATE TABLE %s (id BIGINT DEFAULT 1, data INT DEFAULT 1 + 1) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Types.NestedField idField = validationCatalog.loadTable(tableIdent).schema().findField("id");
    assertThat(idField.initialDefault()).isEqualTo(1L);
    assertThat(idField.writeDefault()).isEqualTo(1L);

    Types.NestedField dataField =
        validationCatalog.loadTable(tableIdent).schema().findField("data");
    assertThat(dataField.initialDefault()).isEqualTo(2);
    assertThat(dataField.writeDefault()).isEqualTo(2);
  }

  @TestTemplate
  public void testAddColumnDefaultWithMismatchedValues() {
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Table table =
        validationCatalog.createTable(
            tableIdent,
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of("format-version", "3"));

    ColumnDefaultValue defaultValue =
        new ColumnDefaultValue(
            org.apache.spark.sql.connector.expressions.Expressions.literal("default-value"), null);
    TableChange change =
        TableChange.addColumn(
            new String[] {"data"}, DataTypes.StringType, true, null, null, defaultValue);

    assertThatThrownBy(
            () -> {
              UpdateSchema update = table.updateSchema();
              Spark3Util.applySchemaChanges(update, ImmutableList.of(change));
            })
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("initial and write defaults must match");
  }

  @TestTemplate
  public void testAlterTableAddColumnWithDefault() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql("CREATE TABLE %s (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')", tableName);
    sql("INSERT INTO %s VALUES (1)", tableName);

    sql("ALTER TABLE %s ADD COLUMN data STRING DEFAULT 'default-value'", tableName);

    Types.NestedField field = validationCatalog.loadTable(tableIdent).schema().findField("data");
    assertThat(field.initialDefault()).isEqualTo("default-value");
    assertThat(field.writeDefault()).isEqualTo("default-value");

    sql("INSERT INTO %s (id) VALUES (2)", tableName);

    assertEquals(
        "Should use the default for existing and new rows",
        ImmutableList.of(row(1, "default-value"), row(2, "default-value")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testAlterTableAddColumnWithTypedDefaults() {
    sql("CREATE TABLE %s (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')", tableName);
    sql("INSERT INTO %s VALUES (1)", tableName);

    sql(
        "ALTER TABLE %s ADD COLUMNS "
            + "(int_col INT DEFAULT 34, date_col DATE DEFAULT DATE '2026-10-29')",
        tableName);

    Types.NestedField intField =
        validationCatalog.loadTable(tableIdent).schema().findField("int_col");
    assertThat(intField.initialDefault()).isEqualTo(34);
    assertThat(intField.writeDefault()).isEqualTo(34);

    Types.NestedField dateField =
        validationCatalog.loadTable(tableIdent).schema().findField("date_col");
    int expectedDays = DateTimeUtil.daysFromDate(LocalDate.of(2026, 10, 29));
    assertThat(dateField.initialDefault()).isEqualTo(expectedDays);
    assertThat(dateField.writeDefault()).isEqualTo(expectedDays);

    sql("INSERT INTO %s (id) VALUES (2)", tableName);

    assertEquals(
        "Should apply typed defaults to existing and new rows",
        ImmutableList.of(row(1, 34, "2026-10-29"), row(2, 34, "2026-10-29")),
        sql("SELECT id, int_col, CAST(date_col AS STRING) FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testAlterColumnDefaultKeepsRowsWrittenWithPreviousDefault() {
    sql("CREATE TABLE %s (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')", tableName);
    sql("INSERT INTO %s VALUES (1)", tableName);

    sql("ALTER TABLE %s ADD COLUMN data STRING DEFAULT 'initial'", tableName);
    sql("INSERT INTO %s (id) VALUES (2)", tableName);

    sql("ALTER TABLE %s ALTER COLUMN data SET DEFAULT 'updated'", tableName);
    sql("INSERT INTO %s (id) VALUES (3)", tableName);

    assertEquals(
        "Should keep rows written with the previous default",
        ImmutableList.of(row(1, "initial"), row(2, "initial"), row(3, "updated")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testAlterColumnDefaultChangesOnlyNewRows() {
    sql("CREATE TABLE %s (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')", tableName);
    sql("INSERT INTO %s VALUES (1)", tableName);

    sql("ALTER TABLE %s ADD COLUMN data STRING DEFAULT 'initial'", tableName);
    sql("ALTER TABLE %s ALTER COLUMN data SET DEFAULT 'updated'", tableName);

    Types.NestedField updated = validationCatalog.loadTable(tableIdent).schema().findField("data");
    assertThat(updated.initialDefault()).isEqualTo("initial");
    assertThat(updated.writeDefault()).isEqualTo("updated");

    sql("INSERT INTO %s (id) VALUES (2)", tableName);

    assertEquals(
        "Should apply the new default to new rows only",
        ImmutableList.of(row(1, "initial"), row(2, "updated")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("ALTER TABLE %s ALTER COLUMN data DROP DEFAULT", tableName);
    sql("INSERT INTO %s (id) VALUES (3)", tableName);

    Types.NestedField dropped = validationCatalog.loadTable(tableIdent).schema().findField("data");
    assertThat(dropped.initialDefault()).isEqualTo("initial");
    assertThat(dropped.writeDefault()).isNull();

    assertEquals(
        "Should write null after dropping the default",
        ImmutableList.of(row(1, "initial"), row(2, "updated"), row(3, null)),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testCtasExcludesDefault() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (data STRING DEFAULT '-') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    sql("INSERT INTO %s VALUES (DEFAULT)", tableName);

    sql("CREATE TABLE %s USING iceberg AS SELECT * FROM %s", tableName("dest"), tableName);

    TableIdentifier destIdent = TableIdentifier.of(Namespace.of("default"), "dest");
    Types.NestedField dataField = validationCatalog.loadTable(destIdent).schema().findField("data");
    assertThat(dataField.initialDefault()).isNull();
    assertThat(dataField.writeDefault()).isNull();

    assertEquals(
        "Should have correct default values from source table",
        ImmutableList.of(row("-")),
        sql("SELECT * FROM %s", tableName("dest")));
  }

  @TestTemplate
  public void testRtasExcludesDefault() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE TABLE %s (data STRING DEFAULT '-') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    sql("INSERT INTO %s VALUES (DEFAULT)", tableName);

    sql(
        "CREATE TABLE %s (data STRING DEFAULT '-') USING iceberg TBLPROPERTIES ('format-version'='3')",
        tableName("dest"));

    sql("REPLACE TABLE %s USING iceberg AS SELECT * FROM %s", tableName("dest"), tableName);

    TableIdentifier destIdent = TableIdentifier.of(Namespace.of("default"), "dest");
    Types.NestedField dataField = validationCatalog.loadTable(destIdent).schema().findField("data");
    assertThat(dataField.initialDefault()).isNull();
    assertThat(dataField.writeDefault()).isNull();

    assertEquals(
        "Should have correct default values from source table",
        ImmutableList.of(row("-")),
        sql("SELECT * FROM %s", tableName("dest")));
  }

  @TestTemplate
  public void testSchemaEvolutionWithDefaultValueChanges() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema initialSchema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    validationCatalog.createTable(
        tableIdent,
        initialSchema,
        PartitionSpec.unpartitioned(),
        ImmutableMap.of("format-version", "3"));

    sql("INSERT INTO %s VALUES (1), (2)", commitTarget());

    // Add a column with a default value
    validationCatalog
        .loadTable(tableIdent)
        .updateSchema()
        .addColumn("data", Types.StringType.get(), Literal.of("default_data"))
        .commit();

    // Refresh this when using SparkCatalog since otherwise the new column would not be caught.
    sql("REFRESH TABLE %s", commitTarget());

    sql("INSERT INTO %s VALUES (3, DEFAULT)", commitTarget());

    assertEquals(
        "Should have correct default values for existing and new rows",
        ImmutableList.of(row(1, "default_data"), row(2, "default_data"), row(3, "default_data")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }
}
