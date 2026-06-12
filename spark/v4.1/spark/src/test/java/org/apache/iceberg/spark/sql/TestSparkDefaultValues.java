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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.SparkException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

/**
 * Tests for Spark SQL Default values integration with Iceberg default values.
 *
 * <p>These tests cover Spark SQL DEFAULT writes against Iceberg defaults and Spark SQL DDL default
 * clauses that create or update Iceberg defaults.
 */
public class TestSparkDefaultValues extends CatalogTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
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
        "CREATE TABLE %s (id INT, data STRING DEFAULT 'default-value') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault()).isEqualTo("default-value");
    assertThat(table.schema().findField("data").writeDefault()).isEqualTo("default-value");

    sql("INSERT INTO %s VALUES (1, DEFAULT)", commitTarget());

    assertEquals(
        "Should use default value from CREATE TABLE DDL",
        ImmutableList.of(row(1, "default-value")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testReplaceTableWithDefaults() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    sql(
        "REPLACE TABLE %s (id INT, data STRING DEFAULT 'replace-default') USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault()).isEqualTo("replace-default");
    assertThat(table.schema().findField("data").writeDefault()).isEqualTo("replace-default");

    sql("INSERT INTO %s VALUES (1, DEFAULT)", commitTarget());

    assertEquals(
        "Should use default value from REPLACE TABLE DDL",
        ImmutableList.of(row(1, "replace-default")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testCreateOrReplaceTableWithDefaults() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    sql(
        "CREATE OR REPLACE TABLE %s (id INT, data STRING DEFAULT 'create-or-replace-default') "
            + "USING iceberg TBLPROPERTIES ('format-version'='3')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault())
        .isEqualTo("create-or-replace-default");
    assertThat(table.schema().findField("data").writeDefault())
        .isEqualTo("create-or-replace-default");

    sql("INSERT INTO %s VALUES (1, DEFAULT)", commitTarget());

    assertEquals(
        "Should use default value from CREATE OR REPLACE TABLE DDL",
        ImmutableList.of(row(1, "create-or-replace-default")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testAlterTableAddColumnWithDefault() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    sql("INSERT INTO %s VALUES (1), (2)", commitTarget());

    sql("ALTER TABLE %s ADD COLUMN data STRING DEFAULT 'default-value'", tableName);
    sql("REFRESH TABLE %s", commitTarget());
    sql("INSERT INTO %s VALUES (3, DEFAULT)", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault()).isEqualTo("default-value");
    assertThat(table.schema().findField("data").writeDefault()).isEqualTo("default-value");

    assertEquals(
        "Should use default value for existing and new rows",
        ImmutableList.of(row(1, "default-value"), row(2, "default-value"), row(3, "default-value")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testAlterTableSetAndDropColumnDefault() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withInitialDefault(Literal.of("initial-default"))
                .withWriteDefault(Literal.of("old-default"))
                .build());

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    sql("ALTER TABLE %s ALTER COLUMN data SET DEFAULT 'new-default'", tableName);
    sql("REFRESH TABLE %s", commitTarget());
    sql("INSERT INTO %s VALUES (1, DEFAULT)", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault()).isEqualTo("initial-default");
    assertThat(table.schema().findField("data").writeDefault()).isEqualTo("new-default");

    assertEquals(
        "Should use updated write default",
        ImmutableList.of(row(1, "new-default")),
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    sql("ALTER TABLE %s ALTER COLUMN data DROP DEFAULT", tableName);
    sql("REFRESH TABLE %s", commitTarget());

    Table updatedTable = validationCatalog.loadTable(tableIdent);
    assertThat(updatedTable.schema().findField("data").initialDefault())
        .isEqualTo("initial-default");
    assertThat(updatedTable.schema().findField("data").writeDefault()).isNull();
  }

  @TestTemplate
  public void testCreateTableDefaultValuesForPrimitiveTypes() {
    List<DefaultColumn> columns = Lists.newArrayList();
    columns.add(new DefaultColumn("bool_col", "BOOLEAN", "true", true));
    columns.add(new DefaultColumn("int_col", "INT", "42", 42));
    columns.add(new DefaultColumn("long_col", "BIGINT", "100L", 100L));
    columns.add(new DefaultColumn("float_col", "FLOAT", "CAST('3.14' AS FLOAT)", 3.14F));
    columns.add(new DefaultColumn("double_col", "DOUBLE", "2.718D", 2.718D));
    columns.add(
        new DefaultColumn("decimal_col", "DECIMAL(10, 2)", "99.99BD", new BigDecimal("99.99")));
    columns.add(new DefaultColumn("string_col", "STRING", "'default-value'", "default-value"));
    columns.add(
        new DefaultColumn(
            "date_col",
            "DATE",
            "DATE '2024-01-01'",
            Literal.of("2024-01-01").to(Types.DateType.get()).value()));
    columns.add(
        new DefaultColumn(
            "timestamp_col",
            "TIMESTAMP",
            "TIMESTAMP '2026-06-07 06:30:46.619896 UTC+00:00'",
            Literal.of("2026-06-07T06:30:46.619896+00:00")
                .to(Types.TimestampType.withZone())
                .value()));
    columns.add(
        new DefaultColumn(
            "timestamp_ntz_col",
            "TIMESTAMP_NTZ",
            "TIMESTAMP_NTZ '2026-06-07 06:30:46.619896'",
            Literal.of("2026-06-07T06:30:46.619896")
                .to(Types.TimestampType.withoutZone())
                .value()));
    columns.add(
        new DefaultColumn(
            "binary_col", "BINARY", "X'0A0B'", ByteBuffer.wrap(new byte[] {0x0a, 0x0b})));

    StringBuilder createTable = new StringBuilder("CREATE TABLE ");
    createTable.append(tableName).append(" (id INT");
    for (DefaultColumn column : columns) {
      createTable
          .append(", ")
          .append(column.name)
          .append(" ")
          .append(column.sqlType)
          .append(" DEFAULT ")
          .append(column.defaultSql);
    }

    createTable.append(") USING iceberg TBLPROPERTIES ('format-version'='3')");

    sql(createTable.toString());

    Table table = validationCatalog.loadTable(tableIdent);
    for (DefaultColumn column : columns) {
      assertThat(table.schema().findField(column.name).initialDefault())
          .as("Initial default for %s", column.name)
          .isEqualTo(column.expectedValue);
      assertThat(table.schema().findField(column.name).writeDefault())
          .as("Write default for %s", column.name)
          .isEqualTo(column.expectedValue);
    }
  }

  @TestTemplate
  public void testCreateTableUnsupportedDefaultTypesFailClearly() {
    for (DefaultColumn column : unsupportedDefaultColumns()) {
      assertThatThrownBy(
              () ->
                  sql(
                      "CREATE TABLE %s (id INT, %s %s DEFAULT %s) USING iceberg "
                          + "TBLPROPERTIES ('format-version'='3')",
                      tableName, column.name, column.sqlType, column.defaultSql))
          .as("Should reject default for unsupported type %s", column.sqlType)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Column default")
          .hasMessageContaining("not supported by Iceberg Spark DDL");
    }
  }

  @TestTemplate
  public void testAlterTableAddColumnUnsupportedDefaultTypesFailClearly() {
    sql("CREATE TABLE %s (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')", tableName);

    for (DefaultColumn column : unsupportedDefaultColumns()) {
      assertThatThrownBy(
              () ->
                  sql(
                      "ALTER TABLE %s ADD COLUMN %s %s DEFAULT %s",
                      tableName, column.name, column.sqlType, column.defaultSql))
          .as("Should reject default for unsupported type %s", column.sqlType)
          .isInstanceOf(SparkException.class)
          .hasMessageContaining("Column default")
          .hasMessageContaining("not supported by Iceberg Spark DDL");
    }
  }

  @TestTemplate
  public void testCreateTableWithNullDefault() {
    sql(
        "CREATE TABLE %s (id INT, data STRING DEFAULT NULL) USING iceberg "
            + "TBLPROPERTIES ('format-version'='3')",
        tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.schema().findField("data").initialDefault()).isNull();
    assertThat(table.schema().findField("data").writeDefault()).isNull();
  }

  private static List<DefaultColumn> unsupportedDefaultColumns() {
    List<DefaultColumn> unsupportedColumns = Lists.newArrayList();
    unsupportedColumns.add(new DefaultColumn("array_col", "ARRAY<INT>", "array(1, 2)", null));
    unsupportedColumns.add(new DefaultColumn("map_col", "MAP<INT, INT>", "map(1, 2)", null));
    unsupportedColumns.add(
        new DefaultColumn("struct_col", "STRUCT<a: INT>", "named_struct('a', 1)", null));
    return unsupportedColumns;
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

  private static class DefaultColumn {
    private final String name;
    private final String sqlType;
    private final String defaultSql;
    private final Object expectedValue;

    DefaultColumn(String name, String sqlType, String defaultSql, Object expectedValue) {
      this.name = name;
      this.sqlType = sqlType;
      this.defaultSql = defaultSql;
      this.expectedValue = expectedValue;
    }
  }
}
