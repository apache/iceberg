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

import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for Spark SQL Default values integration with Iceberg default values.
 *
 * <p>Note: These tests use {@code validationCatalog.createTable()} to create tables with default
 * values because the Iceberg Spark integration does not yet support default value clauses in Spark
 * DDL.
 *
 * <p>Partial column INSERT statements (e.g., {@code INSERT INTO table (col1) VALUES (val1)}) are
 * not supported for DSV2 in Spark 4.0
 */
@ExtendWith(ParameterizedTestExtension.class)
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
  public void testCreateTableWithDefaultsUnsupported() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    assertThatThrownBy(
            () ->
                sql(
                    "CREATE TABLE %s (id INT, data STRING DEFAULT 'default-value') USING iceberg",
                    tableName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("does not support column default value");
  }

  @TestTemplate
  public void testAlterTableAddColumnWithDefaultUnsupported() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    assertThatThrownBy(
            () -> sql("ALTER TABLE %s ADD COLUMN data STRING DEFAULT 'default-value'", tableName))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("default values in Spark is currently unsupported");
  }

  @TestTemplate
  public void testPartialInsertUnsupported() {
    assertThat(validationCatalog.tableExists(tableIdent))
        .as("Table should not already exist")
        .isFalse();

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional("data")
                .withId(2)
                .ofType(Types.StringType.get())
                .withWriteDefault(Literal.of("default-data"))
                .build());

    validationCatalog.createTable(
        tableIdent, schema, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", "3"));

    assertThatThrownBy(() -> sql("INSERT INTO %s (id) VALUES (1)", commitTarget()))
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Cannot find data for the output column");
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
