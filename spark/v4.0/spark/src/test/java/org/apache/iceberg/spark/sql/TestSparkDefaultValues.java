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
 * DDL. See {@link #testCreateTableWithDefaultsNotYetSupported()} and {@link
 * #testAlterTableAddColumnWithDefaultNotYetSupported()} for verification that DDL with defaults
 * currently throws exceptions.
 *
 * <p>Partial column INSERT statements (e.g., {@code INSERT INTO table (col1) VALUES (val1)}) are
 * not supported for DSV2 in Spark 4.0 See {@link #testPartialInsertNotYetSupportedInSpark()} for
 * verification.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkDefaultValues extends CatalogTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testWriteDefaultWithExplicitDefault() {
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

    sql("INSERT INTO %s VALUES (1, DEFAULT)", commitTarget());

    assertEquals(
        "Should insert row with default values",
        ImmutableList.of(row(1, "default-data")),
        sql("SELECT * FROM %s", selectTarget()));
  }

  @TestTemplate
  public void testWriteDefaultForMultipleColumns() {
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
  public void testCreateTableWithDefaultsNotYetSupported() {
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
  public void testAlterTableAddColumnWithDefaultNotYetSupported() {
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
  public void testPartialInsertNotYetSupportedInSpark() {
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
}
