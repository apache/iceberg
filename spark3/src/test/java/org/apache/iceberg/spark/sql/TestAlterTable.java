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

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.spark.SparkException;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestAlterTable extends SparkCatalogTestBase {
  private final TableIdentifier renamedIdent = TableIdentifier.of(Namespace.of("default"), "table2");

  public TestAlterTable(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s2", tableName);
  }

  @Test
  public void testAddColumnNotNull() {
    AssertHelpers.assertThrows("Should reject adding NOT NULL column",
        SparkException.class, "Incompatible change: cannot add required column",
        () -> sql("ALTER TABLE %s ADD COLUMN c3 INT NOT NULL", tableName));
  }

  @Test
  public void testAddColumn() {
    sql("ALTER TABLE %s ADD COLUMN point struct<x: double NOT NULL, y: double NOT NULL> AFTER id", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(3, "point", Types.StructType.of(
            NestedField.required(4, "x", Types.DoubleType.get()),
            NestedField.required(5, "y", Types.DoubleType.get())
        )),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());

    sql("ALTER TABLE %s ADD COLUMN point.z double COMMENT 'May be null' FIRST", tableName);

    Types.StructType expectedSchema2 = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(3, "point", Types.StructType.of(
            NestedField.optional(6, "z", Types.DoubleType.get(), "May be null"),
            NestedField.required(4, "x", Types.DoubleType.get()),
            NestedField.required(5, "y", Types.DoubleType.get())
        )),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema2, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testDropColumn() {
    sql("ALTER TABLE %s DROP COLUMN data", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testRenameColumn() {
    sql("ALTER TABLE %s RENAME COLUMN id TO row_id", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "row_id", Types.LongType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testAlterColumnComment() {
    sql("ALTER TABLE %s ALTER COLUMN id COMMENT 'Record id'", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get(), "Record id"),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testAlterColumnType() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count TYPE bigint", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "data", Types.StringType.get()),
        NestedField.optional(3, "count", Types.LongType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testAlterColumnDropNotNull() {
    sql("ALTER TABLE %s ALTER COLUMN id DROP NOT NULL", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.optional(1, "id", Types.LongType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testAlterColumnSetNotNull() {
    // no-op changes are allowed
    sql("ALTER TABLE %s ALTER COLUMN id SET NOT NULL", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());

    AssertHelpers.assertThrows("Should reject adding NOT NULL constraint to an optional column",
        AnalysisException.class, "Cannot change nullable column to non-nullable: data",
        () -> sql("ALTER TABLE %s ALTER COLUMN data SET NOT NULL", tableName));
  }

  @Test
  public void testAlterColumnPositionAfter() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count AFTER id", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(3, "count", Types.IntegerType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testAlterColumnPositionFirst() {
    sql("ALTER TABLE %s ADD COLUMN count int", tableName);
    sql("ALTER TABLE %s ALTER COLUMN count FIRST", tableName);

    Types.StructType expectedSchema = Types.StructType.of(
        NestedField.optional(3, "count", Types.IntegerType.get()),
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "data", Types.StringType.get()));

    Assert.assertEquals("Schema should match expected",
        expectedSchema, validationCatalog.loadTable(tableIdent).schema().asStruct());
  }

  @Test
  public void testTableRename() {
    Assume.assumeFalse("Hadoop catalog does not support rename", validationCatalog instanceof HadoopCatalog);

    Assert.assertTrue("Initial name should exist", validationCatalog.tableExists(tableIdent));
    Assert.assertFalse("New name should not exist", validationCatalog.tableExists(renamedIdent));

    sql("ALTER TABLE %s RENAME TO %s2", tableName, tableName);

    Assert.assertFalse("Initial name should not exist", validationCatalog.tableExists(tableIdent));
    Assert.assertTrue("New name should exist", validationCatalog.tableExists(renamedIdent));
  }

  @Test
  public void testSetTableProperties() {
    sql("ALTER TABLE %s SET TBLPROPERTIES ('prop'='value')", tableName);

    Assert.assertEquals("Should have the new table property",
        "value", validationCatalog.loadTable(tableIdent).properties().get("prop"));

    sql("ALTER TABLE %s UNSET TBLPROPERTIES ('prop')", tableName);

    Assert.assertNull("Should not have the removed table property",
        validationCatalog.loadTable(tableIdent).properties().get("prop"));
  }
}
