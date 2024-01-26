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
package org.apache.iceberg.flink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.constraints.UniqueConstraint;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestFlinkCatalogTable extends CatalogTestBase {

  @Override
  @BeforeEach
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @AfterEach
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP TABLE IF EXISTS %s.tl2", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @TestTemplate
  public void testGetTable() {
    sql("CREATE TABLE tl(id BIGINT, strV STRING)");

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, "tl"));
    Schema iSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "strV", Types.StringType.get()));
    assertThat(table.schema().toString())
        .as("Should load the expected iceberg schema")
        .isEqualTo(iSchema.toString());
  }

  @TestTemplate
  public void testRenameTable() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support rename table").isFalse();
    final Schema tableSchema =
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(TableIdentifier.of(icebergNamespace, "tl"), tableSchema);
    sql("ALTER TABLE tl RENAME TO tl2");

    assertThatThrownBy(() -> getTableEnv().from("tl"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Table `tl` was not found.");

    Schema actualSchema = FlinkSchemaUtil.convert(getTableEnv().from("tl2").getSchema());
    assertThat(tableSchema.asStruct()).isEqualTo(actualSchema.asStruct());
  }

  @TestTemplate
  public void testCreateTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT)");

    Table table = table("tl");
    assertThat(table.schema().asStruct())
        .isEqualTo(
            new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct());
    CatalogTable catalogTable = catalogTable("tl");
    assertThat(catalogTable.getSchema())
        .isEqualTo(TableSchema.builder().field("id", DataTypes.BIGINT()).build());
  }

  @TestTemplate
  public void testCreateTableWithPrimaryKey() throws Exception {
    sql("CREATE TABLE tl(id BIGINT, data STRING, key STRING PRIMARY KEY NOT ENFORCED)");

    Table table = table("tl");
    assertThat(table.schema().identifierFieldIds())
        .as("Should have the expected row key.")
        .isEqualTo(Sets.newHashSet(table.schema().findField("key").fieldId()));
    CatalogTable catalogTable = catalogTable("tl");
    Optional<UniqueConstraint> uniqueConstraintOptional = catalogTable.getSchema().getPrimaryKey();
    assertThat(uniqueConstraintOptional).isPresent();
    assertThat(uniqueConstraintOptional.get().getColumns()).containsExactly("key");
  }

  @TestTemplate
  public void testCreateTableWithMultiColumnsInPrimaryKey() throws Exception {
    sql(
        "CREATE TABLE tl(id BIGINT, data STRING, CONSTRAINT pk_constraint PRIMARY KEY(data, id) NOT ENFORCED)");

    Table table = table("tl");
    assertThat(table.schema().identifierFieldIds())
        .as("Should have the expected RowKey")
        .isEqualTo(
            Sets.newHashSet(
                table.schema().findField("id").fieldId(),
                table.schema().findField("data").fieldId()));
    CatalogTable catalogTable = catalogTable("tl");
    Optional<UniqueConstraint> uniqueConstraintOptional = catalogTable.getSchema().getPrimaryKey();
    assertThat(uniqueConstraintOptional).isPresent();
    assertThat(uniqueConstraintOptional.get().getColumns()).containsExactly("id", "data");
  }

  @TestTemplate
  public void testCreateTableIfNotExists() {
    sql("CREATE TABLE tl(id BIGINT)");

    // Assert that table does exist.
    assertThat(table("tl")).isNotNull();

    sql("DROP TABLE tl");
    Assertions.assertThatThrownBy(() -> table("tl"))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: " + getFullQualifiedTableName("tl"));

    sql("CREATE TABLE IF NOT EXISTS tl(id BIGINT)");
    assertThat(table("tl").properties()).doesNotContainKey("key");

    table("tl").updateProperties().set("key", "value").commit();
    assertThat(table("tl").properties()).containsEntry("key", "value");

    sql("CREATE TABLE IF NOT EXISTS tl(id BIGINT)");
    assertThat(table("tl").properties()).containsEntry("key", "value");
  }

  @TestTemplate
  public void testCreateTableLike() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT)");
    sql("CREATE TABLE tl2 LIKE tl");

    Table table = table("tl2");
    assertThat(table.schema().asStruct())
        .isEqualTo(
            new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct());
    CatalogTable catalogTable = catalogTable("tl2");
    assertThat(catalogTable.getSchema())
        .isEqualTo(TableSchema.builder().field("id", DataTypes.BIGINT()).build());
  }

  @TestTemplate
  public void testCreateTableLocation() {
    assumeThat(isHadoopCatalog)
        .as("HadoopCatalog does not support creating table with location")
        .isFalse();
    sql("CREATE TABLE tl(id BIGINT) WITH ('location'='file:///tmp/location')");

    Table table = table("tl");
    assertThat(table.schema().asStruct())
        .isEqualTo(
            new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct());
    assertThat(table.location()).isEqualTo("file:///tmp/location");
  }

  @TestTemplate
  public void testCreatePartitionTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT, dt STRING) PARTITIONED BY(dt)");

    Table table = table("tl");
    assertThat(table.schema().asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    assertThat(table.spec())
        .isEqualTo(PartitionSpec.builderFor(table.schema()).identity("dt").build());
    CatalogTable catalogTable = catalogTable("tl");
    assertThat(catalogTable.getSchema())
        .isEqualTo(
            TableSchema.builder()
                .field("id", DataTypes.BIGINT())
                .field("dt", DataTypes.STRING())
                .build());
    assertThat(catalogTable.getPartitionKeys()).isEqualTo(Collections.singletonList("dt"));
  }

  @TestTemplate
  public void testCreateTableWithFormatV2ThroughTableProperty() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='2')");

    Table table = table("tl");
    assertThat(((BaseTable) table).operations().current().formatVersion()).isEqualTo(2);
  }

  @TestTemplate
  public void testUpgradeTableWithFormatV2ThroughTableProperty() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='1')");

    Table table = table("tl");
    TableOperations ops = ((BaseTable) table).operations();
    assertThat(ops.refresh().formatVersion())
        .as("should create table using format v1")
        .isEqualTo(1);
    sql("ALTER TABLE tl SET('format-version'='2')");
    assertThat(ops.refresh().formatVersion())
        .as("should update table to use format v2")
        .isEqualTo(2);
  }

  @TestTemplate
  public void testDowngradeTableToFormatV1ThroughTablePropertyFails() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='2')");

    Table table = table("tl");
    TableOperations ops = ((BaseTable) table).operations();
    assertThat(ops.refresh().formatVersion())
        .as("should create table using format v2")
        .isEqualTo(2);
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl SET('format-version'='1')"))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v2 table to v1");
  }

  @TestTemplate
  public void testLoadTransformPartitionTable() throws TableNotExistException {
    Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        schema,
        PartitionSpec.builderFor(schema).bucket("id", 100).build());

    CatalogTable catalogTable = catalogTable("tl");
    assertThat(catalogTable.getSchema())
        .isEqualTo(TableSchema.builder().field("id", DataTypes.BIGINT()).build());
    assertThat(catalogTable.getPartitionKeys()).isEmpty();
  }

  @TestTemplate
  public void testAlterTableProperties() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT) WITH ('oldK'='oldV')");
    Map<String, String> properties = Maps.newHashMap();
    properties.put("oldK", "oldV");

    // new
    sql("ALTER TABLE tl SET('newK'='newV')");
    properties.put("newK", "newV");
    assertThat(table("tl").properties()).containsAllEntriesOf(properties);

    // update old
    sql("ALTER TABLE tl SET('oldK'='oldV2')");
    properties.put("oldK", "oldV2");
    assertThat(table("tl").properties()).containsAllEntriesOf(properties);

    // remove property
    sql("ALTER TABLE tl RESET('oldK')");
    properties.remove("oldK");
    assertThat(table("tl").properties()).containsAllEntriesOf(properties);
  }

  @TestTemplate
  public void testAlterTableAddColumn() {
    sql("CREATE TABLE tl(id BIGINT)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct());
    sql("ALTER TABLE tl ADD (dt STRING)");
    Schema schemaAfter1 = table("tl").schema();
    assertThat(schemaAfter1.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    // Add multiple columns
    sql("ALTER TABLE tl ADD (col1 STRING, col2 BIGINT)");
    Schema schemaAfter2 = table("tl").schema();
    assertThat(schemaAfter2.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()),
                    Types.NestedField.optional(4, "col2", Types.LongType.get()))
                .asStruct());
    // Adding a required field should fail because Iceberg's SchemaUpdate does not allow
    // incompatible changes.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl ADD (pk STRING NOT NULL)"))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Incompatible change: cannot add required column: pk");

    // Adding an existing field should fail due to Flink's internal validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl ADD (id STRING)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Try to add a column `id` which already exists in the table.");
  }

  @TestTemplate
  public void testAlterTableDropColumn() {
    sql("CREATE TABLE tl(id BIGINT, dt STRING, col1 STRING, col2 BIGINT)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()),
                    Types.NestedField.optional(4, "col2", Types.LongType.get()))
                .asStruct());
    sql("ALTER TABLE tl DROP (dt)");
    Schema schemaAfter1 = table("tl").schema();
    assertThat(schemaAfter1.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()),
                    Types.NestedField.optional(4, "col2", Types.LongType.get()))
                .asStruct());
    // Drop multiple columns
    sql("ALTER TABLE tl DROP (col1, col2)");
    Schema schemaAfter2 = table("tl").schema();
    assertThat(schemaAfter2.asStruct())
        .isEqualTo(
            new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct());
    // Dropping an non-existing field should fail due to Flink's internal validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl DROP (foo)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("The column `foo` does not exist in the base table.");

    // Dropping an already-deleted field should fail due to Flink's internal validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl DROP (dt)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("The column `dt` does not exist in the base table.");
  }

  @TestTemplate
  public void testAlterTableModifyColumnName() {
    sql("CREATE TABLE tl(id BIGINT, dt STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    sql("ALTER TABLE tl RENAME dt TO data");
    Schema schemaAfter = table("tl").schema();
    assertThat(schemaAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "data", Types.StringType.get()))
                .asStruct());
  }

  @TestTemplate
  public void testAlterTableModifyColumnType() {
    sql("CREATE TABLE tl(id INTEGER, dt STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    // Promote type from Integer to Long
    sql("ALTER TABLE tl MODIFY (id BIGINT)");
    Schema schemaAfter = table("tl").schema();
    assertThat(schemaAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    // Type change that doesn't follow the type-promotion rule should fail due to Iceberg's
    // validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl MODIFY (dt INTEGER)"))
        .isInstanceOf(TableException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Cannot change column type: dt: string -> int");
  }

  @TestTemplate
  public void testAlterTableModifyColumnNullability() {
    sql("CREATE TABLE tl(id INTEGER NOT NULL, dt STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    // Changing nullability from optional to required should fail
    // because Iceberg's SchemaUpdate does not allow incompatible changes.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl MODIFY (dt STRING NOT NULL)"))
        .isInstanceOf(TableException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Cannot change column nullability: dt: optional -> required");

    // Set nullability from required to optional
    sql("ALTER TABLE tl MODIFY (id INTEGER)");
    Schema schemaAfter = table("tl").schema();
    assertThat(schemaAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
  }

  @TestTemplate
  public void testAlterTableModifyColumnPosition() {
    sql("CREATE TABLE tl(id BIGINT, dt STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());

    sql("ALTER TABLE tl MODIFY (dt STRING FIRST)");
    Schema schemaAfter = table("tl").schema();
    assertThat(schemaAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(1, "id", Types.LongType.get()))
                .asStruct());

    sql("ALTER TABLE tl MODIFY (dt STRING AFTER id)");
    Schema schemaAfterAfter = table("tl").schema();
    assertThat(schemaAfterAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());
    // Modifying the position of a non-existing column should fail due to Flink's internal
    // validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl MODIFY (non_existing STRING FIRST)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Try to modify a column `non_existing` which does not exist in the table.");

    // Moving a column after a non-existing column should fail due to Flink's internal validation.
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl MODIFY (dt STRING AFTER non_existing)"))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Referenced column `non_existing` by 'AFTER' does not exist in the table.");
  }

  @TestTemplate
  public void testAlterTableModifyColumnComment() {
    sql("CREATE TABLE tl(id BIGINT, dt STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(2, "dt", Types.StringType.get()))
                .asStruct());

    sql("ALTER TABLE tl MODIFY (dt STRING COMMENT 'comment for dt field')");
    Schema schemaAfter = table("tl").schema();
    assertThat(schemaAfter.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.optional(1, "id", Types.LongType.get()),
                    Types.NestedField.optional(
                        2, "dt", Types.StringType.get(), "comment for dt field"))
                .asStruct());
  }

  @TestTemplate
  public void testAlterTableConstraint() {
    sql("CREATE TABLE tl(id BIGINT NOT NULL, dt STRING NOT NULL, col1 STRING)");
    Schema schemaBefore = table("tl").schema();
    assertThat(schemaBefore.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()))
                .asStruct());
    assertThat(schemaBefore.identifierFieldNames()).isEmpty();
    sql("ALTER TABLE tl ADD (PRIMARY KEY (id) NOT ENFORCED)");
    Schema schemaAfterAdd = table("tl").schema();
    assertThat(schemaAfterAdd.identifierFieldNames()).containsExactly("id");
    sql("ALTER TABLE tl MODIFY (PRIMARY KEY (dt) NOT ENFORCED)");
    Schema schemaAfterModify = table("tl").schema();
    assertThat(schemaAfterModify.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()))
                .asStruct());
    assertThat(schemaAfterModify.identifierFieldNames()).containsExactly("dt");
    // Composite primary key
    sql("ALTER TABLE tl MODIFY (PRIMARY KEY (id, dt) NOT ENFORCED)");
    Schema schemaAfterComposite = table("tl").schema();
    assertThat(schemaAfterComposite.asStruct())
        .isEqualTo(
            new Schema(
                    Types.NestedField.required(1, "id", Types.LongType.get()),
                    Types.NestedField.required(2, "dt", Types.StringType.get()),
                    Types.NestedField.optional(3, "col1", Types.StringType.get()))
                .asStruct());
    assertThat(schemaAfterComposite.identifierFieldNames()).containsExactlyInAnyOrder("id", "dt");
    // Setting an optional field as primary key should fail
    // because Iceberg's SchemaUpdate does not allow incompatible changes.
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE tl MODIFY (PRIMARY KEY (col1) NOT ENFORCED)"))
        .isInstanceOf(TableException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Cannot add field col1 as an identifier field: not a required field");

    // Setting a composite key containing an optional field should fail
    // because Iceberg's SchemaUpdate does not allow incompatible changes.
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE tl MODIFY (PRIMARY KEY (id, col1) NOT ENFORCED)"))
        .isInstanceOf(TableException.class)
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Cannot add field col1 as an identifier field: not a required field");

    // Dropping constraints is not supported yet
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl DROP PRIMARY KEY"))
        .isInstanceOf(TableException.class)
        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
        .hasRootCauseMessage("Unsupported table change: DropConstraint.");
  }

  @TestTemplate
  public void testRelocateTable() {
    assumeThat(isHadoopCatalog).as("HadoopCatalog does not support relocate table").isFalse();
    sql("CREATE TABLE tl(id BIGINT)");
    sql("ALTER TABLE tl SET('location'='file:///tmp/location')");
    assertThat(table("tl").location()).isEqualTo("file:///tmp/location");
  }

  @TestTemplate
  public void testSetCurrentAndCherryPickSnapshotId() {
    sql("CREATE TABLE tl(c1 INT, c2 STRING, c3 STRING) PARTITIONED BY (c1)");

    Table table = table("tl");

    DataFile fileA =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("c1=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    DataFile fileB =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("c1=1") // easy way to set partition data for now
            .withRecordCount(1)
            .build();
    DataFile replacementFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a-replacement.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("c1=0") // easy way to set partition data for now
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(fileA).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(replacementFile).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);
    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(fileB).commit();

    // test cherry pick
    sql("ALTER TABLE tl SET('cherry-pick-snapshot-id'='%s')", staged.snapshotId());
    validateTableFiles(table, fileB, replacementFile);

    // test set current snapshot
    sql("ALTER TABLE tl SET('current-snapshot-id'='%s')", snapshotId);
    validateTableFiles(table, fileA);
  }

  private void validateTableFiles(Table tbl, DataFile... expectedFiles) {
    tbl.refresh();
    Set<CharSequence> expectedFilePaths =
        Arrays.stream(expectedFiles).map(DataFile::path).collect(Collectors.toSet());
    Set<CharSequence> actualFilePaths =
        StreamSupport.stream(tbl.newScan().planFiles().spliterator(), false)
            .map(FileScanTask::file)
            .map(ContentFile::path)
            .collect(Collectors.toSet());
    assertThat(actualFilePaths).as("Files should match").isEqualTo(expectedFilePaths);
  }

  private Table table(String name) {
    return validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, name));
  }

  private CatalogTable catalogTable(String name) throws TableNotExistException {
    return (CatalogTable)
        getTableEnv()
            .getCatalog(getTableEnv().getCurrentCatalog())
            .get()
            .getTable(new ObjectPath(DATABASE, name));
  }
}
