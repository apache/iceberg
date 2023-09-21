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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.table.api.DataTypes;
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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

public class TestFlinkCatalogTable extends FlinkCatalogTestBase {

  public TestFlinkCatalogTable(String catalogName, Namespace baseNamespace) {
    super(catalogName, baseNamespace);
  }

  @Override
  @Before
  public void before() {
    super.before();
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE CATALOG %s", catalogName);
    sql("USE %s", DATABASE);
  }

  @After
  public void cleanNamespaces() {
    sql("DROP TABLE IF EXISTS %s.tl", flinkDatabase);
    sql("DROP TABLE IF EXISTS %s.tl2", flinkDatabase);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testGetTable() {
    sql("CREATE TABLE tl(id BIGINT, strV STRING)");

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, "tl"));
    Schema iSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "strV", Types.StringType.get()));
    Assert.assertEquals(
        "Should load the expected iceberg schema", iSchema.toString(), table.schema().toString());
  }

  @Test
  public void testRenameTable() {
    Assume.assumeFalse("HadoopCatalog does not support rename table", isHadoopCatalog);

    final Schema tableSchema =
        new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(TableIdentifier.of(icebergNamespace, "tl"), tableSchema);
    sql("ALTER TABLE tl RENAME TO tl2");

    Assertions.assertThatThrownBy(() -> getTableEnv().from("tl"))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Table `tl` was not found.");

    Schema actualSchema = FlinkSchemaUtil.convert(getTableEnv().from("tl2").getSchema());
    Assert.assertEquals(tableSchema.asStruct(), actualSchema.asStruct());
  }

  @Test
  public void testCreateTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).build(), catalogTable.getSchema());
  }

  @Test
  public void testCreateTableWithPrimaryKey() throws Exception {
    sql("CREATE TABLE tl(id BIGINT, data STRING, key STRING PRIMARY KEY NOT ENFORCED)");

    Table table = table("tl");
    Assert.assertEquals(
        "Should have the expected row key.",
        Sets.newHashSet(table.schema().findField("key").fieldId()),
        table.schema().identifierFieldIds());

    CatalogTable catalogTable = catalogTable("tl");
    Optional<UniqueConstraint> uniqueConstraintOptional = catalogTable.getSchema().getPrimaryKey();
    Assert.assertTrue(
        "Should have the expected unique constraint", uniqueConstraintOptional.isPresent());
    Assert.assertEquals(
        "Should have the expected columns",
        ImmutableList.of("key"),
        uniqueConstraintOptional.get().getColumns());
  }

  @Test
  public void testCreateTableWithMultiColumnsInPrimaryKey() throws Exception {
    sql(
        "CREATE TABLE tl(id BIGINT, data STRING, CONSTRAINT pk_constraint PRIMARY KEY(data, id) NOT ENFORCED)");

    Table table = table("tl");
    Assert.assertEquals(
        "Should have the expected RowKey",
        Sets.newHashSet(
            table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()),
        table.schema().identifierFieldIds());

    CatalogTable catalogTable = catalogTable("tl");
    Optional<UniqueConstraint> uniqueConstraintOptional = catalogTable.getSchema().getPrimaryKey();
    Assert.assertTrue(
        "Should have the expected unique constraint", uniqueConstraintOptional.isPresent());
    Assert.assertEquals(
        "Should have the expected columns",
        ImmutableSet.of("data", "id"),
        ImmutableSet.copyOf(uniqueConstraintOptional.get().getColumns()));
  }

  @Test
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

  @Test
  public void testCreateTableLike() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT)");
    sql("CREATE TABLE tl2 LIKE tl");

    Table table = table("tl2");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());

    CatalogTable catalogTable = catalogTable("tl2");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).build(), catalogTable.getSchema());
  }

  @Test
  public void testCreateTableLocation() {
    Assume.assumeFalse(
        "HadoopCatalog does not support creating table with location", isHadoopCatalog);

    sql("CREATE TABLE tl(id BIGINT) WITH ('location'='file:///tmp/location')");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(Types.NestedField.optional(1, "id", Types.LongType.get())).asStruct(),
        table.schema().asStruct());
    Assert.assertEquals("file:///tmp/location", table.location());
  }

  @Test
  public void testCreatePartitionTable() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT, dt STRING) PARTITIONED BY(dt)");

    Table table = table("tl");
    Assert.assertEquals(
        new Schema(
                Types.NestedField.optional(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "dt", Types.StringType.get()))
            .asStruct(),
        table.schema().asStruct());
    Assert.assertEquals(
        PartitionSpec.builderFor(table.schema()).identity("dt").build(), table.spec());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder()
            .field("id", DataTypes.BIGINT())
            .field("dt", DataTypes.STRING())
            .build(),
        catalogTable.getSchema());
    Assert.assertEquals(Collections.singletonList("dt"), catalogTable.getPartitionKeys());
  }

  @Test
  public void testCreateTableWithFormatV2ThroughTableProperty() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='2')");

    Table table = table("tl");
    Assert.assertEquals(
        "should create table using format v2",
        2,
        ((BaseTable) table).operations().current().formatVersion());
  }

  @Test
  public void testUpgradeTableWithFormatV2ThroughTableProperty() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='1')");

    Table table = table("tl");
    TableOperations ops = ((BaseTable) table).operations();
    Assert.assertEquals("should create table using format v1", 1, ops.refresh().formatVersion());

    sql("ALTER TABLE tl SET('format-version'='2')");
    Assert.assertEquals("should update table to use format v2", 2, ops.refresh().formatVersion());
  }

  @Test
  public void testDowngradeTableToFormatV1ThroughTablePropertyFails() throws Exception {
    sql("CREATE TABLE tl(id BIGINT) WITH ('format-version'='2')");

    Table table = table("tl");
    TableOperations ops = ((BaseTable) table).operations();
    Assert.assertEquals("should create table using format v2", 2, ops.refresh().formatVersion());

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE tl SET('format-version'='1')"))
        .rootCause()
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot downgrade v2 table to v1");
  }

  @Test
  public void testLoadTransformPartitionTable() throws TableNotExistException {
    Schema schema = new Schema(Types.NestedField.optional(0, "id", Types.LongType.get()));
    validationCatalog.createTable(
        TableIdentifier.of(icebergNamespace, "tl"),
        schema,
        PartitionSpec.builderFor(schema).bucket("id", 100).build());

    CatalogTable catalogTable = catalogTable("tl");
    Assert.assertEquals(
        TableSchema.builder().field("id", DataTypes.BIGINT()).build(), catalogTable.getSchema());
    Assert.assertEquals(Collections.emptyList(), catalogTable.getPartitionKeys());
  }

  @Test
  public void testAlterTable() throws TableNotExistException {
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
    CatalogTable catalogTable = catalogTable("tl");
    properties.remove("oldK");
    getTableEnv()
        .getCatalog(getTableEnv().getCurrentCatalog())
        .get()
        .alterTable(new ObjectPath(DATABASE, "tl"), catalogTable.copy(properties), false);
    assertThat(table("tl").properties()).containsAllEntriesOf(properties);
  }

  @Test
  public void testAlterTableWithPrimaryKey() throws TableNotExistException {
    sql("CREATE TABLE tl(id BIGINT, PRIMARY KEY(id) NOT ENFORCED) WITH ('oldK'='oldV')");
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
    CatalogTable catalogTable = catalogTable("tl");
    properties.remove("oldK");
    getTableEnv()
        .getCatalog(getTableEnv().getCurrentCatalog())
        .get()
        .alterTable(new ObjectPath(DATABASE, "tl"), catalogTable.copy(properties), false);
    assertThat(table("tl").properties()).containsAllEntriesOf(properties);
  }

  @Test
  public void testRelocateTable() {
    Assume.assumeFalse("HadoopCatalog does not support relocate table", isHadoopCatalog);

    sql("CREATE TABLE tl(id BIGINT)");
    sql("ALTER TABLE tl SET('location'='file:///tmp/location')");
    Assert.assertEquals("file:///tmp/location", table("tl").location());
  }

  @Test
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
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

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
    Assert.assertEquals("Files should match", expectedFilePaths, actualFilePaths);
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
