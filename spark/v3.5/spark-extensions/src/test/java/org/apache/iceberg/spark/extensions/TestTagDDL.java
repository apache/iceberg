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

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.extensions.IcebergParseException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTagDDL extends ExtensionsTestBase {
  private static final String[] TIME_UNITS = {"DAYS", "HOURS", "MINUTES"};

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      }
    };
  }

  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testCreateTagWithRetain() throws NoSuchTableException {
    Table table = insertRows();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long maxRefAge = 10L;

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    for (String timeUnit : TIME_UNITS) {
      String tagName = "t1" + timeUnit;
      sql(
          "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN %d %s",
          tableName, tagName, firstSnapshotId, maxRefAge, timeUnit);
      table.refresh();
      SnapshotRef ref = table.refs().get(tagName);
      assertThat(ref.snapshotId())
          .as("The tag needs to point to a specific snapshot id.")
          .isEqualTo(firstSnapshotId);
      assertThat(ref.maxRefAgeMs().longValue())
          .as("The tag needs to have the correct max ref age.")
          .isEqualTo(TimeUnit.valueOf(timeUnit.toUpperCase(Locale.ENGLISH)).toMillis(maxRefAge));
    }

    String tagName = "t1";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN",
                    tableName, tagName, firstSnapshotId, maxRefAge))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input");

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s CREATE TAG %s RETAIN %s DAYS", tableName, tagName, "abc"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN %d SECONDS",
                    tableName, tagName, firstSnapshotId, maxRefAge))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'}");
  }

  @TestTemplate
  public void testCreateTagOnEmptyTable() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE TAG %s", tableName, "abc"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot complete create or replace tag operation on %s, main has no snapshot",
            tableName);
  }

  @TestTemplate
  public void testCreateTagUseDefaultConfig() throws NoSuchTableException {
    Table table = insertRows();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "t1";

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d", tableName, tagName, -1))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot set " + tagName + " to unknown snapshot: -1");

    sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName);
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    assertThat(ref.snapshotId())
        .as("The tag needs to point to a specific snapshot id.")
        .isEqualTo(snapshotId);
    assertThat(ref.maxRefAgeMs())
        .as("The tag needs to have the default max ref age, which is null.")
        .isNull();

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists");

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s CREATE TAG %s", tableName, "123"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input '123'");

    table.manageSnapshots().removeTag(tagName).commit();
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    snapshotId = table.currentSnapshot().snapshotId();
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d", tableName, tagName, snapshotId);
    table.refresh();
    ref = table.refs().get(tagName);
    assertThat(ref.snapshotId())
        .as("The tag needs to point to a specific snapshot id.")
        .isEqualTo(snapshotId);
    assertThat(ref.maxRefAgeMs())
        .as("The tag needs to have the default max ref age, which is null.")
        .isNull();
  }

  @TestTemplate
  public void testCreateTagIfNotExists() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String tagName = "t1";
    sql("ALTER TABLE %s CREATE TAG %s RETAIN %d days", tableName, tagName, maxSnapshotAge);
    sql("ALTER TABLE %s CREATE TAG IF NOT EXISTS %s", tableName, tagName);

    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    assertThat(ref.snapshotId())
        .as("The tag needs to point to a specific snapshot id.")
        .isEqualTo(table.currentSnapshot().snapshotId());
    assertThat(ref.maxRefAgeMs().longValue())
        .as("The tag needs to have the correct max ref age.")
        .isEqualTo(TimeUnit.DAYS.toMillis(maxSnapshotAge));
  }

  @TestTemplate
  public void testReplaceTagFailsForBranch() throws NoSuchTableException {
    String branchName = "branch1";
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(branchName, first).commit();
    insertRows();
    long second = table.currentSnapshot().snapshotId();

    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s REPLACE Tag %s", tableName, branchName, second))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Ref branch1 is a branch not a tag");
  }

  @TestTemplate
  public void testReplaceTag() throws NoSuchTableException {
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    long expectedMaxRefAgeMs = 1000;
    table
        .manageSnapshots()
        .createTag(tagName, first)
        .setMaxRefAgeMs(tagName, expectedMaxRefAgeMs)
        .commit();

    insertRows();
    long second = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s REPLACE Tag %s AS OF VERSION %d", tableName, tagName, second);
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    assertThat(ref.snapshotId())
        .as("The tag needs to point to a specific snapshot id.")
        .isEqualTo(second);
    assertThat(ref.maxRefAgeMs().longValue())
        .as("The tag needs to have the correct max ref age.")
        .isEqualTo(expectedMaxRefAgeMs);
  }

  @TestTemplate
  public void testReplaceTagDoesNotExist() throws NoSuchTableException {
    Table table = insertRows();

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "ALTER TABLE %s REPLACE Tag %s AS OF VERSION %d",
                    tableName, "someTag", table.currentSnapshot().snapshotId()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Tag does not exist");
  }

  @TestTemplate
  public void testReplaceTagWithRetain() throws NoSuchTableException {
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    table.manageSnapshots().createTag(tagName, first).commit();
    insertRows();
    long second = table.currentSnapshot().snapshotId();

    long maxRefAge = 10;
    for (String timeUnit : TIME_UNITS) {
      sql(
          "ALTER TABLE %s REPLACE Tag %s AS OF VERSION %d RETAIN %d %s",
          tableName, tagName, second, maxRefAge, timeUnit);

      table.refresh();
      SnapshotRef ref = table.refs().get(tagName);
      assertThat(ref.snapshotId())
          .as("The tag needs to point to a specific snapshot id.")
          .isEqualTo(second);
      assertThat(ref.maxRefAgeMs().longValue())
          .as("The tag needs to have the correct max ref age.")
          .isEqualTo(TimeUnit.valueOf(timeUnit).toMillis(maxRefAge));
    }
  }

  @TestTemplate
  public void testCreateOrReplace() throws NoSuchTableException {
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    insertRows();
    long second = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagName, second).commit();

    sql("ALTER TABLE %s CREATE OR REPLACE TAG %s AS OF VERSION %d", tableName, tagName, first);
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    assertThat(ref.snapshotId())
        .as("The tag needs to point to a specific snapshot id.")
        .isEqualTo(first);
  }

  @TestTemplate
  public void testDropTag() throws NoSuchTableException {
    insertRows();
    Table table = validationCatalog.loadTable(tableIdent);
    String tagName = "t1";
    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();
    SnapshotRef ref = table.refs().get(tagName);
    assertThat(ref.snapshotId()).as("").isEqualTo(table.currentSnapshot().snapshotId());

    sql("ALTER TABLE %s DROP TAG %s", tableName, tagName);
    table.refresh();
    ref = table.refs().get(tagName);
    assertThat(ref).as("The tag needs to be dropped.").isNull();
  }

  @TestTemplate
  public void testDropTagNonConformingName() {
    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP TAG %s", tableName, "123"))
        .isInstanceOf(IcebergParseException.class)
        .hasMessageContaining("mismatched input '123'");
  }

  @TestTemplate
  public void testDropTagDoesNotExist() {
    Assertions.assertThatThrownBy(
            () -> sql("ALTER TABLE %s DROP TAG %s", tableName, "nonExistingTag"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Tag does not exist: nonExistingTag");
  }

  @TestTemplate
  public void testDropTagFailesForBranch() throws NoSuchTableException {
    String branchName = "b1";
    Table table = insertRows();
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();

    Assertions.assertThatThrownBy(() -> sql("ALTER TABLE %s DROP TAG %s", tableName, branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Ref b1 is a branch not a tag");
  }

  @TestTemplate
  public void testDropTagIfExists() throws NoSuchTableException {
    String tagName = "nonExistingTag";
    Table table = insertRows();
    assertThat(table.refs().get(tagName)).as("The tag does not exists.").isNull();

    sql("ALTER TABLE %s DROP TAG IF EXISTS %s", tableName, tagName);
    table.refresh();
    assertThat(table.refs().get(tagName)).as("The tag still does not exist.").isNull();

    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();
    assertThat(table.refs().get(tagName).snapshotId())
        .as("The tag has been created successfully.")
        .isEqualTo(table.currentSnapshot().snapshotId());

    sql("ALTER TABLE %s DROP TAG IF EXISTS %s", tableName, tagName);
    table.refresh();
    assertThat(table.refs().get(tagName)).as("The tag needs to be dropped.").isNull();
  }

  @TestTemplate
  public void createOrReplaceWithNonExistingTag() throws NoSuchTableException {
    Table table = insertRows();
    String tagName = "t1";
    insertRows();
    long snapshotId = table.currentSnapshot().snapshotId();

    sql("ALTER TABLE %s CREATE OR REPLACE TAG %s AS OF VERSION %d", tableName, tagName, snapshotId);
    table.refresh();
    assertThat(table.refs().get(tagName).snapshotId()).isEqualTo(snapshotId);
  }

  private Table insertRows() throws NoSuchTableException {
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    return validationCatalog.loadTable(tableIdent);
  }
}
