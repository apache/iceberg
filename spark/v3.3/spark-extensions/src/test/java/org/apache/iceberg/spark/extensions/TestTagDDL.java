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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.AssertHelpers;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestTagDDL extends SparkExtensionsTestBase {
  private static final String[] TIME_UNITS = {"DAYS", "HOURS", "MINUTES"};

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      }
    };
  }

  public TestTagDDL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void before() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
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
      Assert.assertEquals(
          "The tag needs to point to a specific snapshot id.", firstSnapshotId, ref.snapshotId());
      Assert.assertEquals(
          "The tag needs to have the correct max ref age.",
          TimeUnit.valueOf(timeUnit.toUpperCase(Locale.ENGLISH)).toMillis(maxRefAge),
          ref.maxRefAgeMs().longValue());
    }

    String tagName = "t1";
    AssertHelpers.assertThrows(
        "Illegal statement",
        IcebergParseException.class,
        "mismatched input",
        () ->
            sql(
                "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN",
                tableName, tagName, firstSnapshotId, maxRefAge));

    AssertHelpers.assertThrows(
        "Illegal statement",
        IcebergParseException.class,
        "mismatched input",
        () -> sql("ALTER TABLE %s CREATE TAG %s RETAIN %s DAYS", tableName, tagName, "abc"));

    AssertHelpers.assertThrows(
        "Illegal statement",
        IcebergParseException.class,
        "mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'}",
        () ->
            sql(
                "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN %d SECONDS",
                tableName, tagName, firstSnapshotId, maxRefAge));
  }

  @Test
  public void testCreateTagUseDefaultConfig() throws NoSuchTableException {
    Table table = insertRows();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "t1";

    AssertHelpers.assertThrows(
        "unknown snapshot",
        ValidationException.class,
        "unknown snapshot: -1",
        () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d", tableName, tagName, -1));

    sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName);
    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.", snapshotId, ref.snapshotId());
    Assert.assertNull(
        "The tag needs to have the default max ref age, which is null.", ref.maxRefAgeMs());

    AssertHelpers.assertThrows(
        "Cannot create an exist tag",
        IllegalArgumentException.class,
        "already exists",
        () -> sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName));

    AssertHelpers.assertThrows(
        "Non-conforming tag name",
        IcebergParseException.class,
        "mismatched input '123'",
        () -> sql("ALTER TABLE %s CREATE TAG %s", tableName, "123"));

    table.manageSnapshots().removeTag(tagName).commit();
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    snapshotId = table.currentSnapshot().snapshotId();
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d", tableName, tagName, snapshotId);
    table.refresh();
    ref = table.refs().get(tagName);
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.", snapshotId, ref.snapshotId());
    Assert.assertNull(
        "The tag needs to have the default max ref age, which is null.", ref.maxRefAgeMs());
  }

  @Test
  public void testCreateTagIfNotExists() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = insertRows();
    String tagName = "t1";
    sql("ALTER TABLE %s CREATE TAG %s RETAIN %d days", tableName, tagName, maxSnapshotAge);
    sql("ALTER TABLE %s CREATE TAG IF NOT EXISTS %s", tableName, tagName);

    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.",
        table.currentSnapshot().snapshotId(),
        ref.snapshotId());
    Assert.assertEquals(
        "The tag needs to have the correct max ref age.",
        TimeUnit.DAYS.toMillis(maxSnapshotAge),
        ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testReplaceTagFailsForBranch() throws NoSuchTableException {
    String branchName = "branch1";
    Table table = insertRows();
    long first = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch(branchName, first).commit();
    insertRows();
    long second = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Cannot perform replace tag on branches",
        IllegalArgumentException.class,
        "Ref branch1 is a branch not a tag",
        () -> sql("ALTER TABLE %s REPLACE Tag %s", tableName, branchName, second));
  }

  @Test
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
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.", second, ref.snapshotId());
    Assert.assertEquals(
        "The tag needs to have the correct max ref age.",
        expectedMaxRefAgeMs,
        ref.maxRefAgeMs().longValue());
  }

  @Test
  public void testReplaceTagDoesNotExist() throws NoSuchTableException {
    Table table = insertRows();

    AssertHelpers.assertThrows(
        "Cannot perform replace tag on tag which does not exist",
        IllegalArgumentException.class,
        "Tag does not exist",
        () ->
            sql(
                "ALTER TABLE %s REPLACE Tag %s AS OF VERSION %d",
                tableName, "someTag", table.currentSnapshot().snapshotId()));
  }

  @Test
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
      Assert.assertEquals(
          "The tag needs to point to a specific snapshot id.", second, ref.snapshotId());
      Assert.assertEquals(
          "The tag needs to have the correct max ref age.",
          TimeUnit.valueOf(timeUnit).toMillis(maxRefAge),
          ref.maxRefAgeMs().longValue());
    }
  }

  @Test
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
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.", first, ref.snapshotId());
  }

  @Test
  public void testDropTag() throws NoSuchTableException {
    insertRows();
    Table table = validationCatalog.loadTable(tableIdent);
    String tagName = "t1";
    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(
        "The tag needs to point to a specific snapshot id.",
        table.currentSnapshot().snapshotId(),
        ref.snapshotId());

    sql("ALTER TABLE %s DROP TAG %s", tableName, tagName);
    table.refresh();
    ref = table.refs().get(tagName);
    Assert.assertNull("The tag needs to be dropped.", ref);
  }

  @Test
  public void testDropTagNonConformingName() {
    AssertHelpers.assertThrows(
        "Non-conforming tag name",
        IcebergParseException.class,
        "mismatched input '123'",
        () -> sql("ALTER TABLE %s DROP TAG %s", tableName, "123"));
  }

  @Test
  public void testDropTagDoesNotExist() {
    AssertHelpers.assertThrows(
        "Cannot perform drop tag on tag which does not exist",
        IllegalArgumentException.class,
        "Tag does not exist: nonExistingTag",
        () -> sql("ALTER TABLE %s DROP TAG %s", tableName, "nonExistingTag"));
  }

  @Test
  public void testDropTagFailesForBranch() throws NoSuchTableException {
    String branchName = "b1";
    Table table = insertRows();
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();

    AssertHelpers.assertThrows(
        "Cannot perform drop tag on branch",
        IllegalArgumentException.class,
        "Ref b1 is a branch not a tag",
        () -> sql("ALTER TABLE %s DROP TAG %s", tableName, branchName));
  }

  @Test
  public void testDropTagIfExists() throws NoSuchTableException {
    String tagName = "nonExistingTag";
    Table table = insertRows();
    Assert.assertNull("The tag does not exists.", table.refs().get(tagName));

    sql("ALTER TABLE %s DROP TAG IF EXISTS %s", tableName, tagName);
    table.refresh();
    Assert.assertNull("The tag still does not exist.", table.refs().get(tagName));

    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();
    Assert.assertEquals(
        "The tag has been created successfully.",
        table.currentSnapshot().snapshotId(),
        table.refs().get(tagName).snapshotId());

    sql("ALTER TABLE %s DROP TAG IF EXISTS %s", tableName, tagName);
    table.refresh();
    Assert.assertNull("The tag needs to be dropped.", table.refs().get(tagName));
  }

  private Table insertRows() throws NoSuchTableException {
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    return validationCatalog.loadTable(tableIdent);
  }
}
