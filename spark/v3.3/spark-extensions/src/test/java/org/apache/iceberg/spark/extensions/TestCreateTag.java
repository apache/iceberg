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
import org.junit.Test;
import org.junit.runners.Parameterized;

public class TestCreateTag extends SparkExtensionsTestBase {
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

  public TestCreateTag(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCreateTagWithRetain() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
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
      Assert.assertEquals(firstSnapshotId, ref.snapshotId());
      Assert.assertEquals(
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
        "mismatched input 'SECONDS' expecting {'DAYS', 'HOURS', 'MINUTES'}",
        () ->
            sql(
                "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN %d SECONDS",
                tableName, tagName, firstSnapshotId, maxRefAge));
  }

  @Test
  public void testCreateTagUseDefaultConfig() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
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
    Assert.assertEquals(snapshotId, ref.snapshotId());
    Assert.assertNull(ref.maxRefAgeMs());

    AssertHelpers.assertThrows(
        "Cannot create an exist tag",
        IllegalArgumentException.class,
        "already exists",
        () -> sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName));

    table.manageSnapshots().removeTag(tagName).commit();
    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    snapshotId = table.currentSnapshot().snapshotId();
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d", tableName, tagName, snapshotId);
    table.refresh();
    ref = table.refs().get(tagName);
    Assert.assertEquals(snapshotId, ref.snapshotId());
    Assert.assertNull(ref.maxRefAgeMs());
  }

  @Test
  public void testCreateTagIfNotExists() throws NoSuchTableException {
    long maxSnapshotAge = 2L;
    Table table = createDefaultTableAndInsert2Row();
    String tagName = "t1";
    sql("ALTER TABLE %s CREATE TAG %s RETAIN %d days", tableName, tagName, maxSnapshotAge);
    sql("ALTER TABLE %s CREATE TAG IF NOT EXISTS %s", tableName, tagName);

    table.refresh();
    SnapshotRef ref = table.refs().get(tagName);
    Assert.assertEquals(table.currentSnapshot().snapshotId(), ref.snapshotId());
    Assert.assertEquals(TimeUnit.DAYS.toMillis(maxSnapshotAge), ref.maxRefAgeMs().longValue());
  }

  private Table createDefaultTableAndInsert2Row() throws NoSuchTableException {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    return table;
  }
}
