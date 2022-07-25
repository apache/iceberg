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
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestSnapshotRefSQL extends SparkExtensionsTestBase {
  public TestSnapshotRefSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCreateTag() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    long maxRefAge = 10L;
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
        tableName, tagName, snapshotId, maxRefAge);
    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertNotNull(ref);
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000, ref.maxRefAgeMs().longValue());

    AssertHelpers.assertThrows("Cannot create an exist tag",
        IllegalArgumentException.class, "already exists",
        () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
            tableName, tagName, snapshotId, maxRefAge));

    AssertHelpers.assertThrows("Tag name can not be empty or null.",
        IllegalArgumentException.class, "Tag name can not be empty or null",
        () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
            tableName, "` `", snapshotId, maxRefAge));

    String tagName2 = "t2";
    AssertHelpers.assertThrows("Snapshot Id must be greater than 0",
        IllegalArgumentException.class, "must be greater than 0",
        () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
            tableName, tagName2, -1, maxRefAge));
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000, ref.maxRefAgeMs().longValue());

    AssertHelpers.assertThrows("Cannot set tag to unknown snapshot",
        ValidationException.class, "unknown snapshot: 999",
        () -> sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
            tableName, tagName2, 999, maxRefAge));
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000, ref.maxRefAgeMs().longValue());

    String tagName3 = "t3";
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d",
        tableName, tagName3, snapshotId);
    table.refresh();
    SnapshotRef ref3 = ((BaseTable) table).operations().current().ref(tagName3);
    Assert.assertEquals(Long.MAX_VALUE, ref3.maxRefAgeMs().longValue());
  }

  @Test
  public void testReplaceTag() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();

    long snapshotId = table.currentSnapshot().snapshotId();
    String tagName = "t1";
    long maxRefAge = 10L;
    sql("ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
        tableName, tagName, snapshotId, maxRefAge);
    table.refresh();
    SnapshotRef ref1 = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref1.maxRefAgeMs().longValue());
    Assert.assertEquals(snapshotId, ref1.snapshotId());

    List<SimpleRecord> records = ImmutableList.of(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    table.refresh();
    long snapshotId2 = table.currentSnapshot().snapshotId();
    maxRefAge = 9L;
    sql("ALTER TABLE %s REPLACE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
        tableName, tagName, snapshotId2, maxRefAge);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref2.maxRefAgeMs().longValue());
    Assert.assertEquals(snapshotId2, ref2.snapshotId());

    sql("ALTER TABLE %s REPLACE TAG %s RETAIN FOR %d DAYS", tableName, tagName, maxRefAge);
    table.refresh();
    SnapshotRef ref3 = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertEquals(snapshotId2, ref3.snapshotId());

    AssertHelpers.assertThrows("Cannot set tag to unknown snapshot",
        ValidationException.class, "unknown snapshot: 999",
        () -> sql(
            "ALTER TABLE %s REPLACE TAG %s AS OF VERSION %d",
            tableName, tagName, 999));

    String tagName2 = "t2";
    AssertHelpers.assertThrows("Cannot replace a tag that does not exist",
        IllegalArgumentException.class, String.format("Tag does not exist: %s", tagName2),
        () -> sql(
            "ALTER TABLE %s REPLACE TAG %s AS OF VERSION %d",
            tableName, tagName2, 1));
  }

  @Test
  public void testDropTag() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    String tagName = "t1";
    sql("ALTER TABLE %s CREATE TAG %s", tableName, tagName);
    table.refresh();
    SnapshotRef ref = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertNotNull(ref);

    sql("ALTER TABLE %s DROP TAG %s", tableName, tagName);
    table.refresh();
    ref = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertNull(ref);

    AssertHelpers.assertThrows("Cannot drop tag that does not exists",
        IllegalArgumentException.class, String.format("Tag does not exist: %s", tagName),
        () -> sql("ALTER TABLE %s DROP TAG %s",
            tableName, tagName));
  }

  @Test
  public void testAlterTagRefRetention() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();

    long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = ImmutableList.of(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();

    String tagName = "t1";
    long maxRefAge = 7L;
    sql(
        "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
        tableName, tagName, snapshotId, maxRefAge);
    table.refresh();
    SnapshotRef ref1 = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertEquals(maxRefAge * 24 * 60 * 60 * 1000L, ref1.maxRefAgeMs().longValue());
    Assert.assertEquals(snapshotId, ref1.snapshotId());

    String tagName2 = "t2";
    AssertHelpers.assertThrows("Cannot alter tag that does not exist",
        IllegalArgumentException.class, String.format("does not exist: %s", tagName2),
        () -> sql("ALTER TABLE %s ALTER TAG %s RETAIN FOR %d DAYS",
            tableName, "t2", maxRefAge));

    long maxRefAge2 = 6L;
    sql(
        "ALTER TABLE %s ALTER TAG %s RETAIN FOR %d DAYS",
        tableName, tagName, maxRefAge2);
    table.refresh();
    SnapshotRef ref2 = ((BaseTable) table).operations().current().ref(tagName);
    Assert.assertEquals(maxRefAge2 * 24 * 60 * 60 * 1000L, ref2.maxRefAgeMs().longValue());
    Assert.assertEquals(snapshotId, ref2.snapshotId());

    AssertHelpers.assertThrows("Max reference age must be greater than 0",
        IllegalArgumentException.class, "Max reference age must be greater than 0",
        () -> sql("ALTER TABLE %s ALTER TAG %s RETAIN FOR %d DAYS",
            tableName, tagName, -1));
  }

  private Table createDefaultTableAndInsert2Row() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records = ImmutableList.of(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b")
    );
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    return table;
  }
}
