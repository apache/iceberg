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
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
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
  public void testUseBranch() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    Table table = validationCatalog.loadTable(tableIdent);
    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();

    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    String prefix = "at_branch_";

    // read the table at the branch
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s.%s order by id", tableName, prefix + branchName));

    String mainBranch = "main";
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(1, "a"), row(2, "b"), row(2, "b")),
        sql("SELECT * FROM %s.%s order by id", tableName, prefix + mainBranch));

    String branchNotExist = "b2";
    AssertHelpers.assertThrows(
        "Cant not use a ref that doest not exist!",
        IllegalArgumentException.class,
        "Snapshot ref does not exist: b2",
        () -> sql("SELECT * FROM %s.%s order by id", tableName, prefix + branchNotExist));

    sql("USE BRANCH %s", branchName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));

    sql("USE BRANCH %s", mainBranch);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(1, "a"), row(2, "b"), row(2, "b")),
        sql("SELECT * FROM %s order by id", tableName));

    sql("USE BRANCH %s", branchNotExist);
    AssertHelpers.assertThrows(
        "Cant not use a ref that doest not exist!",
        IllegalArgumentException.class,
        "Snapshot ref does not exist: b2",
        () -> sql("SELECT * FROM %s order by id", tableName));
  }

  @Test
  public void testUseTag() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    String tagName = "t1";
    table.manageSnapshots().createTag(tagName, table.currentSnapshot().snapshotId()).commit();

    spark.createDataFrame(records, SimpleRecord.class).writeTo(tableName).append();

    String prefix = "at_tag_";

    // read the table at the branch
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s.%s", tableName, prefix + tagName));

    String tagNotExist = "b2";
    AssertHelpers.assertThrows(
        "Cant not use a ref that doest not exist!",
        IllegalArgumentException.class,
        "Snapshot ref does not exist: b2",
        () -> sql("SELECT * FROM %s.%s", tableName, prefix + tagNotExist));

    sql("USE Tag %s", tagName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "a"), row(2, "b")),
        sql("SELECT * FROM %s ", tableName));

    sql("USE Tag %s", tagNotExist);
    AssertHelpers.assertThrows(
        "Cant not use a ref that doest not exist!",
        IllegalArgumentException.class,
        "Snapshot ref does not exist: b2",
        () -> sql("SELECT * FROM %s ", tableName));
  }

  private Table createDefaultTableAndInsert2Row() throws NoSuchTableException {
    Assume.assumeTrue(catalogName.equalsIgnoreCase("testhive"));
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);

    List<SimpleRecord> records =
        ImmutableList.of(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.writeTo(tableName).append();
    Table table = validationCatalog.loadTable(tableIdent);
    return table;
  }
}
