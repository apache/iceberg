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
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestSnapshotRefSQL extends SparkExtensionsTestBase {
  public TestSnapshotRefSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testQueryRefsMetadata() throws NoSuchTableException {
    Table table = createDefaultTableAndInsert2Row();
    long snapshotId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    Integer minSnapshotsToKeep = 2;
    long maxSnapshotAge = 2L;
    long maxRefAge = 10L;
    sql(
        "ALTER TABLE %s CREATE BRANCH %s AS OF VERSION %d WITH SNAPSHOT RETENTION %d SNAPSHOTS %d DAYS RETAIN %d DAYS",
        tableName,
        branchName,
        snapshotId,
        minSnapshotsToKeep,
        maxSnapshotAge,
        maxRefAge);
    table.refresh();

    String tagName = "t1";
    sql(
        "ALTER TABLE %s CREATE TAG %s AS OF VERSION %d RETAIN FOR %d DAYS",
        tableName, tagName, snapshotId, maxRefAge);
    table.refresh();
    // check all result
    List<Object[]> list = sql("select * from %s.refs", tableName);
    Assert.assertEquals("we should be able to find three values: main, b1, and t1", 3, list.size());
    // check branch b1 result
    List<Object[]> branchRows = sql("select * from %s.refs where reference='%s'", tableName, branchName);
    Assert.assertEquals(1, branchRows.size());
    Assert.assertEquals("BRANCH", branchRows.get(0)[1].toString());
    Assert.assertEquals(
        snapshotId,
        Long.parseLong(branchRows.get(0)[2].toString()));
    Assert.assertEquals(
        maxRefAge * 24 * 60 * 60 * 1000,
        Long.parseLong((branchRows.get(0)[3].toString())));
    Assert.assertEquals(
        minSnapshotsToKeep.intValue(),
        Integer.parseInt(branchRows.get(0)[4].toString()));
    Assert.assertEquals(
        maxSnapshotAge * 24 * 60 * 60 * 1000,
        Long.parseLong((branchRows.get(0)[5].toString())));
    // check tag t1 result
    List<Object[]> tagRows = sql("select * from %s.refs where type='%s'", tableName, "TAG");
    Assert.assertEquals(1, tagRows.size());
    Assert.assertEquals("t1", tagRows.get(0)[0].toString());
    Assert.assertEquals(snapshotId, Long.parseLong((tagRows.get(0)[2].toString())));
    Assert.assertEquals(
        maxRefAge * 24 * 60 * 60 * 1000,
        Long.parseLong((tagRows.get(0)[3].toString())));
    // query with snapshot_id
    List<Object[]> rows3 = sql("select * from %s.refs where snapshot_id=%d", tableName, snapshotId);
    Assert.assertEquals(3, rows3.size());
    // query with all condition
    List<Object[]> rows1 = sql("select * from %s.refs where snapshot_id=%d and max_ref_age_ms=%d and " +
            "min_snapshots_to_keep=%d and max_snapshot_age_ms=%d", tableName,
        snapshotId, maxRefAge * 24 * 60 * 60 * 1000, minSnapshotsToKeep, maxSnapshotAge * 24 * 60 * 60 * 1000);
    Assert.assertEquals(1, rows1.size());
    // query with null
    List<Object[]> main1 = sql("select * from %s.refs where max_ref_age_ms is null", tableName);
    Assert.assertEquals(1, main1.size());
    // query with error Type
    List<Object[]> error1 = sql("select * from %s.refs where type='%s'", tableName, "TAG1");
    Assert.assertEquals(0, error1.size());
    // query with error field
    AssertHelpers.assertThrows(
        "Cannot use unknown column",
        AnalysisException.class,
        () -> sql("select * from %s.refs where type1='%s'", tableName, "TAG"));
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
