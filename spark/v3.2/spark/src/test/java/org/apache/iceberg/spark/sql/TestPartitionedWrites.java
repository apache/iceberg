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

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPartitionedWrites extends SparkCatalogTestBase {
  public TestPartitionedWrites(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string, ts timestamp) USING iceberg PARTITIONED BY" +
            " (bucket(3, id), truncate(5, data), hours(ts))", tableName);
    sql("INSERT INTO %s VALUES (1, 'a', CAST('2022-01-09 21:00:00' as timestamp))," +
            " (2, 'b', CAST('2022-01-09 21:00:00' as timestamp))," +
            " (3, 'c', CAST('2022-01-09 21:00:00' as timestamp))", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testInsertAppend() {
    Assert.assertEquals("Should have 3 rows", 3L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    sql("INSERT INTO %s VALUES (4, 'd', CAST('2022-01-09 21:00:00' as timestamp))," +
            " (5, 'e', CAST('2022-01-09 21:00:00' as timestamp))", tableName);

    Assert.assertEquals("Should have 5 rows after insert", 5L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(2L, "b", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(3L, "c", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(4L, "d", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(5L, "e", Timestamp.valueOf("2022-01-09 21:00:00"))
    );

    assertEquals("Row data should match expected", expected,
            sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testInsertOverwrite() {
    Assert.assertEquals("Should have 3 rows", 3L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    // 4 and 5 replace 3 in the partition (id - (id % 3)) = 3
    sql("INSERT OVERWRITE %s VALUES (4, 'd', CAST('2022-01-09 21:00:00' as timestamp))," +
            " (5, 'e', CAST('2022-01-09 21:00:00' as timestamp))", tableName);

    Assert.assertEquals("Should have 4 rows after overwrite", 4L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(2L, "b", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(4L, "d", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(5L, "e", Timestamp.valueOf("2022-01-09 21:00:00"))
    );

    assertEquals("Row data should match expected", expected,
            sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Append() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).append();

    Assert.assertEquals("Should have 5 rows after insert", 5L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(2L, "b", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(3L, "c", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(4L, "d", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(5L, "e", Timestamp.valueOf("2022-01-09 21:00:00"))
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2DynamicOverwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwritePartitions();

    Assert.assertEquals("Should have 4 rows after overwrite", 4L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(2L, "b", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(4L, "d", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(5L, "e", Timestamp.valueOf("2022-01-09 21:00:00"))
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Overwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwrite(functions.col("id").$less(3));

    Assert.assertEquals("Should have 3 rows after overwrite", 3L,
            scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(3L, "c", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(4L, "d", Timestamp.valueOf("2022-01-09 21:00:00")),
        row(5L, "e", Timestamp.valueOf("2022-01-09 21:00:00"))
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testViewsReturnRecentResults() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    assertEquals("View should have expected rows",
        ImmutableList.of(row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00"))),
        sql("SELECT * FROM tmp"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a', CAST('2022-01-09 21:00:00' as timestamp))", tableName);

    assertEquals("View should have expected rows",
        ImmutableList.of(row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00")),
                row(1L, "a", Timestamp.valueOf("2022-01-09 21:00:00"))),
        sql("SELECT * FROM tmp"));
  }

  @Test
  public void testAddPartition() {
    // only check V2 command [IF NOT EXISTS] syntax
    Table table = validationCatalog.loadTable(tableIdent);
    sql("ALTER TABLE %s ADD IF NOT EXISTS PARTITION" +
            " (id_bucket=2, data_trunc='2022', ts_hour='2022-01-08-23')", tableName);
    table.refresh();
    Assert.assertEquals("Table should start with 3 partition field", 3, table.spec().fields().size());
  }

  @Test
  public void testDropPartition() {
    // only check V2 command [IF EXISTS] syntax
    Table table = validationCatalog.loadTable(tableIdent);
    sql("ALTER TABLE %s DROP IF EXISTS PARTITION" +
            " (id_bucket=2, data_trunc='2022', ts_hour='2022-01-08-23')", tableName);
    table.refresh();
    Assert.assertEquals("Table should start with 3 partition field", 3, table.spec().fields().size());
  }

  @Test
  public void testShowPartitions() {
    Assert.assertEquals("Table should has 3 partitions", 3,
            sql("SHOW PARTITIONS %s", tableName).size());
  }
}
