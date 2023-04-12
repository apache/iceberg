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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
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
  public TestPartitionedWrites(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (truncate(id, 3))",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testInsertAppend() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", tableName);

    Assert.assertEquals(
        "Should have 5 rows after insert", 5L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testInsertOverwrite() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    // 4 and 5 replace 3 in the partition (id - (id % 3)) = 3
    sql("INSERT OVERWRITE %s VALUES (4, 'd'), (5, 'e')", tableName);

    Assert.assertEquals(
        "Should have 4 rows after overwrite", 4L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Append() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).append();

    Assert.assertEquals(
        "Should have 5 rows after insert", 5L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2DynamicOverwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwritePartitions();

    Assert.assertEquals(
        "Should have 4 rows after overwrite", 4L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Overwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwrite(functions.col("id").$less(3));

    Assert.assertEquals(
        "Should have 3 rows after overwrite", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testViewsReturnRecentResults() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    assertEquals(
        "View should have expected rows", ImmutableList.of(row(1L, "a")), sql("SELECT * FROM tmp"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "View should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM tmp"));
  }

  @Test
  public void testWriteWithOutputSpec() throws NoSuchTableException {
    Table table = validationCatalog.loadTable(tableIdent);

    // Drop all records in table to have a fresh start.
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    final int originalSpecId = table.spec().specId();
    table.updateSpec().addField("data").commit();

    // Refresh this when using SparkCatalog since otherwise the new spec would not be caught.
    sql("REFRESH TABLE %s", tableName);

    // By default, we write to the current spec.
    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(10, "a"));
    spark.createDataFrame(data, SimpleRecord.class).toDF().writeTo(tableName).append();

    List<Object[]> expected = ImmutableList.of(row(10L, "a", table.spec().specId()));
    assertEquals(
        "Rows must match",
        expected,
        sql("SELECT id, data, _spec_id FROM %s WHERE id >= 10 ORDER BY id", tableName));

    // Output spec ID should be respected when present.
    data = ImmutableList.of(new SimpleRecord(11, "b"), new SimpleRecord(12, "c"));
    spark
        .createDataFrame(data, SimpleRecord.class)
        .toDF()
        .writeTo(tableName)
        .option("output-spec-id", Integer.toString(originalSpecId))
        .append();

    expected =
        ImmutableList.of(
            row(10L, "a", table.spec().specId()),
            row(11L, "b", originalSpecId),
            row(12L, "c", originalSpecId));
    assertEquals(
        "Rows must match",
        expected,
        sql("SELECT id, data, _spec_id FROM %s WHERE id >= 10 ORDER BY id", tableName));

    // Verify that the actual partitions are written with the correct spec ID.
    // Two of the partitions should have the original spec ID and one should have the new one.
    Dataset<Row> actualPartitionRows =
        spark
            .read()
            .format("iceberg")
            .load(tableName + ".partitions")
            .select("spec_id", "partition.id_trunc", "partition.data")
            .orderBy("spec_id", "partition.id_trunc");

    expected =
        ImmutableList.of(
            row(originalSpecId, 9L, null),
            row(originalSpecId, 12L, null),
            row(table.spec().specId(), 9L, "a"));
    assertEquals(
        "There are 3 partitions, one with the original spec ID and two with the new one",
        expected,
        rowsToJava(actualPartitionRows.collectAsList()));

    // Even the default spec ID should be followed when present.
    data = ImmutableList.of(new SimpleRecord(13, "d"));
    spark
        .createDataFrame(data, SimpleRecord.class)
        .toDF()
        .writeTo(tableName)
        .option("output-spec-id", Integer.toString(table.spec().specId()))
        .append();

    expected =
        ImmutableList.of(
            row(10L, "a", table.spec().specId()),
            row(11L, "b", originalSpecId),
            row(12L, "c", originalSpecId),
            row(13L, "d", table.spec().specId()));
    assertEquals(
        "Rows must match",
        expected,
        sql("SELECT id, data, _spec_id FROM %s WHERE id >= 10 ORDER BY id", tableName));
  }
}
