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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public abstract class /**/ PartitionedWritesTestBase extends CatalogTestBase {

  @BeforeEach
  public void createTables() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (truncate(id, 3))",
        tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testInsertAppend() {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 5 rows after insert")
        .isEqualTo(3L);

    sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", commitTarget());

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 5 rows after insert")
        .isEqualTo(5L);

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testInsertOverwrite() {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 5 rows after insert")
        .isEqualTo(3L);

    // 4 and 5 replace 3 in the partition (id - (id % 3)) = 3
    sql("INSERT OVERWRITE %s VALUES (4, 'd'), (5, 'e')", commitTarget());

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 4 rows after overwrite")
        .isEqualTo(4L);

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testDataFrameV2Append() throws NoSuchTableException {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows")
        .isEqualTo(3L);

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(commitTarget()).append();

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 5 rows after insert")
        .isEqualTo(5L);

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testDataFrameV2DynamicOverwrite() throws NoSuchTableException {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows")
        .isEqualTo(3L);

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(commitTarget()).overwritePartitions();

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 4 rows after overwrite")
        .isEqualTo(4L);

    List<Object[]> expected =
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testDataFrameV2Overwrite() throws NoSuchTableException {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows")
        .isEqualTo(3L);

    List<SimpleRecord> data = ImmutableList.of(new SimpleRecord(4, "d"), new SimpleRecord(5, "e"));
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(commitTarget()).overwrite(functions.col("id").$less(3));

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows after overwrite")
        .isEqualTo(3L);

    List<Object[]> expected = ImmutableList.of(row(3L, "c"), row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testViewsReturnRecentResults() {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows")
        .isEqualTo(3L);

    Dataset<Row> query = spark.sql("SELECT * FROM " + commitTarget() + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    assertEquals(
        "View should have expected rows", ImmutableList.of(row(1L, "a")), sql("SELECT * FROM tmp"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", commitTarget());

    assertEquals(
        "View should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM tmp"));
  }

  // Asserts whether the given table .partitions table has the expected rows. Note that the output
  // row should have spec_id and it is sorted by spec_id and selectPartitionColumns.
  protected void assertPartitionMetadata(
      String tableName, List<Object[]> expected, String... selectPartitionColumns) {
    String[] fullyQualifiedCols =
        Arrays.stream(selectPartitionColumns).map(s -> "partition." + s).toArray(String[]::new);
    Dataset<Row> actualPartitionRows =
        spark
            .read()
            .format("iceberg")
            .load(tableName + ".partitions")
            .select("spec_id", fullyQualifiedCols)
            .orderBy("spec_id", fullyQualifiedCols);

    assertEquals(
        "There are 3 partitions, one with the original spec ID and two with the new one",
        expected,
        rowsToJava(actualPartitionRows.collectAsList()));
  }

  @TestTemplate
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
    // TODO: WAP branch does not support reading partitions table, skip this check for now.
    expected =
        ImmutableList.of(
            row(originalSpecId, 9L, null),
            row(originalSpecId, 12L, null),
            row(table.spec().specId(), 9L, "a"));
    assertPartitionMetadata(tableName, expected, "id_trunc", "data");

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
