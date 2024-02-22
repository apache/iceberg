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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public abstract class UnpartitionedWritesTestBase extends CatalogTestBase {

  @BeforeEach
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testInsertAppend() {
    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 3 rows")
        .isEqualTo(3L);

    sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", commitTarget());

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 5 rows")
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
        .as("Should have 3 rows")
        .isEqualTo(3L);

    sql("INSERT OVERWRITE %s VALUES (4, 'd'), (5, 'e')", commitTarget());

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 2 rows after overwrite")
        .isEqualTo(2L);

    List<Object[]> expected = ImmutableList.of(row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testInsertAppendAtSnapshot() {
    assumeThat(tableName.equals(commitTarget())).isTrue();
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    String prefix = "snapshot_id_";

    Assertions.assertThatThrownBy(
            () ->
                sql("INSERT INTO %s.%s VALUES (4, 'd'), (5, 'e')", tableName, prefix + snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot write to table at a specific snapshot");
  }

  @TestTemplate
  public void testInsertOverwriteAtSnapshot() {
    assumeThat(tableName.equals(commitTarget())).isTrue();
    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    String prefix = "snapshot_id_";

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "INSERT OVERWRITE %s.%s VALUES (4, 'd'), (5, 'e')",
                    tableName, prefix + snapshotId))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot write to table at a specific snapshot");
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
        .as("Should have 2 rows after overwrite")
        .isEqualTo(2L);

    List<Object[]> expected = ImmutableList.of(row(4L, "d"), row(5L, "e"));

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

    ds.writeTo(commitTarget()).overwrite(functions.col("id").$less$eq(3));

    assertThat(scalarSql("SELECT count(*) FROM %s", selectTarget()))
        .as("Should have 2 rows after overwrite")
        .isEqualTo(2L);

    List<Object[]> expected = ImmutableList.of(row(4L, "d"), row(5L, "e"));

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", selectTarget()));
  }
}
