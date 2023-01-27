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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;

public class TestDeleteFrom extends SparkCatalogTestBase {
  public TestDeleteFrom(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDeleteFromUnpartitionedTable() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 2", tableName);

    assertEquals(
        "Should have no rows after successful delete",
        ImmutableList.of(row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 4", tableName);

    assertEquals(
        "Should have no rows after successful delete",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteFromTableAtSnapshot() throws NoSuchTableException {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    long snapshotId = validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
    String prefix = "snapshot_id_";
    AssertHelpers.assertThrows(
        "Should not be able to delete from a table at a specific snapshot",
        IllegalArgumentException.class,
        "Cannot delete from table at a specific snapshot",
        () -> sql("DELETE FROM %s.%s WHERE id < 4", tableName, prefix + snapshotId));
  }

  @Test
  public void testDeleteFromPartitionedTable() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(id, 2))",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.coalesce(1).writeTo(tableName).append();

    assertEquals(
        "Should have 3 rows in 2 partitions",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id > 2", tableName);
    assertEquals(
        "Should have two rows in the second partition",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 2", tableName);

    assertEquals(
        "Should have two rows in the second partition",
        ImmutableList.of(row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteFromWhereFalse() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    sql("DELETE FROM %s WHERE false", tableName);

    table.refresh();

    Assert.assertEquals(
        "Delete should not produce a new snapshot", 1, Iterables.size(table.snapshots()));
  }

  @Test
  public void testTruncate() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    sql("TRUNCATE TABLE %s", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDeleteWithCommonPartitionHashCode() throws NoSuchTableException, ParseException {
    sql(
        "CREATE TABLE %s (log_dateint int, request_dateint int) USING iceberg "
            + "PARTITIONED BY (log_dateint, request_dateint)",
        tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    PartitionData partitionOne = new PartitionData(table.spec().partitionType());
    partitionOne.set(0, 20220730);
    partitionOne.set(1, 20220721);
    PartitionData partitionTwo = new PartitionData(table.spec().partitionType());
    partitionTwo.set(0, 20220728);
    partitionTwo.set(1, 20220803);

    Assert.assertEquals(
        StructLikeWrapper.forType(table.spec().partitionType()).set(partitionOne).hashCode(),
        StructLikeWrapper.forType(table.spec().partitionType()).set(partitionTwo).hashCode());

    Dataset<Row> df =
        spark
            .createDataset(
                ImmutableList.of(
                    Tuple2.apply((int) partitionOne.get(0), (int) partitionOne.get(1)),
                    Tuple2.apply((int) partitionTwo.get(0), (int) partitionTwo.get(1))),
                Encoders.tuple(Encoders.INT(), Encoders.INT()))
            .toDF("log_dateint", "request_dateint");

    df.writeTo(tableName).append();

    Assert.assertEquals(
        1, sql("SELECT * FROM %s WHERE request_dateint == 20220803", tableName).size());
    Assert.assertEquals(
        1, sql("SELECT * FROM %s WHERE request_dateint == 20220721", tableName).size());

    sql("DELETE FROM %s WHERE request_dateint == 20220803", tableName);
    Assert.assertEquals(
        0, sql("SELECT * FROM %s WHERE request_dateint == 20220803", tableName).size());
    Assert.assertEquals(
        1, sql("SELECT * FROM %s WHERE request_dateint == 20220721", tableName).size());
  }
}
