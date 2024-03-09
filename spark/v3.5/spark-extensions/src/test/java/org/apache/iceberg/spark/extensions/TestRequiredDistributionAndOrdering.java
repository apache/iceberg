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

import java.math.BigDecimal;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRequiredDistributionAndOrdering extends ExtensionsTestBase {

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDefaultLocalSortWithBucketTransforms() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should insert a local sort by partition columns by default
    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @TestTemplate
  public void testPartitionColumnsArePrependedForRangeDistribution() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should automatically prepend partition columns to the ordering
    sql("ALTER TABLE %s WRITE ORDERED BY c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @TestTemplate
  public void testSortOrderIncludesPartitionColumns() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should succeed with a correct sort order
    sql("ALTER TABLE %s WRITE ORDERED BY bucket(2, c3), c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @TestTemplate
  public void testHashDistributionOnBucketedColumn() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should automatically prepend partition columns to the local ordering after hash distribution
    sql("ALTER TABLE %s WRITE DISTRIBUTED BY PARTITION ORDERED BY c1, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }

  @TestTemplate
  public void testDisabledDistributionAndOrdering() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c1))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF = ds.coalesce(1).sortWithinPartitions("c1");

    // should fail if ordering is disabled
    Assertions.assertThatThrownBy(
            () ->
                inputDF
                    .writeTo(tableName)
                    .option(SparkWriteOptions.USE_TABLE_DISTRIBUTION_AND_ORDERING, "false")
                    .option(SparkWriteOptions.FANOUT_ENABLED, "false")
                    .append())
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith(
            "Incoming records violate the writer assumption that records are clustered by spec "
                + "and by partition within each spec. Either cluster the incoming records or switch to fanout writers.");
  }

  @TestTemplate
  public void testDefaultSortOnDecimalBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2), (3, 60.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(
            row(1, new BigDecimal("20.20")),
            row(2, new BigDecimal("40.20")),
            row(3, new BigDecimal("60.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @TestTemplate
  public void testDefaultSortOnStringBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, "A"), row(2, "B"));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @TestTemplate
  public void testDefaultSortOnBinaryBucketedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 Binary) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, X'A1B1'), (2, X'A2B2')", tableName);

    byte[] bytes1 = new byte[] {-95, -79};
    byte[] bytes2 = new byte[] {-94, -78};
    List<Object[]> expected = ImmutableList.of(row(1, bytes1), row(2, bytes2));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @TestTemplate
  public void testDefaultSortOnDecimalTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 DECIMAL(20, 2)) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 20.2), (2, 40.2)", tableName);

    List<Object[]> expected =
        ImmutableList.of(row(1, new BigDecimal("20.20")), row(2, new BigDecimal("40.20")));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @TestTemplate
  public void testDefaultSortOnLongTruncatedColumn() {
    sql(
        "CREATE TABLE %s (c1 INT, c2 BIGINT) "
            + "USING iceberg "
            + "PARTITIONED BY (truncate(2, c2))",
        tableName);

    sql("INSERT INTO %s VALUES (1, 22222222222222), (2, 444444444444)", tableName);

    List<Object[]> expected = ImmutableList.of(row(1, 22222222222222L), row(2, 444444444444L));

    assertEquals("Rows must match", expected, sql("SELECT * FROM %s ORDER BY c1", tableName));
  }

  @TestTemplate
  public void testRangeDistributionWithQuotedColumnNames() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (`c.1` INT, c2 STRING, c3 STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(2, `c.1`))",
        tableName);

    List<ThreeColumnRecord> data =
        ImmutableList.of(
            new ThreeColumnRecord(1, null, "A"),
            new ThreeColumnRecord(2, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(4, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(5, "BBBBBBBBBB", "A"),
            new ThreeColumnRecord(6, "BBBBBBBBBB", "B"),
            new ThreeColumnRecord(7, "BBBBBBBBBB", "A"));
    Dataset<Row> ds = spark.createDataFrame(data, ThreeColumnRecord.class);
    Dataset<Row> inputDF =
        ds.selectExpr("c1 as `c.1`", "c2", "c3").coalesce(1).sortWithinPartitions("`c.1`");

    sql("ALTER TABLE %s WRITE ORDERED BY `c.1`, c2", tableName);

    inputDF.writeTo(tableName).append();

    assertEquals(
        "Row count must match",
        ImmutableList.of(row(7L)),
        sql("SELECT count(*) FROM %s", tableName));
  }
}
