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

import java.util.List;
import java.util.stream.IntStream;
import org.apache.iceberg.spark.IcebergSpark;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestPartitionedWritesAsSelect extends TestBaseWithCatalog {

  private final String targetTable = "testhadoop.default.target_table";

  @BeforeEach
  public void createTables() {
    sql(
        "CREATE TABLE %s (id bigint, data string, category string, ts timestamp) USING iceberg",
        tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s", targetTable);
  }

  @TestTemplate
  public void testInsertAsSelectAppend() {
    insertData(3);
    List<Object[]> expected = currentData();

    sql(
        "CREATE TABLE %s (id bigint, data string, category string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (days(ts), category)",
        targetTable);

    sql(
        "INSERT INTO %s SELECT id, data, category, ts FROM %s ORDER BY ts,category",
        targetTable, tableName);
    assertThat(scalarSql("SELECT count(*) FROM %s", targetTable))
        .as("Should have 15 rows after insert")
        .isEqualTo(3 * 5L);

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", targetTable));
  }

  @TestTemplate
  public void testInsertAsSelectWithBucket() {
    insertData(3);
    List<Object[]> expected = currentData();

    sql(
        "CREATE TABLE %s (id bigint, data string, category string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (bucket(8, data))",
        targetTable);

    IcebergSpark.registerBucketUDF(spark, "iceberg_bucket8", DataTypes.StringType, 8);
    sql(
        "INSERT INTO %s SELECT id, data, category, ts FROM %s ORDER BY iceberg_bucket8(data)",
        targetTable, tableName);
    assertThat(scalarSql("SELECT count(*) FROM %s", targetTable))
        .as("Should have 15 rows after insert")
        .isEqualTo(3 * 5L);

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", targetTable));
  }

  @TestTemplate
  public void testInsertAsSelectWithTruncate() {
    insertData(3);
    List<Object[]> expected = currentData();

    sql(
        "CREATE TABLE %s (id bigint, data string, category string, ts timestamp)"
            + "USING iceberg PARTITIONED BY (truncate(data, 4), truncate(id, 4))",
        targetTable);

    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_string4", DataTypes.StringType, 4);
    IcebergSpark.registerTruncateUDF(spark, "iceberg_truncate_long4", DataTypes.LongType, 4);
    sql(
        "INSERT INTO %s SELECT id, data, category, ts FROM %s "
            + "ORDER BY iceberg_truncate_string4(data),iceberg_truncate_long4(id)",
        targetTable, tableName);
    assertThat(scalarSql("SELECT count(*) FROM %s", targetTable))
        .as("Should have 15 rows after insert")
        .isEqualTo(3 * 5L);

    assertEquals(
        "Row data should match expected",
        expected,
        sql("SELECT * FROM %s ORDER BY id", targetTable));
  }

  private void insertData(int repeatCounter) {
    IntStream.range(0, repeatCounter)
        .forEach(
            i -> {
              sql(
                  "INSERT INTO %s VALUES (13, '1', 'bgd16', timestamp('2021-11-10 11:20:10')),"
                      + "(21, '2', 'bgd13', timestamp('2021-11-10 11:20:10')), "
                      + "(12, '3', 'bgd14', timestamp('2021-11-10 11:20:10')),"
                      + "(222, '3', 'bgd15', timestamp('2021-11-10 11:20:10')),"
                      + "(45, '4', 'bgd16', timestamp('2021-11-10 11:20:10'))",
                  tableName);
            });
  }

  private List<Object[]> currentData() {
    return rowsToJava(spark.sql("SELECT * FROM " + tableName + " order by id").collectAsList());
  }
}
