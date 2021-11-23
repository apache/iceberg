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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Test;

public class TestPartitionSQL extends SparkExtensionsTestBase {

  public TestPartitionSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testShowUnPartition() {
    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    AssertHelpers.assertThrows("table is not partitioned",
        AnalysisException.class, "table is not partitioned",
        () -> sql("SHOW PARTITIONS %s", tableName));
  }

  @Test
  public void testShowOrdinaryPartition() {
    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg PARTITIONED BY (dt)", tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210101"), row("dt=20210102"), row("dt=20210103")),
        sql("SHOW PARTITIONS %s", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName);

    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg PARTITIONED BY (age, dt)", tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(row("age=11/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("age=11/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (age = 11)", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("age=11/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (age = 11, dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("age=10/dt=20210101"), row("age=11/dt=20210102"),
            row("age=12/dt=20210103")),
        sql("SHOW PARTITIONS %s", tableName));
  }

  @Test
  public void testShowImplicitPartition() {
    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg PARTITIONED BY (bucket(4, id), dt)",
        tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    List<Object[]> result = sql("SHOW PARTITIONS %s", tableName);
    System.out.println(result.size());

    assertEquals("result should have expected rows",
        ImmutableList.of(row("id_bucket=0/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("id_bucket=0/dt=20210101"), row("id_bucket=0/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (id_bucket = 0)", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("id_bucket=0/dt=20210102")),
        sql("SHOW PARTITIONS %s PARTITION (id_bucket = 0, dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("id_bucket=0/dt=20210101"), row("id_bucket=0/dt=20210102"),
            row("id_bucket=3/dt=20210103")),
        sql("SHOW PARTITIONS %s", tableName));
  }

  @Test
  public void testShowPartitionUnOrderSchema() {
    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg PARTITIONED BY (dt, age)", tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210102/age=11")),
        sql("SHOW PARTITIONS %s PARTITION (dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210102/age=11")),
        sql("SHOW PARTITIONS %s PARTITION (age = 11)", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210102/age=11")),
        sql("SHOW PARTITIONS %s PARTITION (age = 11, dt = '20210102')", tableName));

    assertEquals("result should have expected rows",
        ImmutableList.of(row("dt=20210101/age=10"), row("dt=20210102/age=11"),
            row("dt=20210103/age=12")),
        sql("SHOW PARTITIONS %s", tableName));
  }

  @Test
  public void testUnSupportPartitionsOps() {
    sql("CREATE TABLE %s (id bigint, age bigint, dt string) USING iceberg PARTITIONED BY (dt, age)", tableName);
    sql("INSERT INTO %s VALUES (1, 10, '20210101'), (2, 11, '20210102'), (3, 12, '20210103')", tableName);

    AssertHelpers.assertThrows("not support add partition", UnsupportedOperationException.class,
        () -> sql("ALTER TABLE %s add partition (age = 12, dt = '20210111')", tableName));

    AssertHelpers.assertThrows("not support drop partition", UnsupportedOperationException.class,
        () -> sql("ALTER TABLE %s drop partition (age = 10, dt = '20210101')", tableName));

  }

  @Test
  public void testShowPartitonWithDateEvolution() {

    sql("CREATE TABLE %s (id bigint, age bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts))",
        tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(),
        sql("SHOW PARTITIONS %s", tableName));

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row("ts_day=1970-01-01"),
            row("ts_day=1970-01-02")),
        sql("SHOW PARTITIONS %s", tableName));

    sql("ALTER TABLE %s drop partition field days(ts)", tableName);

    sql("ALTER TABLE %s add partition field hours(ts)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}\n");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row("ts_day=1970-01-01/ts_hour=null"),
            row("ts_day=1970-01-02/ts_hour=null"),
            row("ts_day=null/ts_hour=17"),
            row("ts_day=null/ts_hour=25")),
        sql("SHOW PARTITIONS %s", tableName));

  }

  @Test
  public void testShowPartitonWithBucketEvolution() {
    sql("CREATE TABLE %s (id bigint, age bigint, ts timestamp) USING iceberg PARTITIONED BY (bucket(2, id))",
        tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(),
        sql("SHOW PARTITIONS %s", tableName));

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(row("id_bucket=0")),
        sql("SHOW PARTITIONS %s", tableName));

    sql("ALTER TABLE %s drop partition field bucket(2, id)", tableName);

    sql("ALTER TABLE %s add partition field bucket(4, id)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 3, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row("id_bucket=0/id_bucket_4=null"),
            row("id_bucket=null/id_bucket_4=3")),
        sql("SHOW PARTITIONS %s", tableName));

    sql("ALTER TABLE %s add partition field years(ts)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 4, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}");

    List<Object[]> sql1 = sql("show partitions %s", tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row("id_bucket=0/id_bucket_4=null/ts_year=null"),
            row("id_bucket=null/id_bucket_4=2/ts_year=0"),
            row("id_bucket=null/id_bucket_4=3/ts_year=null")),
        sql("SHOW PARTITIONS %s", tableName));
  }

  protected void append(String table, String jsonData) {
    append(table, null, jsonData);
  }

  protected void append(String table, String schema, String jsonData) {
    try {
      Dataset<Row> ds = toDS(schema, jsonData);
      ds.coalesce(1).writeTo(table).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException("Failed to write data", e);
    }
  }

  private Dataset<Row> toDS(String schema, String jsonData) {
    List<String> jsonRows = Arrays.stream(jsonData.split("\n"))
        .filter(str -> str.trim().length() > 0)
        .collect(Collectors.toList());
    Dataset<String> jsonDS = spark.createDataset(jsonRows, Encoders.STRING());

    if (schema != null) {
      return spark.read().schema(schema).json(jsonDS);
    } else {
      return spark.read().json(jsonDS);
    }
  }
}
