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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Test;

public class TestMetadataTableSQL extends SparkExtensionsTestBase {

  public TestMetadataTableSQL(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testPartitionDateMetaDataTable() {

    sql("CREATE TABLE %s (id bigint, age bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts))",
        tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(),
        sqlPartitions("SELECT * FROM %s.partitions", tableName));

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row(new Date(70, 0, 1)),
            row(new Date(70, 0, 2))),
        sqlPartitions("SELECT * FROM %s.partitions", tableName));

    sql("ALTER TABLE %s drop partition field days(ts)", tableName);

    sql("ALTER TABLE %s add partition field hours(ts)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}\n");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row(null, 17),
            row(null, 25),
            row(new Date(70, 0, 1, 0, 0, 0), null),
            row(new Date(70, 0, 2, 0, 0, 0), null)),
        sqlPartitions("select * from %s.partitions", tableName));

    sql("ALTER TABLE %s drop partition field hours(ts)", tableName);

    sql("ALTER TABLE %s add partition field years(ts)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}\n");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row(null, null, 0),
            row(null, 17, null),
            row(null, 25, null),
            row(new Date(70, 0, 1, 0, 0, 0), null, null),
            row(new Date(70, 0, 2, 0, 0, 0), null, null)),
        sqlPartitions("select * from %s.partitions", tableName));
  }

  @Test
  public void testPartitionBucketMetaDataTable() {

    sql("CREATE TABLE %s (id bigint, age bigint, ts timestamp) USING iceberg PARTITIONED BY (bucket(2, id))",
        tableName);

    assertEquals("result should have expected rows",
        ImmutableList.of(),
        sqlPartitions("SELECT * FROM %s.partitions", tableName));

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 1, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}\n" +
            "{ \"id\": 2, \"age\": 11, \"ts\": \"1970-01-02 09:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row(0)),
        sqlPartitions("SELECT * FROM %s.partitions", tableName));

    sql("ALTER TABLE %s drop partition field bucket(2, id)", tableName);

    sql("ALTER TABLE %s add partition field bucket(4, id)", tableName);

    append(tableName, "id bigint, age bigint, ts timestamp",
        "{ \"id\": 3, \"age\": 10, \"ts\": \"1970-01-02 01:00:00\"}");

    assertEquals("result should have expected rows",
        ImmutableList.of(
            row(null, 3),
            row(0, null)),
        sqlPartitions("select * from %s.partitions", tableName));
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

  private List<Object[]> sqlPartitions(String query, Object... args) {
    List<Row> rows = spark.sql(String.format(query, args))
        .orderBy("partition")
        .select("partition").collectAsList();
    if (rows.size() < 1) {
      return ImmutableList.of();
    }

    List<Row> result = rows.stream().map(row -> (Row) row.get(0)).collect(Collectors.toList());
    return rowsToJava(result);
  }
}
