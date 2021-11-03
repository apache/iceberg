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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.spark.sql.AnalysisException;
import org.junit.After;
import org.junit.Test;

public class TestPartitionSQL extends SparkCatalogTestBase {

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
}
