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

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.Spark3VersionUtil;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

public class TestDeleteFrom extends SparkCatalogTestBase {
  public TestDeleteFrom(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDeleteFromUnpartitionedTable() {
    // This test fails in Spark 3.1. `canDeleteWhere` was added to `SupportsDelete` in Spark 3.1,
    // but logic to rewrite the query if `canDeleteWhere` returns false was left to be implemented
    // later.
    Assume.assumeTrue(Spark3VersionUtil.isSpark30());
    // set the shuffle partitions to 1 to force the write to use a single task and produce 1 file
    String originalParallelism = spark.conf().get("spark.sql.shuffle.partitions");
    spark.conf().set("spark.sql.shuffle.partitions", "1");
    try {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
      sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      assertEquals("Should have expected rows",
          ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
          sql("SELECT * FROM %s ORDER BY id", tableName));

      AssertHelpers.assertThrows("Should not delete when not all rows of a file match the filter",
          IllegalArgumentException.class, "Failed to cleanly delete data files",
          () -> sql("DELETE FROM %s WHERE id < 2", tableName));

      sql("DELETE FROM %s WHERE id < 4", tableName);

      Assert.assertEquals("Should have no rows after successful delete",
          0L, scalarSql("SELECT count(1) FROM %s", tableName));

    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", originalParallelism);
    }
  }

  @Test
  public void testDeleteFromPartitionedTable() {
    // This test fails in Spark 3.1. `canDeleteWhere` was added to `SupportsDelete` in Spark 3.1,
    // but logic to rewrite the query if `canDeleteWhere` returns false was left to be implemented
    // later.
    Assume.assumeTrue(Spark3VersionUtil.isSpark30());
    // set the shuffle partitions to 1 to force the write to use a single task and produce 1 file per partition
    String originalParallelism = spark.conf().get("spark.sql.shuffle.partitions");
    spark.conf().set("spark.sql.shuffle.partitions", "1");
    try {
      sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg " +
          "PARTITIONED BY (truncate(id, 2))", tableName);
      sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

      assertEquals("Should have 3 rows in 2 partitions",
          ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
          sql("SELECT * FROM %s ORDER BY id", tableName));

      AssertHelpers.assertThrows("Should not delete when not all rows of a file match the filter",
          IllegalArgumentException.class, "Failed to cleanly delete data files",
          () -> sql("DELETE FROM %s WHERE id > 2", tableName));

      sql("DELETE FROM %s WHERE id < 2", tableName);

      assertEquals("Should have two rows in the second partition",
          ImmutableList.of(row(2L, "b"), row(3L, "c")),
          sql("SELECT * FROM %s ORDER BY id", tableName));

    } finally {
      spark.conf().set("spark.sql.shuffle.partitions", originalParallelism);
    }
  }

  @Test
  public void testDeleteFromWhereFalse() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b"), row(3L, "c")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should have 1 snapshot", 1, Iterables.size(table.snapshots()));

    sql("DELETE FROM %s WHERE false", tableName);

    table.refresh();

    Assert.assertEquals("Delete should not produce a new snapshot", 1, Iterables.size(table.snapshots()));
  }
}
