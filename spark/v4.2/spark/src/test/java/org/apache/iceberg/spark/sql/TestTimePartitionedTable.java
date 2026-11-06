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

import java.time.LocalTime;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTimePartitionedTable extends TestBaseWithCatalog {

  @BeforeEach
  public void enableTimeType() {
    // Spark 4.1 gates the TIME data type in SQL behind an internal flag
    spark.conf().set("spark.sql.timeType.enabled", "true");
  }

  @AfterEach
  public void dropTestTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    spark.conf().unset("spark.sql.timeType.enabled");
  }

  @TestTemplate
  public void testSelectFromTimePartitionedTable() {
    sql("CREATE TABLE %s (id INT, t TIME(6)) USING iceberg PARTITIONED BY (t)", tableName);
    sql(
        "INSERT INTO %s VALUES (1, TIME '10:20:30.123456'), (2, TIME '23:59:59.999999')",
        tableName);

    // identity-partitioned time columns are served from partition metadata constants, which
    // must be converted from Iceberg microseconds to the nanoseconds Spark expects
    assertEquals(
        "Should return correct time values",
        ImmutableList.of(
            row(1, LocalTime.of(10, 20, 30, 123_456_000)),
            row(2, LocalTime.of(23, 59, 59, 999_999_000))),
        sql("SELECT id, t FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testPartitionMetadataColumnWithTimePartition() {
    sql("CREATE TABLE %s (id INT, t TIME(6)) USING iceberg PARTITIONED BY (t)", tableName);
    sql("INSERT INTO %s VALUES (1, TIME '10:20:30.123456')", tableName);

    // _partition is a metadata struct column; a nested time field must disable vectorized
    // Parquet reads and be converted to nanoseconds
    assertEquals(
        "Should return correct partition value",
        ImmutableList.of(row(LocalTime.of(10, 20, 30, 123_456_000))),
        sql("SELECT _partition.t FROM %s", tableName));
  }
}
