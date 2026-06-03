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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDropPartition extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, formatVersion = {3}")
  protected static Object[][] parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    for (Object[] catalogParams : CatalogTestBase.parameters()) {
      for (int version : TestHelpers.ALL_VERSIONS) {
        parameters.add(
            new Object[] {catalogParams[0], catalogParams[1], catalogParams[2], version});
      }
    }

    return parameters.toArray(new Object[0][]);
  }

  @Parameter(index = 3)
  private int formatVersion;

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDropPartitionIdentity() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (data) "
            + "TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);

    sql("ALTER TABLE %s DROP PARTITION (data = 'b')", tableName);

    List<Object[]> remaining = sql("SELECT id, data FROM %s ORDER BY id", tableName);
    assertThat(remaining).containsExactly(new Object[] {1L, "a"}, new Object[] {3L, "c"});
  }

  @TestTemplate
  public void testDropPartitionMultipleColumns() {
    sql(
        "CREATE TABLE %s (id bigint, region string, dt string) USING iceberg "
            + "PARTITIONED BY (region, dt) TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql(
        "INSERT INTO %s VALUES "
            + "(1, 'us', '2024-01-01'), (2, 'us', '2024-01-02'), (3, 'eu', '2024-01-01')",
        tableName);

    sql("ALTER TABLE %s DROP PARTITION (region = 'us', dt = '2024-01-01')", tableName);

    List<Object[]> remaining = sql("SELECT id, region, dt FROM %s ORDER BY id", tableName);
    assertThat(remaining)
        .containsExactly(
            new Object[] {2L, "us", "2024-01-02"}, new Object[] {3L, "eu", "2024-01-01"});
  }

  @TestTemplate
  public void testDropPartitionAtomicMultipleSpecs() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (data) "
            + "TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    long snapshotsBefore = countSnapshots();
    sql("ALTER TABLE %s DROP PARTITION (data = 'b'), PARTITION (data = 'd')", tableName);
    long snapshotsAfter = countSnapshots();

    List<Object[]> remaining = sql("SELECT id, data FROM %s ORDER BY id", tableName);
    assertThat(remaining).containsExactly(new Object[] {1L, "a"}, new Object[] {3L, "c"});
    assertThat(snapshotsAfter - snapshotsBefore)
        .as("Dropping multiple partitions should commit one snapshot")
        .isEqualTo(1L);
  }

  @TestTemplate
  public void testDropPartitionBucketTransform() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (bucket(2, id)) "
            + "TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);

    long before = (Long) sql("SELECT count(*) FROM %s", tableName).get(0)[0];
    sql("ALTER TABLE %s DROP PARTITION (id_bucket = 0)", tableName);
    long after = (Long) sql("SELECT count(*) FROM %s", tableName).get(0)[0];

    // With 2 buckets and 4 distinct ids, both buckets are populated; dropping one removes
    // strictly fewer than all rows
    assertThat(after).as("Should remove at least one row from bucket 0").isLessThan(before);
    assertThat(after).as("Should not remove every row (the other bucket survives)").isPositive();
  }

  @TestTemplate
  public void testDropPartitionDoesNotExist() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (data) "
            + "TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    assertThatThrownBy(() -> sql("ALTER TABLE %s DROP PARTITION (data = 'missing')", tableName))
        .hasMessageContaining("PARTITION (`data` = missing) cannot be found in table");

    List<Object[]> remaining = sql("SELECT id, data FROM %s", tableName);
    assertThat(remaining).containsExactly(new Object[] {1L, "a"});
  }

  @TestTemplate
  public void testDropPartitionIfExists() {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (data) "
            + "TBLPROPERTIES ('%s'='%d')",
        tableName, TableProperties.FORMAT_VERSION, formatVersion);
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    sql("ALTER TABLE %s DROP IF EXISTS PARTITION (data = 'missing')", tableName);

    List<Object[]> remaining = sql("SELECT id, data FROM %s", tableName);
    assertThat(remaining).containsExactly(new Object[] {1L, "a"});
  }

  @TestTemplate
  public void testDropPartitionIdentityTimestamp() {
    withUtcSession(
        () -> {
          sql(
              "CREATE TABLE %s (id bigint, ts timestamp) USING iceberg PARTITIONED BY (ts) "
                  + "TBLPROPERTIES ('%s'='%d')",
              tableName, TableProperties.FORMAT_VERSION, formatVersion);
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, CAST('2024-01-15 10:00:00' AS TIMESTAMP)), "
                  + "(2, CAST('2024-02-15 10:00:00' AS TIMESTAMP)), "
                  + "(3, CAST('2024-03-15 10:00:00' AS TIMESTAMP))",
              tableName);

          sql("ALTER TABLE %s DROP PARTITION (ts = '2024-02-15 10:00:00')", tableName);

          List<Object[]> remaining = sql("SELECT id FROM %s ORDER BY id", tableName);
          assertThat(remaining).containsExactly(new Object[] {1L}, new Object[] {3L});
        });
  }

  @TestTemplate
  public void testDropPartitionDayTransform() {
    withUtcSession(
        () -> {
          sql(
              "CREATE TABLE %s (id bigint, ts timestamp) USING iceberg PARTITIONED BY (days(ts)) "
                  + "TBLPROPERTIES ('%s'='%d')",
              tableName, TableProperties.FORMAT_VERSION, formatVersion);
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, CAST('2024-01-15 10:00:00' AS TIMESTAMP)), "
                  + "(2, CAST('2024-02-15 10:00:00' AS TIMESTAMP)), "
                  + "(3, CAST('2024-02-15 22:00:00' AS TIMESTAMP)), "
                  + "(4, CAST('2024-03-15 10:00:00' AS TIMESTAMP))",
              tableName);

          sql("ALTER TABLE %s DROP PARTITION (ts_day = '2024-02-15')", tableName);

          List<Object[]> remaining = sql("SELECT id FROM %s ORDER BY id", tableName);
          assertThat(remaining).containsExactly(new Object[] {1L}, new Object[] {4L});
        });
  }

  @TestTemplate
  public void testDropPartitionHourTransform() {
    withUtcSession(
        () -> {
          sql(
              "CREATE TABLE %s (id bigint, ts timestamp) USING iceberg PARTITIONED BY (hours(ts)) "
                  + "TBLPROPERTIES ('%s'='%d')",
              tableName, TableProperties.FORMAT_VERSION, formatVersion);
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, CAST('2024-01-15 10:00:00' AS TIMESTAMP)), "
                  + "(2, CAST('2024-01-15 11:15:00' AS TIMESTAMP)), "
                  + "(3, CAST('2024-01-15 11:45:00' AS TIMESTAMP)), "
                  + "(4, CAST('2024-01-15 12:00:00' AS TIMESTAMP))",
              tableName);

          sql("ALTER TABLE %s DROP PARTITION (ts_hour = '2024-01-15-11')", tableName);

          List<Object[]> remaining = sql("SELECT id FROM %s ORDER BY id", tableName);
          assertThat(remaining).containsExactly(new Object[] {1L}, new Object[] {4L});
        });
  }

  @TestTemplate
  public void testDropPartitionMonthTransform() {
    withUtcSession(
        () -> {
          sql(
              "CREATE TABLE %s (id bigint, ts timestamp) USING iceberg PARTITIONED BY (months(ts)) "
                  + "TBLPROPERTIES ('%s'='%d')",
              tableName, TableProperties.FORMAT_VERSION, formatVersion);
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, CAST('2024-01-01 10:00:00' AS TIMESTAMP)), "
                  + "(2, CAST('2024-01-15 10:00:00' AS TIMESTAMP)), "
                  + "(3, CAST('2024-02-15 10:00:00' AS TIMESTAMP))",
              tableName);

          sql("ALTER TABLE %s DROP PARTITION (ts_month = '2024-01')", tableName);

          List<Object[]> remaining = sql("SELECT id FROM %s ORDER BY id", tableName);
          assertThat(remaining).containsExactly(new Object[] {3L});
        });
  }

  @TestTemplate
  public void testDropPartitionYearTransform() {
    withUtcSession(
        () -> {
          sql(
              "CREATE TABLE %s (id bigint, ts timestamp) USING iceberg PARTITIONED BY (years(ts)) "
                  + "TBLPROPERTIES ('%s'='%d')",
              tableName, TableProperties.FORMAT_VERSION, formatVersion);
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, CAST('2024-01-15 10:00:00' AS TIMESTAMP)), "
                  + "(2, CAST('2024-07-15 10:00:00' AS TIMESTAMP)), "
                  + "(3, CAST('2025-01-15 10:00:00' AS TIMESTAMP))",
              tableName);

          sql("ALTER TABLE %s DROP PARTITION (ts_year = '2024')", tableName);

          List<Object[]> remaining = sql("SELECT id FROM %s ORDER BY id", tableName);
          assertThat(remaining).containsExactly(new Object[] {3L});
        });
  }

  private long countSnapshots() {
    List<Object[]> snapshots = sql("SELECT count(*) FROM %s.snapshots", tableName);
    return (Long) snapshots.get(0)[0];
  }

  private void withUtcSession(Runnable action) {
    String key = "spark.sql.session.timeZone";
    String prev = spark.conf().getOption(key).getOrElse(() -> null);
    spark.conf().set(key, "UTC");
    try {
      action.run();
    } finally {
      if (prev != null) {
        spark.conf().set(key, prev);
      } else {
        spark.conf().unset(key);
      }
    }
  }
}
