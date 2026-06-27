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
package org.apache.iceberg.flink.procedure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.TestTemplate;

/** Unit tests for {@link ExpireSnapshotsProcedure}. */
public class TestExpireSnapshotsProcedure extends ProcedureTestBase {
  @TestTemplate
  public void testDefaultOptions() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 4; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    sql("CALL sys.expire_snapshots(`table` => '%s')", tableName);

    // Default options are retain_last = 1, older_than = now - 5 days
    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder(
            "+I[k1, 2024-01-01]", "+I[k2, 2024-01-01]", "+I[k3, 2024-01-01]", "+I[k4, 2024-01-01]");
  }

  @TestTemplate
  public void testOneSnapshotStays() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 4; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    long latestSnapshotId = getLastSnapshotId(tableName);
    long latestSnapshotTime = getLastSnapshotTimestamp(tableName);

    sql(
        "CALL sys.expire_snapshots(`table` => '%s', `older_than` => '%s')",
        tableName, toDateTime(latestSnapshotTime + 10_000));

    List<Long> snapshotIds = getSnapshotIdsSortedByTimestamps(tableName);

    // By default, at least one snapshot stays
    assertThat(snapshotIds).containsExactly(latestSnapshotId);
  }

  @TestTemplate
  public void testTwoSnapshotsStay() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 4; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(1000L);
    }

    String tableName = getFullQualifiedTableName("T");

    List<Long> snapshotTimestamps = getSnapshotTimestampsSorted(tableName);

    assertThat(snapshotTimestamps).hasSize(4);

    sql(
        "CALL sys.expire_snapshots(`table` => '%s', `older_than` => '%s')",
        tableName, toDateTime(snapshotTimestamps.get(2)));

    List<Long> snapshotIds = getSnapshotIdsSortedByTimestamps(tableName);

    // Only last two snapshots should stay
    assertThat(snapshotIds).hasSize(2);
  }

  @TestTemplate
  public void testRetainLastAllStay() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 4; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    long latestSnapshotTime = getLastSnapshotTimestamp(tableName);

    sql(
        "CALL sys.expire_snapshots(`table` => '%s', `older_than` => '%s', `retain_last` => %s)",
        tableName, toDateTime(latestSnapshotTime + 10_000), 4);

    List<Long> snapshotIds = getSnapshotIdsSortedByTimestamps(tableName);

    // All 4 snapshots stay
    assertThat(snapshotIds).hasSize(4);
  }

  @TestTemplate
  public void testRetainLastNoneStays() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 4; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    long latestSnapshotTime = getLastSnapshotTimestamp(tableName);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL sys.expire_snapshots(`table` => '%s', `older_than` => '%s', `retain_last` => %s)",
                    tableName, toDateTime(latestSnapshotTime + 10_000), 0))
        .isInstanceOf(TableException.class)
        .hasMessage(
            "The call method caused an error: Number of snapshots to retain must be at least 1, cannot be: 0.");
  }
}
