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

import org.apache.flink.table.api.TableException;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.TestTemplate;

/** Unit tests for {@link TestRollbackToProcedure}. */
public class TestRollbackToProcedure extends ProcedureTestBase {
  @TestTemplate
  public void testRollbackToSnapshot() throws Exception {
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
    long firstSnapshotId = getFirstSnapshotId(tableName);

    assertThat(
            sql(
                "CALL sys.rollback_to(`table` => '%s', `snapshot_id` => %s)",
                tableName, firstSnapshotId))
        .containsExactly(Row.of(latestSnapshotId, firstSnapshotId));

    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");
  }

  @TestTemplate
  public void testRollbackToTag() throws Exception {
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
    long firstSnapshotId = getFirstSnapshotId(tableName);

    sql("CALL sys.create_tag('%s', 'tag1', %s)", tableName, firstSnapshotId);

    assertThat(sql("CALL sys.rollback_to(`table` => '%s', `tag` => 'tag1')", tableName))
        .containsExactly(Row.of(latestSnapshotId, firstSnapshotId));

    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");
  }

  @TestTemplate
  public void testRollbackToTagAndSnapshot() throws Exception {
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
    long firstSnapshotId = getFirstSnapshotId(tableName);

    sql("CALL sys.create_tag('%s', 'tag1', %s)", tableName, firstSnapshotId);

    assertThat(
            sql(
                "CALL sys.rollback_to(`table` => '%s', `tag` => 'tag1', `snapshot_id` => %s)",
                tableName, firstSnapshotId))
        .containsExactly(Row.of(latestSnapshotId, firstSnapshotId));

    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");
  }

  @TestTemplate
  public void testRollbackToTagAndSnapshotIncompatible() throws Exception {
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
    long firstSnapshotId = getFirstSnapshotId(tableName);

    sql("CALL sys.create_tag('%s', 'tag1', %s)", tableName, firstSnapshotId);

    assertThatThrownBy(
            () ->
                sql(
                    "CALL sys.rollback_to(`table` => '%s', `tag` => 'tag1', `snapshot_id` => %s)",
                    tableName, latestSnapshotId))
        .isInstanceOf(TableException.class)
        .hasMessage(
            "The call method caused an error: Snapshot with provided snapshot id is not the same snapshot provided tag refers to. Please specify a tag or a snapshot id, or be sure both refer to the same snapshot.");
  }

  @TestTemplate
  public void testRollbackToEmptyArgs() throws Exception {
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

    assertThatThrownBy(() -> sql("CALL sys.rollback_to(`table` => '%s')", tableName))
        .isInstanceOf(TableException.class)
        .hasMessage(
            "The call method caused an error: No arguments to rollback to. Please specify a tag or a snapshot id.");
  }
}
