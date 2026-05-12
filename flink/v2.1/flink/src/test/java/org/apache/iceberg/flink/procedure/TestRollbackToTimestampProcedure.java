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

/** Unit tests for {@link RollbackToTimestampProcedure}. */
public class TestRollbackToTimestampProcedure extends ProcedureTestBase {
  @TestTemplate
  public void testRollbackToTimestampProcedure() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 3; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    List<Long> timestamps = getSnapshotTimestampsSorted(tableName);
    List<Long> snapshotIds = getSnapshotIdsSortedByTimestamps(tableName);

    assertThat(sql("CALL sys.rollback_to_timestamp('%s', %s)", tableName, timestamps.get(1) + 1))
        .containsExactly(Row.of(snapshotIds.get(2), snapshotIds.get(1)));

    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]", "+I[k2, 2024-01-01]");

    assertThat(sql("CALL sys.rollback_to_timestamp('%s', %s)", tableName, timestamps.get(0) + 1))
        .containsExactly(Row.of(snapshotIds.get(1), snapshotIds.get(0)));

    assertThat(sql("select * from T").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");
  }

  @TestTemplate
  public void testNoSnapshotToRollbackTo() throws Exception {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    for (int i = 1; i <= 3; i++) {
      sql("insert into T values('k" + i + "', '2024-01-01')");
      Thread.sleep(100L);
    }

    String tableName = getFullQualifiedTableName("T");

    long firstTimestamp = getFirstSnapshotTimestamp(tableName);

    assertThatThrownBy(
            () -> sql("CALL sys.rollback_to_timestamp('%s', %s)", tableName, firstTimestamp - 1))
        .isInstanceOf(TableException.class)
        .hasMessage(
            "The call method caused an error: Could not find any snapshot whose commit-time earlier than %s.",
            (firstTimestamp - 1));
  }
}
