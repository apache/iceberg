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

import org.apache.flink.types.Row;
import org.junit.jupiter.api.TestTemplate;

/** Unit tests for {@link CreateTagProcedure} and {@link DeleteTagProcedure}. */
public class TestCreateAndDeleteTagProcedure extends ProcedureTestBase {
  @TestTemplate
  public void testCreateAndDeleteTags() throws Exception {
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

    sql("CALL sys.create_tag('%s', 'tag1')", tableName);

    assertThat(sql("select * from T /*+ OPTIONS('tag'='tag1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder(
            "+I[k4, 2024-01-01]", "+I[k3, 2024-01-01]", "+I[k2, 2024-01-01]", "+I[k1, 2024-01-01]");

    long firstSnapshotId = getFirstSnapshotId(tableName);

    sql("CALL sys.create_tag('%s', 'tag2', %s)", tableName, firstSnapshotId);

    assertThat(sql("select * from T /*+ OPTIONS('tag'='tag2') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");

    sql("CALL sys.delete_tag('%s', 'tag2')", tableName);

    assertThatThrownBy(
            () -> sql("select * from T /*+ OPTIONS('tag'='tag2') */").stream().map(Row::toString))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to collect table result");
  }

  @TestTemplate
  public void testCreateAndDeleteTagsWithNamedParameters() throws Exception {
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

    sql("CALL sys.create_tag(`table` => '%s', `tag` => 'tag1')", tableName);

    assertThat(sql("select * from T /*+ OPTIONS('tag'='tag1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder(
            "+I[k4, 2024-01-01]", "+I[k3, 2024-01-01]", "+I[k2, 2024-01-01]", "+I[k1, 2024-01-01]");

    long firstSnapshotId = getFirstSnapshotId(tableName);

    sql(
        "CALL sys.create_tag(`table` => '%s', `tag` => 'tag2', `snapshot_id` => %s)",
        tableName, firstSnapshotId);

    assertThat(sql("select * from T /*+ OPTIONS('tag'='tag2') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");

    sql("CALL sys.delete_tag(`table` => '%s', `tag` => 'tag2')", tableName);

    assertThatThrownBy(
            () -> sql("select * from T /*+ OPTIONS('tag'='tag2') */").stream().map(Row::toString))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Failed to collect table result");
  }
}
