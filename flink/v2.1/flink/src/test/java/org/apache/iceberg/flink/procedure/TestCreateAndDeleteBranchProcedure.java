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

/** Unit tests for {@link CreateBranchProcedure} and {@link DeleteBranchProcedure}. */
public class TestCreateAndDeleteBranchProcedure extends ProcedureTestBase {
  @TestTemplate
  public void testCreateAndDeleteBranchFromCurrentSnapshot() {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    sql("insert into T values('k', '2024-01-01')");
    sql("insert into T values('k2', '2024-01-02')");
    sql("insert into T values('k3', '2024-01-03')");

    String tableName = getFullQualifiedTableName("T");

    sql("CALL sys.create_branch('%s', 'branch1')", tableName);

    sql("insert into T /*+ OPTIONS('branch'='branch1') */ values('k4', '2024-01-04')");

    assertThat(
            sql("select * from T /*+ OPTIONS('branch'='branch1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder(
            "+I[k4, 2024-01-04]", "+I[k3, 2024-01-03]", "+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");

    sql("CALL sys.delete_branch('%s', 'branch1')", tableName);

    assertThat(sql("select * from T ").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k3, 2024-01-03]", "+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");
  }

  @TestTemplate
  public void testDeleteNonExistentBranch() {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");

    String tableName = getFullQualifiedTableName("T");

    assertThatThrownBy(() -> sql("CALL sys.delete_branch('%s', 'branch1')", tableName))
        .isInstanceOf(TableException.class)
        .hasMessage("The call method caused an error: Branch does not exist: branch1.");
  }

  @TestTemplate
  public void testCreateEmptyBranch() {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");

    String tableName = getFullQualifiedTableName("T");

    sql("CALL sys.create_branch('%s', 'branch1')", tableName);

    sql("insert into T /*+ OPTIONS('branch'='branch1') */ values('k1', '2024-01-01')");

    assertThat(
            sql("select * from T /*+ OPTIONS('branch'='branch1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k1, 2024-01-01]");

    assertThat(sql("select * from T ").stream().map(Row::toString)).isEmpty();
  }

  @TestTemplate
  public void testCreateAndDeleteBranchFromTag() {
    sql(
        "CREATE TABLE T ("
            + " k STRING,"
            + " dt STRING,"
            + " PRIMARY KEY (k, dt) NOT ENFORCED"
            + ") PARTITIONED BY (dt)");
    sql("insert into T values('k', '2024-01-01')");
    sql("insert into T values('k2', '2024-01-02')");
    sql("insert into T values('k3', '2024-01-03')");

    String tableName = getFullQualifiedTableName("T");

    sql("CALL sys.create_tag('%s', 'tag1')", tableName);

    sql("CALL sys.create_branch('%s', 'branch1', 'tag1')", tableName);

    sql("insert into T /*+ OPTIONS('branch'='branch1') */ values('k4', '2024-01-04')");

    assertThat(
            sql("select * from T /*+ OPTIONS('branch'='branch1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder(
            "+I[k4, 2024-01-04]", "+I[k3, 2024-01-03]", "+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");

    sql("CALL sys.delete_branch('%s', 'branch1')", tableName);

    assertThat(sql("select * from T /*+ OPTIONS('tag'='tag1') */").stream().map(Row::toString))
        .containsExactlyInAnyOrder("+I[k3, 2024-01-03]", "+I[k2, 2024-01-02]", "+I[k, 2024-01-01]");
  }
}
