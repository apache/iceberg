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
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestPartitionedWritesToWapBranch extends PartitionedWritesTestBase {

  private static final String BRANCH = "test";

  @BeforeEach
  @Override
  public void createTables() {
    spark.conf().set(SparkSQLProperties.WAP_BRANCH, BRANCH);
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (truncate(id, 3)) OPTIONS (%s = 'true')",
        tableName, TableProperties.WRITE_AUDIT_PUBLISH_ENABLED);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @AfterEach
  @Override
  public void removeTables() {
    super.removeTables();
    spark.conf().unset(SparkSQLProperties.WAP_BRANCH);
    spark.conf().unset(SparkSQLProperties.WAP_ID);
  }

  @Override
  protected String commitTarget() {
    return tableName;
  }

  @Override
  protected String selectTarget() {
    return String.format("%s VERSION AS OF '%s'", tableName, BRANCH);
  }

  @TestTemplate
  public void testBranchAndWapBranchCannotBothBeSetForWrite() {
    Table table = validationCatalog.loadTable(tableIdent);
    table.manageSnapshots().createBranch("test2", table.refs().get(BRANCH).snapshotId()).commit();
    sql("REFRESH TABLE " + tableName);
    Assertions.assertThatThrownBy(
            () -> sql("INSERT INTO %s.branch_test2 VALUES (4, 'd')", tableName))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot write to both branch and WAP branch, but got branch [test2] and WAP branch [%s]",
            BRANCH);
  }

  @TestTemplate
  public void testWapIdAndWapBranchCannotBothBeSetForWrite() {
    String wapId = UUID.randomUUID().toString();
    spark.conf().set(SparkSQLProperties.WAP_ID, wapId);
    Assertions.assertThatThrownBy(() -> sql("INSERT INTO %s VALUES (4, 'd')", tableName))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot set both WAP ID and branch, but got ID [%s] and branch [%s]", wapId, BRANCH);
  }

  @Override
  protected void assertPartitionMetadata(
      String tableName, List<Object[]> expected, String... selectPartitionColumns) {
    // Cannot read from the .partitions table newly written data into the WAP branch. See
    // https://github.com/apache/iceberg/issues/7297 for more details.
  }
}
