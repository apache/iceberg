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

import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCopyOnWriteUpdate extends TestUpdate {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  @TestTemplate
  public void testRuntimeFilteringWithReportedPartitioning() {
    createAndInitTable("id INT, dep STRING");
    sql("ALTER TABLE %s ADD PARTITION FIELD dep", tableName);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 3, \"dep\": \"hr\" }");
    createBranchIfNeeded();
    append(
        commitTarget(),
        "{ \"id\": 1, \"dep\": \"hardware\" }\n" + "{ \"id\": 2, \"dep\": \"hardware\" }");

    Map<String, String> sqlConf =
        ImmutableMap.of(
            SQLConf.V2_BUCKETING_ENABLED().key(),
            "true",
            SparkSQLProperties.PRESERVE_DATA_GROUPING,
            "true");

    withSQLConf(sqlConf, () -> sql("UPDATE %s SET id = -1 WHERE id = 2", commitTarget()));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Should have 3 snapshots").hasSize(3);

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    validateCopyOnWrite(currentSnapshot, "1", "1", "1");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "hardware"), row(1, "hardware"), row(1, "hr"), row(3, "hr")),
        sql("SELECT * FROM %s ORDER BY id, dep", selectTarget()));
  }
}
