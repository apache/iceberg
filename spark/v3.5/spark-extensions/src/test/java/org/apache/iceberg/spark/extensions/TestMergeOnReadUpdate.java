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
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.Test;

public class TestMergeOnReadUpdate extends TestUpdate {

  public TestMergeOnReadUpdate(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch,
      PlanningMode planningMode) {
    super(
        catalogName,
        implementation,
        config,
        fileFormat,
        vectorized,
        distributionMode,
        fanoutEnabled,
        branch,
        planningMode);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.UPDATE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  @Test
  public void testUpdateFileGranularity() {
    checkUpdateFileGranularity(DeleteGranularity.FILE);
  }

  @Test
  public void testUpdatePartitionGranularity() {
    checkUpdateFileGranularity(DeleteGranularity.PARTITION);
  }

  private void checkUpdateFileGranularity(DeleteGranularity deleteGranularity) {
    createAndInitTable("id INT, dep STRING", "PARTITIONED BY (dep)", null /* empty */);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.DELETE_GRANULARITY, deleteGranularity);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"hr\" }\n" + "{ \"id\": 4, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 1, \"dep\": \"it\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"it\" }\n" + "{ \"id\": 4, \"dep\": \"it\" }");

    createBranchIfNeeded();

    sql("UPDATE %s SET id = id - 1 WHERE id = 1 OR id = 3", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSize(5);

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    String expectedDeleteFilesCount = deleteGranularity == DeleteGranularity.FILE ? "4" : "2";
    validateMergeOnRead(currentSnapshot, "2", expectedDeleteFilesCount, "2");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(0, "hr"),
            row(2, "hr"),
            row(2, "hr"),
            row(4, "hr"),
            row(0, "it"),
            row(2, "it"),
            row(2, "it"),
            row(4, "it")),
        sql("SELECT * FROM %s ORDER BY dep ASC, id ASC", selectTarget()));
  }
}
