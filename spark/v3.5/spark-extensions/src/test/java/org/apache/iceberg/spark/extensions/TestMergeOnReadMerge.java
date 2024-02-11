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
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Encoders;
import org.junit.Test;

public class TestMergeOnReadMerge extends TestMerge {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.FORMAT_VERSION,
        "2",
        TableProperties.MERGE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  @Test
  public void testMergeDeleteFileGranularity() {
    checkMergeDeleteGranularity(DeleteGranularity.FILE);
  }

  @Test
  public void testMergeDeletePartitionGranularity() {
    checkMergeDeleteGranularity(DeleteGranularity.PARTITION);
  }

  private void checkMergeDeleteGranularity(DeleteGranularity deleteGranularity) {
    createAndInitTable("id INT, dep STRING", "PARTITIONED BY (dep)", null /* empty */);

    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        tableName, TableProperties.DELETE_GRANULARITY, deleteGranularity);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"hr\" }\n" + "{ \"id\": 4, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 1, \"dep\": \"it\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"it\" }\n" + "{ \"id\": 4, \"dep\": \"it\" }");

    createBranchIfNeeded();

    createOrReplaceView("source", ImmutableList.of(1, 3, 5), Encoders.INT());

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.value "
            + "WHEN MATCHED THEN "
            + " DELETE "
            + "WHEN NOT MATCHED THEN "
            + " INSERT (id, dep) VALUES (-1, 'other')",
        commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSize(5);

    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    String expectedDeleteFilesCount = deleteGranularity == DeleteGranularity.FILE ? "4" : "2";
    validateMergeOnRead(currentSnapshot, "3", expectedDeleteFilesCount, "1");

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(-1, "other"), row(2, "hr"), row(2, "it"), row(4, "hr"), row(4, "it")),
        sql("SELECT * FROM %s ORDER BY id ASC, dep ASC", selectTarget()));
  }
}
