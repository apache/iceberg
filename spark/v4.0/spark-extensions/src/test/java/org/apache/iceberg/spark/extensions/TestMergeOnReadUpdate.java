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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMergeOnReadUpdate extends TestUpdate {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.UPDATE_MODE, RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  @TestTemplate
  public void testUpdateFileGranularity() {
    assumeThat(formatVersion).isEqualTo(2);
    checkUpdateFileGranularity(DeleteGranularity.FILE);
  }

  @TestTemplate
  public void testUpdatePartitionGranularity() {
    assumeThat(formatVersion).isEqualTo(2);
    checkUpdateFileGranularity(DeleteGranularity.PARTITION);
  }

  @TestTemplate
  public void testPositionDeletesAreMaintainedDuringUpdate() {
    assumeThat(formatVersion).isEqualTo(2);
    // Range distribution will produce partition scoped deletes which will not be cleaned up
    assumeThat(distributionMode).isNotEqualToIgnoringCase("range");

    checkUpdateFileGranularity(DeleteGranularity.FILE);
    sql("UPDATE %s SET id = id + 1 WHERE id = 4", commitTarget());
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    String expectedDeleteFilesCount = "2";
    validateMergeOnRead(currentSnapshot, "2", expectedDeleteFilesCount, "2");

    assertThat(currentSnapshot.removedDeleteFiles(table.io())).hasSize(2);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(0, "hr"),
            row(2, "hr"),
            row(2, "hr"),
            row(5, "hr"),
            row(0, "it"),
            row(2, "it"),
            row(2, "it"),
            row(5, "it")),
        sql("SELECT * FROM %s ORDER BY dep ASC, id ASC", selectTarget()));
  }

  @TestTemplate
  public void testUnpartitionedPositionDeletesAreMaintainedDuringUpdate() {
    assumeThat(formatVersion).isEqualTo(2);
    // Range distribution will produce partition scoped deletes which will not be cleaned up
    assumeThat(distributionMode).isNotEqualToIgnoringCase("range");
    initTable("", DeleteGranularity.FILE);

    sql("UPDATE %s SET id = id - 1 WHERE id = 1 OR id = 3", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).hasSize(5);
    Snapshot currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    String expectedDeleteFilesCount = "4";
    validateMergeOnRead(currentSnapshot, "1", expectedDeleteFilesCount, "1");
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

    sql("UPDATE %s SET id = id + 1 WHERE id = 4", commitTarget());
    table.refresh();
    currentSnapshot = SnapshotUtil.latestSnapshot(table, branch);
    expectedDeleteFilesCount = "2";

    validateMergeOnRead(currentSnapshot, "1", expectedDeleteFilesCount, "1");
    assertThat(currentSnapshot.removedDeleteFiles(table.io())).hasSize(2);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(0, "hr"),
            row(2, "hr"),
            row(2, "hr"),
            row(5, "hr"),
            row(0, "it"),
            row(2, "it"),
            row(2, "it"),
            row(5, "it")),
        sql("SELECT * FROM %s ORDER BY dep ASC, id ASC", selectTarget()));
  }

  private void checkUpdateFileGranularity(DeleteGranularity deleteGranularity) {
    initTable("PARTITIONED BY (dep)", deleteGranularity);

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

  @TestTemplate
  public void testUpdateWithDVAndHistoricalPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);
    createTableWithDeleteGranularity(
        "id INT, dep STRING", "PARTITIONED BY (dep)", DeleteGranularity.PARTITION);
    createBranchIfNeeded();
    append(
        commitTarget(),
        "{ \"id\": 1, \"dep\": \"hr\" }\n"
            + "{ \"id\": 2, \"dep\": \"hr\" }\n"
            + "{ \"id\": 3, \"dep\": \"hr\" }");
    append(
        commitTarget(),
        "{ \"id\": 4, \"dep\": \"hr\" }\n"
            + "{ \"id\": 5, \"dep\": \"hr\" }\n"
            + "{ \"id\": 6, \"dep\": \"hr\" }");

    // Produce partition scoped deletes for the two modified files
    sql("UPDATE %s SET id = id - 1 WHERE id = 1 or id = 4", commitTarget());

    // Produce 1 file-scoped deletes for the second update
    Map<String, String> fileGranularityProps =
        ImmutableMap.of(TableProperties.DELETE_GRANULARITY, DeleteGranularity.FILE.toString());
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(fileGranularityProps));
    sql("UPDATE %s SET id = id + 2 WHERE id = 5", commitTarget());

    Map<String, String> updateFormatProperties =
        ImmutableMap.of(TableProperties.FORMAT_VERSION, "3");
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES (%s)",
        tableName, tablePropsAsString(updateFormatProperties));

    // Produce a DV which will contain 3 positions from the second data file
    // 2 existing deleted positions from the earlier file-scoped and partition-scoped deletes
    // and 1 new deleted position
    sql("UPDATE %s SET id = id + 1 where id = 6", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Set<DeleteFile> deleteFiles =
        TestHelpers.deleteFiles(table, SnapshotUtil.latestSnapshot(table, branch));
    List<DeleteFile> dvs =
        deleteFiles.stream().filter(ContentFileUtil::isDV).collect(Collectors.toList());
    assertThat(dvs).hasSize(1);
    assertThat(dvs.get(0).recordCount()).isEqualTo(3);
    assertThat(dvs).allMatch(dv -> FileFormat.fromFileName(dv.location()) == FileFormat.PUFFIN);
  }

  private void initTable(String partitionedBy, DeleteGranularity deleteGranularity) {
    createTableWithDeleteGranularity("id INT, dep STRING", partitionedBy, deleteGranularity);

    append(tableName, "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"hr\" }\n" + "{ \"id\": 4, \"dep\": \"hr\" }");
    append(tableName, "{ \"id\": 1, \"dep\": \"it\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");
    append(tableName, "{ \"id\": 3, \"dep\": \"it\" }\n" + "{ \"id\": 4, \"dep\": \"it\" }");

    createBranchIfNeeded();
  }
}
