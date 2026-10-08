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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSessionRowLevelOperationMode extends SparkRowLevelOperationsTestBase {

  private static final String SCHEMA = "id INT, dep STRING";
  private static final String DATA =
      "{ \"id\": 1, \"dep\": \"hr\" }\n{ \"id\": 2, \"dep\": \"hr\" }";
  private static final String SOURCE_VIEW = "session_mode_source";

  @AfterEach
  public void unsetSessionMode() {
    spark.conf().unset(SparkSQLProperties.ROW_LEVEL_OPERATION_MODE);
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  /** When the session mode is set, it must win over the copy-on-write table properties. */
  @TestTemplate
  public void testSessionModeOverridesTablePropertiesForDelete() {
    createTableAndSource();
    withSQLConf(mergeOnReadSessionConf(), () -> sql("DELETE FROM %s WHERE id = 1", commitTarget()));
    assertMergeOnRead("delete");
  }

  @TestTemplate
  public void testSessionModeOverridesTablePropertiesForUpdate() {
    createTableAndSource();
    withSQLConf(
        mergeOnReadSessionConf(),
        () -> sql("UPDATE %s SET dep = 'xyz' WHERE id = 2", commitTarget()));
    assertMergeOnRead("update");
  }

  @TestTemplate
  public void testSessionModeOverridesTablePropertiesForMerge() {
    createTableAndSource();
    withSQLConf(mergeOnReadSessionConf(), this::runMerge);
    assertMergeOnRead("merge");
  }

  /** Without the session mode, the copy-on-write table properties must still be respected. */
  @TestTemplate
  public void testTablePropertiesUsedWhenSessionModeIsNotSet() {
    createTableAndSource();

    assertThat(spark.conf().get(SparkSQLProperties.ROW_LEVEL_OPERATION_MODE, null))
        .as("Session row-level mode must not be set")
        .isNull();

    sql("DELETE FROM %s WHERE id = 1", commitTarget());
    assertCopyOnWrite("delete");
  }

  @TestTemplate
  public void testTablePropertiesUsedWhenSessionModeIsNotSetForUpdate() {
    createTableAndSource();

    sql("UPDATE %s SET dep = 'xyz' WHERE id = 2", commitTarget());
    assertCopyOnWrite("update");
  }

  @TestTemplate
  public void testTablePropertiesUsedWhenSessionModeIsNotSetForMerge() {
    createTableAndSource();

    runMerge();
    assertCopyOnWrite("merge");
  }

  private void createTableAndSource() {
    createAndInitTable(SCHEMA, DATA);
    createOrReplaceView(SOURCE_VIEW, "{ \"id\": 1, \"dep\": \"xyz\" }");
  }

  private void runMerge() {
    sql(
        "MERGE INTO %s t USING %s s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.dep = s.dep",
        commitTarget(), SOURCE_VIEW);
  }

  private Map<String, String> mergeOnReadSessionConf() {
    return ImmutableMap.of(
        SparkSQLProperties.ROW_LEVEL_OPERATION_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName());
  }

  private void assertMergeOnRead(String operation) {
    Snapshot snapshot = latestSnapshot();
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(snapshot.deleteManifests(table.io()))
        .as("%s must run in merge-on-read and write delete files", operation)
        .isNotEmpty();
  }

  private void assertCopyOnWrite(String operation) {
    Snapshot snapshot = latestSnapshot();
    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(snapshot.deleteManifests(table.io()))
        .as("%s must run in copy-on-write and must not write delete files", operation)
        .isEmpty();
  }

  private Snapshot latestSnapshot() {
    Table table = validationCatalog.loadTable(tableIdent);
    return SnapshotUtil.latestSnapshot(table, branch);
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.DELETE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName(),
        TableProperties.UPDATE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName(),
        TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }
}
