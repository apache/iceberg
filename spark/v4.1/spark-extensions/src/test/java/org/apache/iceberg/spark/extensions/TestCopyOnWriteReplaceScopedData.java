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

import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.DELETED_FILES_PROP;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.TestTemplate;

public class TestCopyOnWriteReplaceScopedData extends TestReplaceScopedData {

  @Override
  protected Map<String, String> extraTableProperties() {
    return ImmutableMap.of(
        TableProperties.MERGE_MODE, RowLevelOperationMode.COPY_ON_WRITE.modeName());
  }

  @TestTemplate
  public void testScopedReplaceRewritesDataFiles() {
    createAndInitTable(
        "id INT, dep STRING",
        "PARTITIONED BY (dep)",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"it\" }");
    createOrReplaceView("source", "id INT, dep STRING", "{ \"id\": 10, \"dep\": \"hr\" }");

    sql("INSERT INTO %s REPLACE USING (dep) SELECT id, dep FROM source", commitTarget());

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot = SnapshotUtil.latestSnapshot(table, branch);
    assertThat(snapshot.summary()).containsKey(DELETED_FILES_PROP);
    assertThat(snapshot.summary()).doesNotContainKey(ADDED_DELETE_FILES_PROP);
  }
}
