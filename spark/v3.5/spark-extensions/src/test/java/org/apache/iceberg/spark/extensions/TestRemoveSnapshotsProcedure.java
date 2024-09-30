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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestRemoveSnapshotsProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRemoveCorruptSnapshots() throws IOException {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    assertThat(table.snapshots()).as("Should be 2 snapshots").hasSize(2);

    long firstSnapshotId = table.currentSnapshot().parentId();
    FileSystem localFs = FileSystem.getLocal(new Configuration());
    Path manifestListPath = new Path(table.snapshot(firstSnapshotId).manifestListLocation());
    assertThat(localFs.exists(manifestListPath)).isTrue();
    localFs.delete(manifestListPath, false);
    assertThat(localFs.exists(manifestListPath)).isFalse();

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.expire_snapshots(table => '%s', snapshot_ids => ARRAY(%d))",
                    catalogName, tableIdent, firstSnapshotId))
        .isInstanceOf(NotFoundException.class)
        .hasMessageStartingWith("File does not exist");
    List<Object[]> output =
        sql(
            "CALL %s.system.remove_snapshots(" + "table => '%s'," + "snapshot_ids => ARRAY(%d))",
            catalogName, tableIdent, firstSnapshotId);
    assertEquals("Procedure output must match", ImmutableList.of(row(1L)), output);

    // There should only be one single snapshot left.
    table.refresh();
    assertThat(table.snapshots()).as("Should be 1 snapshots").hasSize(1);
    assertThat(
            Iterables.filter(
                table.snapshots(), snapshot -> snapshot.snapshotId() == firstSnapshotId))
        .as("Snapshot ID should not be present")
        .hasSize(0);
  }
}
