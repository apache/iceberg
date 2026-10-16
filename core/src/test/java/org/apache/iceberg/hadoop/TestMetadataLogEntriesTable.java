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
package org.apache.iceberg.hadoop;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestMetadataLogEntriesTable extends HadoopTableTestBase {

  @Test
  void snapshotDetailsAfterExpiration() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot first = table.currentSnapshot();
    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot second = table.currentSnapshot();
    table.newFastAppend().appendFile(FILE_C).commit();

    List<List<Object>> before = rows(metadataLogEntries().newScan());

    table
        .expireSnapshots()
        .expireSnapshotId(second.snapshotId())
        .cleanupLevel(ExpireSnapshots.CleanupLevel.NONE)
        .commit();

    List<List<Object>> after = rows(metadataLogEntries().newScan());
    assertThat(after).hasSize(before.size() + 1);
    assertThat(after.subList(0, before.size())).isEqualTo(before);
    assertThat(after.get(1))
        .containsExactly(
            after.get(1).get(0),
            after.get(1).get(1),
            first.snapshotId(),
            first.schemaId(),
            first.sequenceNumber());
  }

  @Test
  void snapshotColumnsNotProjected() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    List<List<Object>> before = rows(metadataLogEntries().newScan().select("timestamp", "file"));
    deletePreviousMetadataFiles();

    assertThat(rows(metadataLogEntries().newScan().select("timestamp", "file"))).isEqualTo(before);
  }

  @Test
  void missingMetadataFileFallsBackToSnapshotLog() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    List<List<Object>> before = rows(metadataLogEntries().newScan());
    deletePreviousMetadataFiles();

    assertThat(rows(metadataLogEntries().newScan())).isEqualTo(before);
  }

  private org.apache.iceberg.Table metadataLogEntries() {
    return MetadataTableUtils.createMetadataTableInstance(
        table, MetadataTableType.METADATA_LOG_ENTRIES);
  }

  private void deletePreviousMetadataFiles() {
    TableMetadata current = ((HasTableOperations) table).operations().current();
    for (TableMetadata.MetadataLogEntry entry : current.previousFiles()) {
      table.io().deleteFile(entry.file());
    }
  }

  private static List<List<Object>> rows(TableScan scan) throws IOException {
    List<List<Object>> rows = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = scan.planFiles();
        CloseableIterable<StructLike> structs =
            Iterables.getOnlyElement(tasks).asDataTask().rows()) {
      for (StructLike struct : structs) {
        List<Object> row = Lists.newArrayList();
        for (int pos = 0; pos < struct.size(); pos += 1) {
          row.add(struct.get(pos, Object.class));
        }

        rows.add(row);
      }
    }

    return rows;
  }
}
