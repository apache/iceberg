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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

class TestMetadataLogEntriesTable extends HadoopTableTestBase {

  @Test
  void snapshotDetailsAfterExpiration() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
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
    assertThat(after.get(2).subList(2, 5))
        .containsExactly(second.snapshotId(), second.schemaId(), second.sequenceNumber());
  }

  @Test
  void snapshotColumnsNotProjected() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    TableMetadata current = ((HasTableOperations) table).operations().current();
    FileIO io = spy(table.io());

    rows(metadataLogEntries(current, io).newScan().select("timestamp", "file"));

    for (TableMetadata.MetadataLogEntry previous : current.previousFiles()) {
      verify(io, never()).newInputFile(previous.file());
    }
  }

  @Test
  void missingMetadataFileReturnsNullSnapshotDetails() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    TableMetadata current = ((HasTableOperations) table).operations().current();
    List<List<Object>> before = rows(metadataLogEntries().newScan());
    for (TableMetadata.MetadataLogEntry previous : current.previousFiles()) {
      table.io().deleteFile(previous.file());
    }

    List<List<Object>> expected = Lists.newArrayList();
    for (List<Object> row : before.subList(0, before.size() - 1)) {
      expected.add(Lists.newArrayList(row.get(0), row.get(1), null, null, null));
    }

    expected.add(Iterables.getLast(before));

    assertThat(rows(metadataLogEntries().newScan())).isEqualTo(expected);
  }

  private org.apache.iceberg.Table metadataLogEntries() {
    return MetadataTableUtils.createMetadataTableInstance(
        table, MetadataTableType.METADATA_LOG_ENTRIES);
  }

  private static org.apache.iceberg.Table metadataLogEntries(TableMetadata current, FileIO io) {
    TableOperations operations = mock(TableOperations.class);
    when(operations.current()).thenReturn(current);
    when(operations.io()).thenReturn(io);
    return MetadataTableUtils.createMetadataTableInstance(
        operations, "table", "table.metadata_log_entries", MetadataTableType.METADATA_LOG_ENTRIES);
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
