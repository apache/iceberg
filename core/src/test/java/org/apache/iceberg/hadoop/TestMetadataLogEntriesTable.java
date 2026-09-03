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

import static org.apache.iceberg.MetadataTableType.METADATA_LOG_ENTRIES;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.Test;

class TestMetadataLogEntriesTable extends HadoopTableTestBase {

  @Test
  void preservesSnapshotDetailsAfterSnapshotExpiration() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    TableMetadata beforeExpiration = ((HasTableOperations) table).operations().current();
    List<TableMetadata.MetadataLogEntry> metadataLog = beforeExpiration.previousFiles();

    table
        .expireSnapshots()
        .expireSnapshotId(firstSnapshot.snapshotId())
        .cleanupLevel(ExpireSnapshots.CleanupLevel.NONE)
        .commit();

    TableMetadata afterExpiration = ((HasTableOperations) table).operations().current();

    Table metadataTable =
        MetadataTableUtils.createMetadataTableInstance(table, METADATA_LOG_ENTRIES);

    try (CloseableIterable<FileScanTask> tasks = metadataTable.newScan().planFiles()) {
      DataTask task = Iterables.getOnlyElement(tasks).asDataTask();

      try (CloseableIterable<StructLike> rows = task.rows()) {
        Iterator<StructLike> iterator = rows.iterator();

        assertMetadataLogRow(iterator.next(), metadataLog.get(0).file(), null);
        assertMetadataLogRow(iterator.next(), metadataLog.get(1).file(), firstSnapshot);
        assertMetadataLogRow(
            iterator.next(), beforeExpiration.metadataFileLocation(), secondSnapshot);
        assertMetadataLogRow(
            iterator.next(), afterExpiration.metadataFileLocation(), secondSnapshot);

        assertThat(iterator).isExhausted();
      }
    }
  }

  @Test
  void avoidLoadHistoricalMetadataWhenSnapshotColumnsAreNotProjected() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    TableMetadata current = ((HasTableOperations) table).operations().current();

    List<String> expectedFiles =
        current.previousFiles().stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toList());
    expectedFiles.add(current.metadataFileLocation());

    Table metadataTable =
        MetadataTableUtils.createMetadataTableInstance(table, METADATA_LOG_ENTRIES);

    for (TableMetadata.MetadataLogEntry entry : current.previousFiles()) {
      table.io().deleteFile(entry.file());
    }

    try (CloseableIterable<FileScanTask> tasks =
        metadataTable.newScan().select("file").planFiles()) {
      DataTask task = Iterables.getOnlyElement(tasks).asDataTask();

      try (CloseableIterable<StructLike> rows = task.rows()) {
        assertThat(rows)
            .extracting(row -> row.get(0, String.class))
            .containsExactlyElementsOf(expectedFiles);
      }
    }
  }

  private static void assertMetadataLogRow(
      StructLike row, String expectedFile, Snapshot expectedSnapshot) {
    assertThat(row.get(1, String.class)).isEqualTo(expectedFile);

    if (expectedSnapshot == null) {
      assertThat(row.get(2, Long.class)).isNull();
      assertThat(row.get(3, Integer.class)).isNull();
      assertThat(row.get(4, Long.class)).isNull();
    } else {
      assertThat(row.get(2, Long.class)).isEqualTo(expectedSnapshot.snapshotId());
      assertThat(row.get(3, Integer.class)).isEqualTo(expectedSnapshot.schemaId());
      assertThat(row.get(4, Long.class)).isEqualTo(expectedSnapshot.sequenceNumber());
    }
  }
}
