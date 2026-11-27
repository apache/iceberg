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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

class TestStaticMetadataTables {
  @TempDir private Path temp;

  private Table table;

  @BeforeEach
  void createTable() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()));
    table = new HadoopTables().create(schema, PartitionSpec.unpartitioned(), temp.toString());
  }

  @ParameterizedTest
  @CsvSource({
    "SNAPSHOTS, 1",
    "HISTORY, 1",
    "REFS, 1",
    "METADATA_LOG_ENTRIES, 2",
    "MANIFESTS, 1",
    "PARTITIONS, 1"
  })
  void plansRowsWithoutStattingMetadata(MetadataTableType type, long expectedRows)
      throws IOException {
    appendFile();
    Table loaded = loadedTableWithoutMetadataAccess();
    Table metadataTable = MetadataTableUtils.createMetadataTableInstance(loaded, type);

    try (CloseableIterable<CombinedScanTask> groups = metadataTable.newScan().planTasks()) {
      CombinedScanTask group = Iterables.getOnlyElement(groups);
      DataTask task = Iterables.getOnlyElement(group.files()).asDataTask();
      assertThat(group.estimatedRowsCount()).isEqualTo(expectedRows);
      assertThat(group.sizeBytes()).isPositive();
      String expectedLocation =
          type == MetadataTableType.MANIFESTS
              ? table.currentSnapshot().manifestListLocation()
              : ((BaseTable) loaded).operations().current().metadataFileLocation();
      assertThat(task.file().location()).isEqualTo(expectedLocation);
      try (CloseableIterable<StructLike> rows = task.rows()) {
        assertThat(rows).hasSize((int) expectedRows);
      }
    }
  }

  @Test
  void projectedRowsAndEstimatesSurviveSerialization() throws IOException {
    appendFile();
    Snapshot snapshot = table.currentSnapshot();
    TableScan scan =
        new SnapshotsTable(loadedTableWithoutMetadataAccess())
            .newScan()
            .select("snapshot_id", "summary");

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      FileScanTask task = Iterables.getOnlyElement(tasks);
      DataTask restored = ScanTaskParser.fromJson(ScanTaskParser.toJson(task), true).asDataTask();
      assertThat(restored.estimatedRowsCount()).isEqualTo(1);
      assertThat(restored.sizeBytes()).isPositive().isEqualTo(task.sizeBytes());
      assertThat(restored.file().location()).isEqualTo(task.file().location());
      try (CloseableIterable<StructLike> rows = restored.rows()) {
        assertThat(rows)
            .extracting(row -> row.get(0, Long.class), row -> row.get(1, Map.class))
            .containsExactly(tuple(snapshot.snapshotId(), snapshot.summary()));
      }
    }
  }

  @Test
  void emptyProjectionPreservesEstimates() throws IOException {
    appendFile();
    TableScan scan = new SnapshotsTable(loadedTableWithoutMetadataAccess()).newScan();

    try (CloseableIterable<FileScanTask> fullTasks = scan.planFiles();
        CloseableIterable<FileScanTask> tasks = scan.select(List.of()).planFiles()) {
      DataTask task = Iterables.getOnlyElement(tasks).asDataTask();
      assertThat(task.estimatedRowsCount()).isEqualTo(1);
      assertThat(task.sizeBytes())
          .isPositive()
          .isEqualTo(Iterables.getOnlyElement(fullTasks).sizeBytes());
      try (CloseableIterable<StructLike> rows = task.rows()) {
        assertThat(rows).extracting(StructLike::size).containsExactly(0);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(
      value = MetadataTableType.class,
      names = {"SNAPSHOTS", "HISTORY", "REFS"})
  void emptyTablesHaveZeroEstimates(MetadataTableType type) throws IOException {
    Table metadataTable =
        MetadataTableUtils.createMetadataTableInstance(loadedTableWithoutMetadataAccess(), type);

    try (CloseableIterable<CombinedScanTask> groups = metadataTable.newScan().planTasks()) {
      CombinedScanTask group = Iterables.getOnlyElement(groups);
      assertThat(group.estimatedRowsCount()).isZero();
      assertThat(group.sizeBytes()).isZero();
      DataTask task = Iterables.getOnlyElement(group.files()).asDataTask();
      try (CloseableIterable<StructLike> rows = task.rows()) {
        assertThat(rows).isEmpty();
      }
    }
  }

  private void appendFile() {
    DataFile file =
        DataFiles.builder(table.spec())
            .withPath(temp.resolve("data.parquet").toString())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(file).commit();
  }

  private Table loadedTableWithoutMetadataAccess() {
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    TableMetadata metadata = TableMetadataParser.read(table.io(), metadataLocation);
    FileIO io =
        new TestTables.LocalFileIO() {
          @Override
          public InputFile newInputFile(String path) {
            if (!path.equals(metadataLocation)) {
              return super.newInputFile(path);
            }

            return new InputFile() {
              @Override
              public long getLength() {
                throw new AssertionError("Static metadata tasks must not stat the metadata file");
              }

              @Override
              public SeekableInputStream newStream() {
                throw new AssertionError("Static metadata tasks must not reread the metadata file");
              }

              @Override
              public String location() {
                return path;
              }

              @Override
              public boolean exists() {
                throw new AssertionError("Static metadata tasks must not stat the metadata file");
              }
            };
          }
        };
    return new BaseTable(new StaticTableOperations(metadata, io), table.name());
  }
}
