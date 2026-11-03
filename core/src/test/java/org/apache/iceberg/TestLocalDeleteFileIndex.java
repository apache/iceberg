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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;

public class TestLocalDeleteFileIndex
    extends DeleteFileIndexTestBase<TableScan, FileScanTask, CombinedScanTask> {

  @Override
  protected TableScan newScan(Table table) {
    return table.newScan();
  }

  @TestTemplate
  void fileScopedPositionDeleteDiscardMetrics() {
    Table table =
        TestTables.create(tableDir, "partitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    table.newAppend().appendFile(FILE_A).commit();

    // add a delete file whose file_path bounds both equal FILE_A
    table
        .newRowDelta()
        .addDeletes(posDeletesWithMetrics(table.spec(), FILE_A.location(), FILE_A.location()))
        .commit();

    // upgrade to the tested format version with the position deletes in place
    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .commit();

    FileScanTask task = Iterables.getOnlyElement(table.newScan().planFiles());
    DeleteFile deleteFile = Iterables.getOnlyElement(task.deletes());
    assertThat(deleteFile.referencedDataFile()).isEqualTo(FILE_A.location());
    assertThat(deleteFile.lowerBounds()).isNull();
    assertThat(deleteFile.upperBounds()).isNull();
  }

  @TestTemplate
  void positionDeleteWithReferencedDataFileDiscardMetrics() {
    Table table =
        TestTables.create(tableDir, "partitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    table.newAppend().appendFile(FILE_A).commit();

    // add a delete file with referenced_data_file set and no file_path bounds
    DeleteFile posDeletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath(UUID.randomUUID() + "/path/to/data-unpartitioned-pos-deletes.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withReferencedDataFile(FILE_A.location())
            .build();
    table.newRowDelta().addDeletes(posDeletes).commit();

    // upgrade to the tested format version with the position deletes in place
    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .commit();

    FileScanTask task = Iterables.getOnlyElement(table.newScan().planFiles());
    DeleteFile deleteFile = Iterables.getOnlyElement(task.deletes());
    assertThat(deleteFile.referencedDataFile()).isEqualTo(FILE_A.location());
  }

  @TestTemplate
  void multiFilePositionDeleteDiscardMetrics() {
    Table table =
        TestTables.create(tableDir, "partitioned", SCHEMA, PartitionSpec.unpartitioned(), 2);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // add a delete file whose file_path bounds span FILE_A and FILE_B
    table
        .newRowDelta()
        .addDeletes(posDeletesWithMetrics(table.spec(), FILE_A.location(), FILE_B.location()))
        .commit();

    // upgrade to the tested format version with the position deletes in place
    table
        .updateProperties()
        .set(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion))
        .commit();

    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).hasSize(2);

    // verify scanned delete files only keep the file_path bounds
    int fieldId = MetadataColumns.DELETE_FILE_PATH.fieldId();
    for (FileScanTask task : tasks) {
      DeleteFile deleteFile = Iterables.getOnlyElement(task.deletes());
      assertThat(deleteFile.referencedDataFile()).isNull();
      assertThat(deleteFile.lowerBounds())
          .containsExactly(
              Map.entry(
                  fieldId, ByteBuffer.wrap(FILE_A.location().getBytes(StandardCharsets.UTF_8))));
      assertThat(deleteFile.upperBounds())
          .containsExactly(
              Map.entry(
                  fieldId, ByteBuffer.wrap(FILE_B.location().getBytes(StandardCharsets.UTF_8))));
    }
  }
}
