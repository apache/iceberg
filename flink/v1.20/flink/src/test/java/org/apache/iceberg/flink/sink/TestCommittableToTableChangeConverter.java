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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.UUID;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCommittableToTableChangeConverter {
  @TempDir private File tempDir;
  private Table table;
  private TableLoader tableLoader;
  private DataFile dataFile;
  DeleteFile posDeleteFile;
  DeleteFile eqDeleteFile;

  @BeforeEach
  public void before() throws Exception {
    String warehouse = tempDir.getAbsolutePath();

    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).as("Should create the table path correctly.").isTrue();
    table = SimpleDataUtil.createTable(tablePath, Maps.newHashMap(), false);
    tableLoader = TableLoader.fromHadoopTable(tablePath);
    dataFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
    posDeleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/pos-deletes.parquet")
            .withFileSizeInBytes(50)
            .withRecordCount(5)
            .build();
    eqDeleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-deletes.parquet")
            .withFileSizeInBytes(30)
            .withRecordCount(3)
            .build();
  }

  @Test
  public void testConvertWriteResultToTableChange() throws Exception {
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    try (OneInputStreamOperatorTestHarness<CommittableMessage<IcebergCommittable>, TableChange>
        harness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new CommittableToTableChangeConverter(tableLoader))) {
      harness.open();
      WriteResult writeResult =
          WriteResult.builder()
              .addDataFiles(dataFile)
              .addDeleteFiles(posDeleteFile, eqDeleteFile)
              .build();
      DeltaManifests deltaManifests =
          FlinkManifestUtil.writeCompletedFiles(writeResult, () -> factory.create(1), table.spec());
      IcebergCommittable committable =
          new IcebergCommittable(
              SimpleVersionedSerialization.writeVersionAndSerialize(
                  DeltaManifestsSerializer.INSTANCE, deltaManifests),
              flinkJobId,
              operatorId,
              1L);
      CommittableWithLineage<IcebergCommittable> message =
          new CommittableWithLineage<>(committable, 1L, 0);
      harness.processElement(new StreamRecord<>(message));
      TableChange tableChange = harness.extractOutputValues().get(0);
      TableChange expectedTableChange =
          TableChange.builder()
              .dataFileCount(1)
              .dataFileSizeInBytes(100)
              .posDeleteFileCount(1)
              .posDeleteRecordCount(5)
              .eqDeleteFileCount(1)
              .eqDeleteRecordCount(3)
              .commitCount(1)
              .build();

      assertThat(tableChange).isEqualTo(expectedTableChange);
    }
  }

  @Test
  public void testEmptyCommit() throws Exception {
    try (OneInputStreamOperatorTestHarness<CommittableMessage<IcebergCommittable>, TableChange>
        harness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new CommittableToTableChangeConverter(tableLoader))) {

      harness.open();
      IcebergCommittable emptyCommittable =
          new IcebergCommittable(new byte[0], "jobId", "operatorId", 1L);
      CommittableWithLineage<IcebergCommittable> message =
          new CommittableWithLineage<>(emptyCommittable, 1L, 0);
      harness.processElement(new StreamRecord<>(message));
      TableChange tableChange = harness.extractOutputValues().get(0);
      assertThat(tableChange).isEqualTo(TableChange.empty());
    }
  }

  @Test
  public void testManifestDeletion() throws Exception {
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    try (OneInputStreamOperatorTestHarness<CommittableMessage<IcebergCommittable>, TableChange>
        harness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new CommittableToTableChangeConverter(tableLoader))) {

      harness.open();

      WriteResult writeResult =
          WriteResult.builder().addDataFiles(dataFile).addDeleteFiles(posDeleteFile).build();
      DeltaManifests deltaManifests =
          FlinkManifestUtil.writeCompletedFiles(writeResult, () -> factory.create(1), table.spec());
      IcebergCommittable committable =
          new IcebergCommittable(
              SimpleVersionedSerialization.writeVersionAndSerialize(
                  DeltaManifestsSerializer.INSTANCE, deltaManifests),
              flinkJobId,
              operatorId,
              1L);
      CommittableWithLineage<IcebergCommittable> message =
          new CommittableWithLineage<>(committable, 1L, 0);
      harness.processElement(new StreamRecord<>(message));

      // mock checkpoint
      harness.snapshot(1L, 1000L);
      harness.notifyOfCompletedCheckpoint(1L);

      // check Manifest files are deleted
      for (ManifestFile manifest : deltaManifests.manifests()) {
        assertThat(new File(manifest.path())).doesNotExist();
      }
    }
  }

  private static String newFlinkJobId() {
    return UUID.randomUUID().toString();
  }

  private static String newOperatorUniqueId() {
    return UUID.randomUUID().toString();
  }
}
