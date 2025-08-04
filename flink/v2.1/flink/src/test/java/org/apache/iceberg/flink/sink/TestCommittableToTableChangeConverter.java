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
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.api.java.tuple.Tuple2;
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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.maintenance.operator.TableChange;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestCommittableToTableChangeConverter {
  @TempDir private File tempDir;
  private Table table;
  private FileIO fileIO;
  private String tableName;
  private Map<Integer, PartitionSpec> specs;
  private DataFile dataFile;
  private DataFile dataFile1;
  private DeleteFile posDeleteFile;
  private DeleteFile eqDeleteFile;

  @BeforeEach
  public void before() throws Exception {
    String warehouse = tempDir.getAbsolutePath();

    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).as("Should create the table path correctly.").isTrue();
    table = SimpleDataUtil.createTable(tablePath, Maps.newHashMap(), false);
    fileIO = table.io();
    tableName = table.name();
    specs = table.specs();
    dataFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(10)
            .build();
    dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data1.parquet")
            .withFileSizeInBytes(101)
            .withRecordCount(11)
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
                new CommittableToTableChangeConverter(fileIO, tableName, specs))) {
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
  public void testConvertReplays() throws Exception {
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    try (OneInputStreamOperatorTestHarness<CommittableMessage<IcebergCommittable>, TableChange>
        harness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new CommittableToTableChangeConverter(fileIO, tableName, specs))) {
      harness.open();

      Tuple2<CommittableWithLineage<IcebergCommittable>, DeltaManifests> icebergCommittable =
          createIcebergCommittable(
              dataFile, posDeleteFile, eqDeleteFile, factory, table, flinkJobId, operatorId, 1L);
      harness.processElement(new StreamRecord<>(icebergCommittable.f0));
      // Duplicate data should be handled properly to avoid job failure.
      harness.processElement(new StreamRecord<>(icebergCommittable.f0));
      List<TableChange> tableChanges = harness.extractOutputValues();
      assertThat(tableChanges).hasSize(1);
      TableChange tableChange = tableChanges.get(0);
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
  public void testReadUnExistManifest() throws Exception {
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    try (OneInputStreamOperatorTestHarness<CommittableMessage<IcebergCommittable>, TableChange>
        harness =
            ProcessFunctionTestHarnesses.forProcessFunction(
                new CommittableToTableChangeConverter(fileIO, tableName, specs))) {
      harness.open();

      Tuple2<CommittableWithLineage<IcebergCommittable>, DeltaManifests> icebergCommittable =
          createIcebergCommittable(
              dataFile, posDeleteFile, eqDeleteFile, factory, table, flinkJobId, operatorId, 1L);

      for (ManifestFile manifest : icebergCommittable.f1.manifests()) {
        fileIO.deleteFile(manifest.path());
        // check Manifest files are deleted
        assertThat(new File(manifest.path())).doesNotExist();
      }

      // Emit the same committable to check read no exist manifest
      // should be handled properly to avoid job failure.
      harness.processElement(new StreamRecord<>(icebergCommittable.f0));

      Tuple2<CommittableWithLineage<IcebergCommittable>, DeltaManifests> icebergCommittable1 =
          createIcebergCommittable(
              dataFile1, posDeleteFile, eqDeleteFile, factory, table, flinkJobId, operatorId, 1L);

      harness.processElement(new StreamRecord<>(icebergCommittable1.f0));

      List<TableChange> tableChanges = harness.extractOutputValues();
      assertThat(tableChanges).hasSize(1);
      TableChange tableChange = tableChanges.get(0);
      TableChange expectedTableChange =
          TableChange.builder()
              .dataFileCount(1)
              .dataFileSizeInBytes(101)
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
                new CommittableToTableChangeConverter(fileIO, tableName, specs))) {

      harness.open();
      IcebergCommittable emptyCommittable =
          new IcebergCommittable(new byte[0], "jobId", "operatorId", 1L);
      CommittableWithLineage<IcebergCommittable> message =
          new CommittableWithLineage<>(emptyCommittable, 1L, 0);
      harness.processElement(new StreamRecord<>(message));
      List<TableChange> tableChanges = harness.extractOutputValues();
      assertThat(tableChanges).hasSize(0);
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
                new CommittableToTableChangeConverter(fileIO, tableName, specs))) {

      harness.open();

      Tuple2<CommittableWithLineage<IcebergCommittable>, DeltaManifests> icebergCommittable =
          createIcebergCommittable(
              dataFile, posDeleteFile, eqDeleteFile, factory, table, flinkJobId, operatorId, 1L);

      harness.processElement(new StreamRecord<>(icebergCommittable.f0));

      // check Manifest files are deleted
      for (ManifestFile manifest : icebergCommittable.f1.manifests()) {
        assertThat(new File(manifest.path())).doesNotExist();
      }
    }
  }

  private static Tuple2<CommittableWithLineage<IcebergCommittable>, DeltaManifests>
      createIcebergCommittable(
          DataFile dataFile,
          DeleteFile posDeleteFile,
          DeleteFile eqDeleteFile,
          ManifestOutputFileFactory factory,
          Table table,
          String flinkJobId,
          String operatorId,
          long checkpointId)
          throws IOException {
    WriteResult writeResult =
        WriteResult.builder()
            .addDataFiles(dataFile)
            .addDeleteFiles(posDeleteFile, eqDeleteFile)
            .build();
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            writeResult, () -> factory.create(checkpointId), table.spec());

    IcebergCommittable committable =
        new IcebergCommittable(
            SimpleVersionedSerialization.writeVersionAndSerialize(
                DeltaManifestsSerializer.INSTANCE, deltaManifests),
            flinkJobId,
            operatorId,
            checkpointId);
    return Tuple2.of(new CommittableWithLineage<>(committable, checkpointId, 0), deltaManifests);
  }

  private static String newFlinkJobId() {
    return UUID.randomUUID().toString();
  }

  private static String newOperatorUniqueId() {
    return UUID.randomUUID().toString();
  }
}
