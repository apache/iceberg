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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkManifest {
  private static final Configuration CONF = new Configuration();

  @TempDir protected Path temporaryFolder;

  private Table table;
  private FlinkFileWriterFactory fileWriterFactory;
  private final AtomicInteger fileCount = new AtomicInteger(0);

  @BeforeEach
  public void before() throws IOException {
    File folder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    String warehouse = folder.getAbsolutePath();

    String tablePath = warehouse.concat("/test");
    assertThat(new File(tablePath).mkdir()).isTrue();

    // Construct the iceberg table.
    table = SimpleDataUtil.createTable(tablePath, ImmutableMap.of(), false);

    int[] equalityFieldIds =
        new int[] {
          table.schema().findField("id").fieldId(), table.schema().findField("data").fieldId()
        };
    this.fileWriterFactory =
        new FlinkFileWriterFactory.Builder(table)
            .dataFileFormat(FileFormat.PARQUET)
            .dataSchema(table.schema())
            .deleteFileFormat(FileFormat.PARQUET)
            .equalityFieldIds(equalityFieldIds)
            .equalityDeleteRowSchema(table.schema())
            .writerProperties(table.properties())
            .build();
  }

  @Test
  public void testIO() throws IOException {
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    for (long checkpointId = 1; checkpointId <= 3; checkpointId++) {
      ManifestOutputFileFactory factory =
          FlinkManifestUtil.createOutputFileFactory(
              () -> table, table.properties(), flinkJobId, operatorId, 1, 1);
      final long curCkpId = checkpointId;

      List<DataFile> dataFiles = generateDataFiles(10);
      List<DeleteFile> eqDeleteFiles = generateEqDeleteFiles(5);
      List<DeleteFile> posDeleteFiles = generatePosDeleteFiles(5);
      DeltaManifests deltaManifests =
          FlinkManifestUtil.writeCompletedFiles(
              WriteResult.builder()
                  .addDataFiles(dataFiles)
                  .addDeleteFiles(eqDeleteFiles)
                  .addDeleteFiles(posDeleteFiles)
                  .build(),
              () -> factory.create(curCkpId),
              table.spec(),
              TableUtil.formatVersion(table));

      WriteResult result =
          FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs());
      assertThat(result.deleteFiles()).hasSize(10);
      for (int i = 0; i < dataFiles.size(); i++) {
        TestHelpers.assertEquals(dataFiles.get(i), result.dataFiles()[i]);
      }
      assertThat(result.deleteFiles()).hasSize(10);
      for (int i = 0; i < 5; i++) {
        TestHelpers.assertEquals(eqDeleteFiles.get(i), result.deleteFiles()[i]);
      }
      for (int i = 0; i < 5; i++) {
        TestHelpers.assertEquals(posDeleteFiles.get(i), result.deleteFiles()[5 + i]);
      }
    }
  }

  @Test
  public void testUserProvidedManifestLocation() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    File userProvidedFolder = Files.createTempDirectory(temporaryFolder, "junit").toFile();
    Map<String, String> props =
        ImmutableMap.of(
            ManifestOutputFileFactory.FLINK_MANIFEST_LOCATION,
            userProvidedFolder.getAbsolutePath() + "///");
    ManifestOutputFileFactory factory =
        new ManifestOutputFileFactory(() -> table, props, flinkJobId, operatorId, 1, 1, null);

    List<DataFile> dataFiles = generateDataFiles(5);
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            WriteResult.builder().addDataFiles(dataFiles).build(),
            () -> factory.create(checkpointId),
            table.spec(),
            TableUtil.formatVersion(table));

    assertThat(deltaManifests.dataManifest()).isNotNull();
    assertThat(deltaManifests.deleteManifests()).isEmpty();
    assertThat(Paths.get(deltaManifests.dataManifest().path()))
        .hasParent(userProvidedFolder.toPath());

    WriteResult result =
        FlinkManifestUtil.readCompletedFiles(deltaManifests, table.io(), table.specs());

    assertThat(result.deleteFiles()).isEmpty();
    assertThat(result.dataFiles()).hasSize(5);

    assertThat(result.dataFiles()).hasSameSizeAs(dataFiles);
    for (int i = 0; i < dataFiles.size(); i++) {
      TestHelpers.assertEquals(dataFiles.get(i), result.dataFiles()[i]);
    }
  }

  @Test
  public void writesDeleteFilesToAManifestOfTheirOwnSpec() throws IOException {
    PartitionSpec unpartitioned = table.spec();
    table.updateSpec().addField("data").commit();
    PartitionSpec partitioned = table.spec();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), newFlinkJobId(), newOperatorUniqueId(), 1, 1);

    DeleteFile oldSpecDelete = metadataOnlyDelete(unpartitioned, null, "old-spec");
    DeleteFile newSpecDelete = metadataOnlyDelete(partitioned, "data=a", "new-spec");
    DeleteFile replaced = metadataOnlyDelete(unpartitioned, null, "replaced");
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            WriteResult.builder()
                .addDeleteFiles(oldSpecDelete, newSpecDelete)
                .addRewrittenDeleteFiles(replaced)
                .build(),
            () -> factory.create(1),
            table.spec(),
            table.specs(),
            TableUtil.formatVersion(table),
            42L);

    assertThat(deltaManifests.deleteManifests())
        .extracting(ManifestFile::partitionSpecId)
        .containsExactlyInAnyOrder(unpartitioned.specId(), partitioned.specId());
    assertThat(deltaManifests.rewrittenDeleteManifests())
        .extracting(ManifestFile::partitionSpecId)
        .containsExactly(unpartitioned.specId());

    assertThatThrownBy(() -> DeltaManifestsSerializer.INSTANCE.serialize(deltaManifests))
        .as("The default serializer keeps writing a version earlier releases can read")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("DV-only");

    byte[] serialized =
        SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.DV_ONLY, deltaManifests);
    DeltaManifests restored =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, serialized);
    assertThat(restored.baselineSnapshotId()).isEqualTo(42L);
    assertThat(restored.manifests()).hasSameSizeAs(deltaManifests.manifests());

    WriteResult result = FlinkManifestUtil.readCompletedFiles(restored, table.io(), table.specs());
    Map<String, Integer> specByLocation = Maps.newHashMap();
    for (DeleteFile deleteFile : result.deleteFiles()) {
      specByLocation.put(deleteFile.location(), deleteFile.specId());
    }

    assertThat(specByLocation)
        .containsEntry(oldSpecDelete.location(), unpartitioned.specId())
        .containsEntry(newSpecDelete.location(), partitioned.specId())
        .hasSize(2);
    assertThat(result.rewrittenDeleteFiles())
        .singleElement()
        .extracting(DeleteFile::location)
        .isEqualTo(replaced.location());
  }

  private DeleteFile metadataOnlyDelete(PartitionSpec spec, String partitionPath, String name) {
    FileMetadata.Builder builder =
        FileMetadata.deleteFileBuilder(spec)
            .ofPositionDeletes()
            .withPath(table.location() + "/data/" + name + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(10)
            .withRecordCount(1);
    if (partitionPath != null) {
      builder.withPartitionPath(partitionPath);
    }

    return builder.build();
  }

  @Test
  public void testVersionedSerializer() throws IOException {
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    List<DataFile> dataFiles = generateDataFiles(10);
    List<DeleteFile> eqDeleteFiles = generateEqDeleteFiles(10);
    List<DeleteFile> posDeleteFiles = generatePosDeleteFiles(10);
    DeltaManifests expected =
        FlinkManifestUtil.writeCompletedFiles(
            WriteResult.builder()
                .addDataFiles(dataFiles)
                .addDeleteFiles(eqDeleteFiles)
                .addDeleteFiles(posDeleteFiles)
                .build(),
            () -> factory.create(checkpointId),
            table.spec(),
            TableUtil.formatVersion(table));

    byte[] versionedSerializeData =
        SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.INSTANCE, expected);
    DeltaManifests actual =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, versionedSerializeData);
    TestHelpers.assertEquals(expected.dataManifest(), actual.dataManifest());
    assertThat(actual.deleteManifests()).hasSameSizeAs(expected.deleteManifests());
    for (int i = 0; i < expected.deleteManifests().size(); i++) {
      TestHelpers.assertEquals(expected.deleteManifests().get(i), actual.deleteManifests().get(i));
    }

    byte[] versionedSerializeData2 =
        SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.INSTANCE, actual);
    assertThat(versionedSerializeData2).containsExactly(versionedSerializeData);
  }

  @Test
  public void theDefaultWritePathKeepsWritingVersion2() throws IOException {
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), newFlinkJobId(), newOperatorUniqueId(), 1, 1);
    DeltaManifests deltaManifests =
        FlinkManifestUtil.writeCompletedFiles(
            WriteResult.builder()
                .addDataFiles(generateDataFiles(2))
                .addDeleteFiles(generateEqDeleteFiles(2))
                .build(),
            () -> factory.create(1),
            table.spec(),
            TableUtil.formatVersion(table));

    assertThat(DeltaManifestsSerializer.INSTANCE.getVersion()).isEqualTo(2);
    byte[] serialized =
        SimpleVersionedSerialization.writeVersionAndSerialize(
            DeltaManifestsSerializer.INSTANCE, deltaManifests);
    DeltaManifests restored =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, serialized);
    TestHelpers.assertEquals(deltaManifests.dataManifest(), restored.dataManifest());
    assertThat(restored.deleteManifests()).hasSize(1);
    TestHelpers.assertEquals(
        deltaManifests.deleteManifests().get(0), restored.deleteManifests().get(0));
  }

  @Test
  public void testCompatibility() throws IOException {
    // The v2 deserializer should be able to deserialize the v1 binary.
    long checkpointId = 1;
    String flinkJobId = newFlinkJobId();
    String operatorId = newOperatorUniqueId();
    ManifestOutputFileFactory factory =
        FlinkManifestUtil.createOutputFileFactory(
            () -> table, table.properties(), flinkJobId, operatorId, 1, 1);

    List<DataFile> dataFiles = generateDataFiles(10);
    ManifestFile manifest =
        FlinkManifestUtil.writeDataFiles(
            factory.create(checkpointId), table.spec(), dataFiles, TableUtil.formatVersion(table));
    byte[] dataV1 =
        SimpleVersionedSerialization.writeVersionAndSerialize(new V1Serializer(), manifest);

    DeltaManifests delta =
        SimpleVersionedSerialization.readVersionAndDeSerialize(
            DeltaManifestsSerializer.INSTANCE, dataV1);
    assertThat(delta.deleteManifests()).isEmpty();
    assertThat(delta.dataManifest()).isNotNull();
    TestHelpers.assertEquals(manifest, delta.dataManifest());

    List<DataFile> actualFiles =
        FlinkManifestUtil.readDataFiles(delta.dataManifest(), table.io(), table.specs());
    assertThat(actualFiles).hasSize(10);
    for (int i = 0; i < 10; i++) {
      TestHelpers.assertEquals(dataFiles.get(i), actualFiles.get(i));
    }
  }

  private static class V1Serializer implements SimpleVersionedSerializer<ManifestFile> {

    @Override
    public int getVersion() {
      return 1;
    }

    @Override
    public byte[] serialize(ManifestFile m) throws IOException {
      return ManifestFiles.encode(m);
    }

    @Override
    public ManifestFile deserialize(int version, byte[] serialized) throws IOException {
      return ManifestFiles.decode(serialized);
    }
  }

  private DataFile writeDataFile(String filename, List<RowData> rows) throws IOException {
    return SimpleDataUtil.writeFile(
        table,
        table.schema(),
        table.spec(),
        CONF,
        table.location(),
        FileFormat.PARQUET.addExtension(filename),
        rows);
  }

  private DeleteFile writeEqDeleteFile(String filename, List<RowData> deletes) throws IOException {
    return SimpleDataUtil.writeEqDeleteFile(
        table, table.spec(), filename, fileWriterFactory, deletes);
  }

  private DeleteFile writePosDeleteFile(String filename, List<Pair<CharSequence, Long>> positions)
      throws IOException {
    return SimpleDataUtil.writePosDeleteFile(
        table, table.spec(), filename, fileWriterFactory, positions);
  }

  private List<DataFile> generateDataFiles(int fileNum) throws IOException {
    List<RowData> rowDataList = Lists.newArrayList();
    List<DataFile> dataFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      rowDataList.add(SimpleDataUtil.createRowData(i, "a" + i));
      dataFiles.add(writeDataFile("data-file-" + fileCount.incrementAndGet(), rowDataList));
    }
    return dataFiles;
  }

  private List<DeleteFile> generateEqDeleteFiles(int fileNum) throws IOException {
    List<RowData> rowDataList = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      rowDataList.add(SimpleDataUtil.createDelete(i, "a" + i));
      deleteFiles.add(
          writeEqDeleteFile("eq-delete-file-" + fileCount.incrementAndGet(), rowDataList));
    }
    return deleteFiles;
  }

  private List<DeleteFile> generatePosDeleteFiles(int fileNum) throws IOException {
    List<Pair<CharSequence, Long>> positions = Lists.newArrayList();
    List<DeleteFile> deleteFiles = Lists.newArrayList();
    for (int i = 0; i < fileNum; i++) {
      positions.add(Pair.of("data-file-1", (long) i));
      deleteFiles.add(
          writePosDeleteFile("pos-delete-file-" + fileCount.incrementAndGet(), positions));
    }
    return deleteFiles;
  }

  private static String newFlinkJobId() {
    return UUID.randomUUID().toString();
  }

  private static String newOperatorUniqueId() {
    return UUID.randomUUID().toString();
  }
}
