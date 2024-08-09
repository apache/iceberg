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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class TestCatalogUtilDropTable extends HadoopTableTestBase {

  @Test
  public void dropTableDataDeletesExpectedFiles() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();
    StatisticsFile statisticsFile =
        writeStatsFile(
            table.currentSnapshot().snapshotId(),
            table.currentSnapshot().sequenceNumber(),
            tableLocation + "/metadata/" + UUID.randomUUID() + ".stats",
            table.io());
    table.updateStatistics().setStatistics(statisticsFile.snapshotId(), statisticsFile).commit();

    PartitionStatisticsFile partitionStatisticsFile =
        writePartitionStatsFile(
            table.currentSnapshot().snapshotId(),
            tableLocation + "/metadata/" + UUID.randomUUID() + ".stats",
            table.io());
    table.updatePartitionStatistics().setPartitionStatistics(partitionStatisticsFile).commit();

    TableMetadata tableMetadata = readMetadataVersion(5);
    Set<Snapshot> snapshotSet = Sets.newHashSet(table.snapshots());

    Set<String> manifestListLocations = manifestListLocations(snapshotSet);
    Set<String> manifestLocations = manifestLocations(snapshotSet, table.io());
    Set<String> dataLocations = dataLocations(snapshotSet, table.io());
    Set<String> metadataLocations = metadataLocations(tableMetadata);
    Set<String> statsLocations = statsLocations(tableMetadata);
    Set<String> partitionStatsLocations = partitionStatsLocations(tableMetadata);

    assertThat(manifestListLocations).as("should have 2 manifest lists").hasSize(2);
    assertThat(metadataLocations).as("should have 5 metadata locations").hasSize(5);
    assertThat(statsLocations)
        .as("should have 1 stats file")
        .containsExactly(statisticsFile.path());
    assertThat(partitionStatsLocations)
        .as("should have 1 partition stats file")
        .containsExactly(partitionStatisticsFile.path());

    FileIO fileIO = createMockFileIO(table.io());

    CatalogUtil.dropTableData(fileIO, tableMetadata);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.verify(
            fileIO,
            Mockito.times(
                manifestListLocations.size()
                    + manifestLocations.size()
                    + dataLocations.size()
                    + metadataLocations.size()
                    + statsLocations.size()
                    + partitionStatsLocations.size()))
        .deleteFile(argumentCaptor.capture());

    List<String> deletedPaths = argumentCaptor.getAllValues();
    assertThat(deletedPaths)
        .as("should contain all created manifest lists")
        .containsAll(manifestListLocations);
    assertThat(deletedPaths)
        .as("should contain all created manifests")
        .containsAll(manifestLocations);
    assertThat(deletedPaths).as("should contain all created data").containsAll(dataLocations);
    assertThat(deletedPaths)
        .as("should contain all created metadata locations")
        .containsAll(metadataLocations);
    assertThat(deletedPaths)
        .as("should contain all created statistics")
        .containsAll(statsLocations);
    assertThat(deletedPaths)
        .as("should contain all created partition stats files")
        .containsAll(partitionStatsLocations);
  }

  @Test
  public void dropTableDataDoNotThrowWhenDeletesFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    TableMetadata tableMetadata = readMetadataVersion(3);
    Set<Snapshot> snapshotSet = Sets.newHashSet(table.snapshots());

    FileIO fileIO = createMockFileIO(table.io());

    CatalogUtil.dropTableData(fileIO, tableMetadata);
    Mockito.verify(
            fileIO,
            Mockito.times(
                manifestListLocations(snapshotSet).size()
                    + manifestLocations(snapshotSet, fileIO).size()
                    + dataLocations(snapshotSet, table.io()).size()
                    + metadataLocations(tableMetadata).size()))
        .deleteFile(ArgumentMatchers.anyString());
  }

  @Test
  public void shouldNotDropDataFilesIfGcNotEnabled() {
    table.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    TableMetadata tableMetadata = readMetadataVersion(4);
    Set<Snapshot> snapshotSet = Sets.newHashSet(table.snapshots());

    Set<String> manifestListLocations = manifestListLocations(snapshotSet);
    Set<String> manifestLocations = manifestLocations(snapshotSet, table.io());
    Set<String> metadataLocations = metadataLocations(tableMetadata);
    assertThat(manifestListLocations).as("should have 2 manifest lists").hasSize(2);
    assertThat(metadataLocations).as("should have 4 metadata locations").hasSize(4);

    FileIO fileIO = createMockFileIO(table.io());

    CatalogUtil.dropTableData(fileIO, tableMetadata);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.verify(
            fileIO,
            Mockito.times(
                manifestListLocations.size() + manifestLocations.size() + metadataLocations.size()))
        .deleteFile(argumentCaptor.capture());

    List<String> deletedPaths = argumentCaptor.getAllValues();
    assertThat(deletedPaths)
        .as("should contain all created manifest lists")
        .containsAll(manifestListLocations);
    assertThat(deletedPaths)
        .as("should contain all created manifests")
        .containsAll(manifestLocations);
    assertThat(deletedPaths)
        .as("should contain all created metadata locations")
        .containsAll(metadataLocations);
  }

  private static FileIO createMockFileIO(FileIO wrapped) {
    FileIO mockIO = Mockito.mock(FileIO.class);

    Mockito.when(mockIO.newInputFile(Mockito.anyString()))
        .thenAnswer(invocation -> wrapped.newInputFile((String) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.anyString(), Mockito.anyLong()))
        .thenAnswer(
            invocation ->
                wrapped.newInputFile(invocation.getArgument(0), invocation.getArgument(1)));
    Mockito.when(mockIO.newInputFile(Mockito.any(ManifestFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((ManifestFile) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.any(DataFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((DataFile) invocation.getArgument(0)));
    Mockito.when(mockIO.newInputFile(Mockito.any(DeleteFile.class)))
        .thenAnswer(invocation -> wrapped.newInputFile((DeleteFile) invocation.getArgument(0)));

    return mockIO;
  }

  private static Set<String> manifestListLocations(Set<Snapshot> snapshotSet) {
    return snapshotSet.stream().map(Snapshot::manifestListLocation).collect(Collectors.toSet());
  }

  private static Set<String> manifestLocations(Set<Snapshot> snapshotSet, FileIO io) {
    return snapshotSet.stream()
        .flatMap(snapshot -> snapshot.allManifests(io).stream())
        .map(ManifestFile::path)
        .collect(Collectors.toSet());
  }

  private static Set<String> dataLocations(Set<Snapshot> snapshotSet, FileIO io) {
    return snapshotSet.stream()
        .flatMap(snapshot -> StreamSupport.stream(snapshot.addedDataFiles(io).spliterator(), false))
        .map(dataFile -> dataFile.path().toString())
        .collect(Collectors.toSet());
  }

  private static Set<String> metadataLocations(TableMetadata tableMetadata) {
    Set<String> metadataLocations =
        tableMetadata.previousFiles().stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toSet());
    metadataLocations.add(tableMetadata.metadataFileLocation());
    return metadataLocations;
  }

  private static Set<String> statsLocations(TableMetadata tableMetadata) {
    return tableMetadata.statisticsFiles().stream()
        .map(StatisticsFile::path)
        .collect(Collectors.toSet());
  }

  private static StatisticsFile writeStatsFile(
      long snapshotId, long snapshotSequenceNumber, String statsLocation, FileIO fileIO)
      throws IOException {
    try (PuffinWriter puffinWriter = Puffin.write(fileIO.newOutputFile(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              ImmutableList.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();

      return new GenericStatisticsFile(
          snapshotId,
          statsLocation,
          puffinWriter.fileSize(),
          puffinWriter.footerSize(),
          puffinWriter.writtenBlobsMetadata().stream()
              .map(GenericBlobMetadata::from)
              .collect(ImmutableList.toImmutableList()));
    }
  }

  private static PartitionStatisticsFile writePartitionStatsFile(
      long snapshotId, String statsLocation, FileIO fileIO) {
    PositionOutputStream positionOutputStream;
    try {
      positionOutputStream = fileIO.newOutputFile(statsLocation).create();
      positionOutputStream.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .fileSizeInBytes(42L)
        .path(statsLocation)
        .build();
  }

  private static Set<String> partitionStatsLocations(TableMetadata tableMetadata) {
    return tableMetadata.partitionStatisticsFiles().stream()
        .map(PartitionStatisticsFile::path)
        .collect(Collectors.toSet());
  }
}
