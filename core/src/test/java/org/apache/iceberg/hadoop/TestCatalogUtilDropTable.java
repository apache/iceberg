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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class TestCatalogUtilDropTable extends HadoopTableTestBase {

  @Test
  public void dropTableDataDeletesExpectedFiles() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    TableMetadata tableMetadata = readMetadataVersion(3);
    Set<Snapshot> snapshotSet = Sets.newHashSet(table.snapshots());

    Set<String> manifestListLocations = manifestListLocations(snapshotSet);
    Set<String> manifestLocations = manifestLocations(snapshotSet, table.io());
    Set<String> dataLocations = dataLocations(snapshotSet, table.io());
    Set<String> metadataLocations = metadataLocations(tableMetadata);
    Assertions.assertThat(manifestListLocations).as("should have 2 manifest lists").hasSize(2);
    Assertions.assertThat(metadataLocations).as("should have 3 metadata locations").hasSize(3);

    FileIO fileIO = Mockito.mock(FileIO.class);
    Mockito.when(fileIO.newInputFile(Mockito.anyString()))
        .thenAnswer(invocation -> table.io().newInputFile(invocation.getArgument(0)));
    Mockito.when(fileIO.newInputFile(Mockito.anyString(), Mockito.anyLong()))
        .thenAnswer(
            invocation ->
                table.io().newInputFile(invocation.getArgument(0), invocation.getArgument(1)));

    CatalogUtil.dropTableData(fileIO, tableMetadata);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.verify(
            fileIO,
            Mockito.times(
                manifestListLocations.size()
                    + manifestLocations.size()
                    + dataLocations.size()
                    + metadataLocations.size()))
        .deleteFile(argumentCaptor.capture());

    List<String> deletedPaths = argumentCaptor.getAllValues();
    Assertions.assertThat(deletedPaths)
        .as("should contain all created manifest lists")
        .containsAll(manifestListLocations);
    Assertions.assertThat(deletedPaths)
        .as("should contain all created manifests")
        .containsAll(manifestLocations);
    Assertions.assertThat(deletedPaths)
        .as("should contain all created data")
        .containsAll(dataLocations);
    Assertions.assertThat(deletedPaths)
        .as("should contain all created metadata locations")
        .containsAll(metadataLocations);
  }

  @Test
  public void dropTableDataDoNotThrowWhenDeletesFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_B).commit();

    TableMetadata tableMetadata = readMetadataVersion(3);
    Set<Snapshot> snapshotSet = Sets.newHashSet(table.snapshots());

    FileIO fileIO = Mockito.mock(FileIO.class);
    Mockito.when(fileIO.newInputFile(Mockito.anyString()))
        .thenAnswer(invocation -> table.io().newInputFile(invocation.getArgument(0)));
    Mockito.when(fileIO.newInputFile(Mockito.anyString(), Mockito.anyLong()))
        .thenAnswer(
            invocation ->
                table.io().newInputFile(invocation.getArgument(0), invocation.getArgument(1)));
    Mockito.doThrow(new RuntimeException()).when(fileIO).deleteFile(ArgumentMatchers.anyString());

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
    Assertions.assertThat(manifestListLocations).as("should have 2 manifest lists").hasSize(2);
    Assertions.assertThat(metadataLocations).as("should have 4 metadata locations").hasSize(4);

    FileIO fileIO = Mockito.mock(FileIO.class);
    Mockito.when(fileIO.newInputFile(Mockito.anyString()))
        .thenAnswer(invocation -> table.io().newInputFile(invocation.getArgument(0)));

    CatalogUtil.dropTableData(fileIO, tableMetadata);
    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    Mockito.verify(
            fileIO,
            Mockito.times(
                manifestListLocations.size() + manifestLocations.size() + metadataLocations.size()))
        .deleteFile(argumentCaptor.capture());

    List<String> deletedPaths = argumentCaptor.getAllValues();
    Assertions.assertThat(deletedPaths)
        .as("should contain all created manifest lists")
        .containsAll(manifestListLocations);
    Assertions.assertThat(deletedPaths)
        .as("should contain all created manifests")
        .containsAll(manifestLocations);
    Assertions.assertThat(deletedPaths)
        .as("should contain all created metadata locations")
        .containsAll(metadataLocations);
  }

  private Set<String> manifestListLocations(Set<Snapshot> snapshotSet) {
    return snapshotSet.stream().map(Snapshot::manifestListLocation).collect(Collectors.toSet());
  }

  private Set<String> manifestLocations(Set<Snapshot> snapshotSet, FileIO io) {
    return snapshotSet.stream()
        .flatMap(snapshot -> snapshot.allManifests(io).stream())
        .map(ManifestFile::path)
        .collect(Collectors.toSet());
  }

  private Set<String> dataLocations(Set<Snapshot> snapshotSet, FileIO io) {
    return snapshotSet.stream()
        .flatMap(snapshot -> StreamSupport.stream(snapshot.addedDataFiles(io).spliterator(), false))
        .map(dataFile -> dataFile.path().toString())
        .collect(Collectors.toSet());
  }

  private Set<String> metadataLocations(TableMetadata tableMetadata) {
    Set<String> metadataLocations =
        tableMetadata.previousFiles().stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toSet());
    metadataLocations.add(tableMetadata.metadataFileLocation());
    return metadataLocations;
  }
}
