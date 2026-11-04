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
package org.apache.iceberg.encryption;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MockFileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PositionDeletesScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class TestInputFilesDecryptor {

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private static final DataFile DATA_FILE =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data.parquet")
          .withFileSizeInBytes(123L)
          .withRecordCount(10L)
          .withFormat(FileFormat.PARQUET)
          .build();

  private static final DataFile OTHER_DATA_FILE =
      DataFiles.builder(SPEC)
          .withPath("/path/to/other-data.parquet")
          .withFileSizeInBytes(321L)
          .withRecordCount(10L)
          .withFormat(FileFormat.PARQUET)
          .build();

  private static final DeleteFile DELETE_FILE =
      FileMetadata.deleteFileBuilder(SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/deletes.parquet")
          .withFileSizeInBytes(45L)
          .withRecordCount(2L)
          .withFormat(FileFormat.PARQUET)
          .build();

  private EncryptingFileIO encryptingIO;
  private InputFile dataInputFile;
  private InputFile deleteInputFile;
  private InputFile otherDataInputFile;
  private Map<String, InputFile> inputFiles;

  @BeforeEach
  void before() {
    this.dataInputFile = Mockito.mock(InputFile.class);
    this.deleteInputFile = Mockito.mock(InputFile.class);
    this.otherDataInputFile = Mockito.mock(InputFile.class);
    this.inputFiles =
        ImmutableMap.of(
            DATA_FILE.location(),
            dataInputFile,
            DELETE_FILE.location(),
            deleteInputFile,
            OTHER_DATA_FILE.location(),
            otherDataInputFile);
    this.encryptingIO = Mockito.mock(EncryptingFileIO.class);
    Mockito.when(encryptingIO.bulkDecrypt(any()))
        .thenAnswer(
            invocation -> {
              Iterable<ContentFile<?>> files = invocation.getArgument(0);
              ImmutableMap.Builder<String, InputFile> decrypted = ImmutableMap.builder();
              files.forEach(
                  file -> decrypted.put(file.location(), inputFiles.get(file.location())));
              return decrypted.buildOrThrow();
            });
  }

  @Test
  void doesNotDecryptOnCreation() {
    InputFilesDecryptor.fromTasks(
        ImmutableList.of(new MockFileScanTask(DATA_FILE, new DeleteFile[] {DELETE_FILE})),
        encryptingIO);

    Mockito.verifyNoInteractions(encryptingIO);
  }

  @Test
  void inputFilesResolvedByBulkDecrypt() {
    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(
            ImmutableList.of(new MockFileScanTask(DATA_FILE, new DeleteFile[] {DELETE_FILE})),
            encryptingIO);

    assertThat(decryptor.getInputFile(DATA_FILE.location())).isSameAs(dataInputFile);
    assertThat(decryptor.getInputFile(DELETE_FILE.location())).isSameAs(deleteInputFile);

    Mockito.verify(encryptingIO, Mockito.times(1)).bulkDecrypt(any());
  }

  @Test
  void failsForFilesThatWereNotResolved() {
    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(
            ImmutableList.of(new MockFileScanTask(DATA_FILE)), encryptingIO);

    assertThatThrownBy(() -> decryptor.getInputFile("other/location"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find input file for location: other/location");
  }

  @Test
  void deduplicatesFiles() {
    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(
            ImmutableList.of(
                new MockFileScanTask(DATA_FILE, new DeleteFile[] {DELETE_FILE}),
                new MockFileScanTask(OTHER_DATA_FILE, new DeleteFile[] {DELETE_FILE})),
            encryptingIO);

    decryptor.getInputFile(DATA_FILE.location());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<Iterable<ContentFile<?>>> captor = ArgumentCaptor.forClass(Iterable.class);
    Mockito.verify(encryptingIO).bulkDecrypt(captor.capture());
    assertThat(captor.getValue())
        .containsExactlyInAnyOrder(DATA_FILE, OTHER_DATA_FILE, DELETE_FILE);
  }

  @Test
  void resolvesInputFilesOnceWhenAccessedConcurrently() throws Exception {
    Mockito.doAnswer(
            invocation -> {
              Thread.sleep(50);
              return ImmutableMap.of(DATA_FILE.location(), dataInputFile);
            })
        .when(encryptingIO)
        .bulkDecrypt(any());

    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(
            ImmutableList.of(new MockFileScanTask(DATA_FILE)), encryptingIO);

    int numReaders = 8;
    CyclicBarrier barrier = new CyclicBarrier(numReaders);
    ExecutorService pool = Executors.newFixedThreadPool(numReaders);
    try {
      List<Future<InputFile>> futures = Lists.newArrayList();
      for (int i = 0; i < numReaders; i++) {
        futures.add(
            pool.submit(
                () -> {
                  barrier.await(30, SECONDS);
                  return decryptor.getInputFile(DATA_FILE.location());
                }));
      }

      for (Future<InputFile> future : futures) {
        assertThat(future.get()).isSameAs(dataInputFile);
      }
    } finally {
      pool.shutdownNow();
    }

    Mockito.verify(encryptingIO, Mockito.times(1)).bulkDecrypt(any());
  }

  @Test
  void resolvesFilesOfAddedRowsTasks() {
    AddedRowsScanTask task = Mockito.mock(AddedRowsScanTask.class);
    Mockito.when(task.file()).thenReturn(DATA_FILE);
    Mockito.when(task.deletes()).thenReturn(ImmutableList.of(DELETE_FILE));

    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(ImmutableList.of(task), encryptingIO);

    assertThat(decryptor.getInputFile(DATA_FILE.location())).isSameAs(dataInputFile);
    assertThat(decryptor.getInputFile(DELETE_FILE.location())).isSameAs(deleteInputFile);

    Mockito.verify(encryptingIO, Mockito.times(1)).bulkDecrypt(any());
  }

  @Test
  void resolvesFilesOfDeletedDataFileTasks() {
    DeletedDataFileScanTask task = Mockito.mock(DeletedDataFileScanTask.class);
    Mockito.when(task.file()).thenReturn(DATA_FILE);
    Mockito.when(task.existingDeletes()).thenReturn(ImmutableList.of(DELETE_FILE));

    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(ImmutableList.of(task), encryptingIO);

    assertThat(decryptor.getInputFile(DATA_FILE.location())).isSameAs(dataInputFile);
    assertThat(decryptor.getInputFile(DELETE_FILE.location())).isSameAs(deleteInputFile);

    Mockito.verify(encryptingIO, Mockito.times(1)).bulkDecrypt(any());
  }

  @Test
  void resolvesFilesOfPositionDeletesTasks() {
    PositionDeletesScanTask task = Mockito.mock(PositionDeletesScanTask.class);
    Mockito.when(task.file()).thenReturn(DELETE_FILE);

    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(ImmutableList.of(task), encryptingIO);

    assertThat(decryptor.getInputFile(DELETE_FILE.location())).isSameAs(deleteInputFile);

    Mockito.verify(encryptingIO, Mockito.times(1)).bulkDecrypt(any());
  }

  @Test
  void failsForUnsupportedTasks() {
    ScanTask task = Mockito.mock(ScanTask.class);

    InputFilesDecryptor decryptor =
        InputFilesDecryptor.fromTasks(ImmutableList.of(task), encryptingIO);

    assertThatThrownBy(() -> decryptor.getInputFile(DATA_FILE.location()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unsupported scan task type: " + task.getClass().getName());
  }
}
