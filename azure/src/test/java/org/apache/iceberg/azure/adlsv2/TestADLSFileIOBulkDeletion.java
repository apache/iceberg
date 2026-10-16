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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestADLSFileIOBulkDeletion {
  private static final String PREFIX = "abfs://container@account.dfs.core.windows.net/";

  @ParameterizedTest(name = "reportsDeletionFailure: {0}")
  @MethodSource("deleteFailures")
  void reportsDeletionFailure(RuntimeException failure) {
    DataLakeFileClient fileClient = failingClient(failure);
    try (ADLSFileIO io = fileIO(Map.of("file", fileClient))) {
      BulkDeletionFailureException thrown =
          catchThrowableOfType(
              BulkDeletionFailureException.class, () -> io.deleteFiles(List.of(PREFIX + "file")));

      assertThat(thrown).isNotNull();
      assertThat(thrown.numberFailedObjects()).isEqualTo(1);
    }
  }

  @Test
  void attemptsEveryFileAndCountsOnlyFailures() {
    DataLakeFileClient successful = successfulClient();
    DataLakeFileClient forbidden = failingClient(storageFailure(403));
    DataLakeFileClient unavailable = failingClient(storageFailure(503));
    DataLakeFileClient missing = missingClient();
    Map<String, DataLakeFileClient> clients =
        Map.of(
            "successful",
            successful,
            "forbidden",
            forbidden,
            "unavailable",
            unavailable,
            "missing",
            missing);
    DataLakeFileSystemClient fileSystemClient = fileSystemClient(clients);

    try (ADLSFileIO io = new ADLSFileIO(location -> fileSystemClient)) {
      BulkDeletionFailureException thrown =
          catchThrowableOfType(
              BulkDeletionFailureException.class,
              () -> io.deleteFiles(clients.keySet().stream().map(PREFIX::concat).toList()));

      assertThat(thrown).isNotNull();
      assertThat(thrown.numberFailedObjects()).isEqualTo(2);
      clients.keySet().forEach(key -> verify(fileSystemClient).getFileClient(key));
    }
  }

  @Test
  void acceptsAlreadyMissingFile() {
    try (ADLSFileIO io = fileIO(Map.of("missing", missingClient()))) {
      assertThatCode(() -> io.deleteFiles(List.of(PREFIX + "missing"))).doesNotThrowAnyException();
    }
  }

  @Test
  void deletingFileTwiceSucceeds() {
    DataLakeFileClient fileClient = successfulClient();
    DataLakeStorageException notFound = storageFailure(404);
    doNothing().doThrow(notFound).when(fileClient).delete();
    when(fileClient.deleteIfExists()).thenReturn(true, false);
    DataLakeFileSystemClient fileSystemClient = fileSystemClient(Map.of("file", fileClient));

    try (ADLSFileIO io = new ADLSFileIO(location -> fileSystemClient)) {
      io.deleteFiles(List.of(PREFIX + "file"));
      assertThatCode(() -> io.deleteFiles(List.of(PREFIX + "file"))).doesNotThrowAnyException();
      verify(fileSystemClient, times(2)).getFileClient("file");
    }
  }

  @Test
  void deletesSuccessfulBatch() {
    DataLakeFileSystemClient fileSystemClient =
        fileSystemClient(Map.of("first", successfulClient(), "second", successfulClient()));
    try (ADLSFileIO io = new ADLSFileIO(location -> fileSystemClient)) {
      assertThatCode(() -> io.deleteFiles(List.of(PREFIX + "first", PREFIX + "second")))
          .doesNotThrowAnyException();
      verify(fileSystemClient).getFileClient("first");
      verify(fileSystemClient).getFileClient("second");
    }
  }

  @Test
  void acceptsEmptyBatch() {
    DataLakeFileSystemClient fileSystemClient = mock(DataLakeFileSystemClient.class);
    try (ADLSFileIO io = new ADLSFileIO(location -> fileSystemClient)) {
      assertThatCode(() -> io.deleteFiles(List.of())).doesNotThrowAnyException();
      verifyNoInteractions(fileSystemClient);
    }
  }

  @ParameterizedTest(name = "singleFileDeletionRemainsBestEffort: {0}")
  @MethodSource("deleteFailures")
  void singleFileDeletionRemainsBestEffort(RuntimeException failure) {
    try (ADLSFileIO io = fileIO(Map.of("file", failingClient(failure)))) {
      assertThatCode(() -> io.deleteFile(PREFIX + "file")).doesNotThrowAnyException();
    }
  }

  private static Stream<RuntimeException> deleteFailures() {
    return Stream.of(
        storageFailure(403),
        storageFailure(500),
        storageFailure(503),
        new UncheckedIOException(new IOException("Connection reset")));
  }

  private static DataLakeStorageException storageFailure(int status) {
    HttpResponse response = mock(HttpResponse.class);
    when(response.getStatusCode()).thenReturn(status);
    when(response.getHeaders()).thenReturn(new HttpHeaders());
    return new DataLakeStorageException("Deletion failed with status " + status, response, null);
  }

  private static DataLakeFileClient successfulClient() {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    when(fileClient.deleteIfExists()).thenReturn(true);
    return fileClient;
  }

  private static DataLakeFileClient missingClient() {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    doThrow(storageFailure(404)).when(fileClient).delete();
    when(fileClient.deleteIfExists()).thenReturn(false);
    return fileClient;
  }

  private static DataLakeFileClient failingClient(RuntimeException failure) {
    DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
    doThrow(failure).when(fileClient).delete();
    when(fileClient.deleteIfExists()).thenThrow(failure);
    return fileClient;
  }

  private static ADLSFileIO fileIO(Map<String, DataLakeFileClient> clients) {
    DataLakeFileSystemClient fileSystemClient = fileSystemClient(clients);
    return new ADLSFileIO(location -> fileSystemClient);
  }

  private static DataLakeFileSystemClient fileSystemClient(
      Map<String, DataLakeFileClient> clients) {
    DataLakeFileSystemClient fileSystemClient = mock(DataLakeFileSystemClient.class);
    when(fileSystemClient.getFileClient(anyString()))
        .thenAnswer(invocation -> clients.get(invocation.getArgument(0)));
    return fileSystemClient;
  }
}
