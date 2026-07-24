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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CapturingFailureHandler;
import org.apache.iceberg.io.FailureCategory;
import org.apache.iceberg.io.FileFailure;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializableFunction;
import org.junit.jupiter.api.Test;

public class TestADLSFileIOFailureHandler {

  private static final String ACCOUNT = "account.dfs.core.windows.net";
  private static final String CONTAINER = "container";

  private static String path(String key) {
    return "abfs://" + CONTAINER + "@" + ACCOUNT + "/" + key;
  }

  private static DataLakeStorageException storageException(String errorCode, int statusCode) {
    HttpHeaders headers = new HttpHeaders();
    if (errorCode != null) {
      headers.set("x-ms-error-code", errorCode);
    }
    HttpResponse response = mock(HttpResponse.class);
    when(response.getHeaders()).thenReturn(headers);
    when(response.getStatusCode()).thenReturn(statusCode);
    return new DataLakeStorageException("test", response, null);
  }

  /** Builds an ADLSFileIO whose file-system client routes per-key delete calls per the map. */
  private ADLSFileIO ioThrowingPerKey(Map<String, DataLakeStorageException> errorsByKey) {
    DataLakeFileSystemClient fsClient = mock(DataLakeFileSystemClient.class);
    when(fsClient.getFileClient(anyString()))
        .thenAnswer(
            invocation -> {
              String key = invocation.getArgument(0);
              DataLakeFileClient fileClient = mock(DataLakeFileClient.class);
              DataLakeStorageException ex = errorsByKey.get(key);
              if (ex != null) {
                doThrow(ex).when(fileClient).delete();
              }
              return fileClient;
            });
    SerializableFunction<ADLSLocation, DataLakeFileSystemClient> supplier = loc -> fsClient;
    return new ADLSFileIO(supplier);
  }

  @Test
  public void singleFailure() {
    Map<String, DataLakeStorageException> errors = Maps.newHashMap();
    errors.put("k1", storageException("AuthenticationFailed", 401));
    CapturingFailureHandler handler = new CapturingFailureHandler();

    try (ADLSFileIO fileIO = ioThrowingPerKey(errors)) {
      assertThatThrownBy(() -> fileIO.deleteFiles(List.of(path("k1")), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.failures()).hasSize(1);
    FileFailure failure = handler.failures().get(0);
    assertThat(failure.path()).isEqualTo(path("k1"));
    assertThat(failure.category()).isEqualTo(FailureCategory.AUTH);
    assertThat(failure.rawErrorCode()).isEqualTo("AuthenticationFailed");
    assertThat(failure.cause()).isInstanceOf(DataLakeStorageException.class);
  }

  @Test
  public void missingObjectIsTreatedAsSuccess() {
    Map<String, DataLakeStorageException> errors = Maps.newHashMap();
    // Azure raises PathNotFound when the object is already gone. For a delete that is the desired
    // end state, so it is not reported to the handler and does not fail the bulk delete.
    errors.put("k1", storageException("PathNotFound", 404));
    CapturingFailureHandler handler = new CapturingFailureHandler();

    try (ADLSFileIO fileIO = ioThrowingPerKey(errors)) {
      fileIO.deleteFiles(List.of(path("k1")), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }

  @Test
  public void multipleFailures() {
    Map<String, DataLakeStorageException> errors = Maps.newHashMap();
    errors.put("k1", storageException("AuthenticationFailed", 401));
    errors.put("k2", storageException("ServerBusy", 503));
    errors.put("k3", storageException("InternalError", 500));
    CapturingFailureHandler handler = new CapturingFailureHandler();

    try (ADLSFileIO fileIO = ioThrowingPerKey(errors)) {
      assertThatThrownBy(
              () -> fileIO.deleteFiles(List.of(path("k1"), path("k2"), path("k3")), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactlyInAnyOrder(path("k1"), path("k2"), path("k3"));
    assertThat(handler.categoryCounts())
        .containsEntry(FailureCategory.AUTH, 1L)
        .containsEntry(FailureCategory.THROTTLED, 1L)
        .containsEntry(FailureCategory.TRANSIENT, 1L);
  }

  @Test
  public void mixedSuccessAndFailureOnlyReportsFailures() {
    Map<String, DataLakeStorageException> errors = Maps.newHashMap();
    errors.put("k2", storageException("AuthenticationFailed", 401));
    CapturingFailureHandler handler = new CapturingFailureHandler();

    try (ADLSFileIO fileIO = ioThrowingPerKey(errors)) {
      assertThatThrownBy(
              () -> fileIO.deleteFiles(List.of(path("k1"), path("k2"), path("k3")), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactly(path("k2"));
  }

  @Test
  public void noFailuresMeansHandlerNotCalled() {
    CapturingFailureHandler handler = new CapturingFailureHandler();

    try (ADLSFileIO fileIO = ioThrowingPerKey(Maps.newHashMap())) {
      fileIO.deleteFiles(List.of(path("k1"), path("k2")), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }
}
