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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CapturingFailureHandler;
import org.apache.iceberg.io.FailureCategory;
import org.apache.iceberg.io.FileFailure;
import org.junit.jupiter.api.Test;

public class TestGCSFileIOFailureHandler {

  private static final String BUCKET = "test-bucket";

  private static String path(String key) {
    return "gs://" + BUCKET + "/" + key;
  }

  private static GCSFileIO ioWithBatchSize(Storage storage, int batchSize) {
    GCSFileIO io = new GCSFileIO(() -> storage);
    io.initialize(Map.of(GCPProperties.GCS_DELETE_BATCH_SIZE, String.valueOf(batchSize)));
    return io;
  }

  @SuppressWarnings("unchecked")
  @Test
  public void missingObjectIsTreatedAsSuccess() {
    Storage storage = mock(Storage.class);
    // A false result means the object was already gone, which is the desired end state for a
    // delete. It is not reported to the handler and does not raise BulkDeletionFailureException.
    when(storage.delete(any(Iterable.class))).thenReturn(List.of(false));

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (GCSFileIO io = new GCSFileIO(() -> storage)) {
      io.deleteFiles(List.of(path("k1")), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void mixedMissingAndDeletedObjectsAreAllSuccess() {
    Storage storage = mock(Storage.class);
    when(storage.delete(any(Iterable.class))).thenReturn(List.of(false, true, false));

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (GCSFileIO io = new GCSFileIO(() -> storage)) {
      io.deleteFiles(List.of(path("k1"), path("k2"), path("k3")), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void storageExceptionAppliesToEveryKeyInBatch() {
    Storage storage = mock(Storage.class);
    StorageException thrown = new StorageException(403, "forbidden", "forbidden", null);
    when(storage.delete(any(Iterable.class))).thenThrow(thrown);

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (GCSFileIO io = new GCSFileIO(() -> storage)) {
      // A StorageException is a real failure: every key in the batch is reported to the
      // handler and the bulk delete fails loudly with BulkDeletionFailureException.
      assertThatThrownBy(() -> io.deleteFiles(List.of(path("k1"), path("k2")), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactlyInAnyOrder(path("k1"), path("k2"));
    assertThat(handler.failures())
        .allSatisfy(
            f -> {
              assertThat(f.category()).isEqualTo(FailureCategory.AUTH);
              assertThat(f.rawErrorCode()).isEqualTo("forbidden");
              assertThat(f.cause()).isSameAs(thrown);
            });
  }

  @SuppressWarnings("unchecked")
  @Test
  public void noFailuresWhenAllSucceed() {
    Storage storage = mock(Storage.class);
    when(storage.delete(any(Iterable.class))).thenReturn(List.of(true, true));

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (GCSFileIO io = new GCSFileIO(() -> storage)) {
      io.deleteFiles(List.of(path("k1"), path("k2")), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void failedBatchDoesNotPreventRemainingBatches() {
    Storage storage = mock(Storage.class);
    StorageException thrown = new StorageException(403, "forbidden", "forbidden", null);
    // Batch size 1 -> two single-key batches, deleted serially. The first batch throws; the
    // second must still be attempted (best-effort), rather than the whole delete failing fast.
    when(storage.delete(any(Iterable.class))).thenThrow(thrown).thenReturn(List.of(true));

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (GCSFileIO io = ioWithBatchSize(storage, 1)) {
      assertThatThrownBy(() -> io.deleteFiles(List.of(path("k1"), path("k2")), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    // Both batches were attempted even though the first one threw.
    verify(storage, times(2)).delete(any(Iterable.class));
    // Only the failing batch's key is reported, classified from the StorageException.
    assertThat(handler.failures()).hasSize(1);
    FileFailure failure = handler.failures().get(0);
    assertThat(failure.path()).isEqualTo(path("k1"));
    assertThat(failure.category()).isEqualTo(FailureCategory.AUTH);
    assertThat(failure.rawErrorCode()).isEqualTo("forbidden");
    assertThat(failure.cause()).isSameAs(thrown);
  }
}
