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
package org.apache.iceberg.aws.s3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.CapturingFailureHandler;
import org.apache.iceberg.io.FailureCategory;
import org.apache.iceberg.io.FileFailure;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestS3FileIOFailureHandler {

  private S3FileIO io(S3Client client) {
    return new S3FileIO(() -> client);
  }

  @Test
  public void singleFailureFromS3Error() {
    S3Client client = mock(S3Client.class);
    DeleteObjectsResponse response =
        DeleteObjectsResponse.builder()
            .errors(S3Error.builder().key("k1").code("AccessDenied").message("nope").build())
            .build();
    when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(response);

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = io(client)) {
      assertThatThrownBy(() -> fileIO.deleteFiles(List.of("s3://b/k1"), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.failures()).hasSize(1);
    FileFailure failure = handler.failures().get(0);
    assertThat(failure.path()).isEqualTo("s3://b/k1");
    assertThat(failure.category()).isEqualTo(FailureCategory.AUTH);
    assertThat(failure.rawErrorCode()).isEqualTo("AccessDenied");
    assertThat(failure.cause()).isNull();
  }

  @Test
  public void multipleFailuresFromS3Errors() {
    S3Client client = mock(S3Client.class);
    DeleteObjectsResponse response =
        DeleteObjectsResponse.builder()
            .errors(
                S3Error.builder().key("k1").code("NoSuchKey").build(),
                S3Error.builder().key("k2").code("SlowDown").build(),
                S3Error.builder().key("k3").code("InternalError").build())
            .build();
    when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(response);

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = io(client)) {
      assertThatThrownBy(
              () -> fileIO.deleteFiles(List.of("s3://b/k1", "s3://b/k2", "s3://b/k3"), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactlyInAnyOrder("s3://b/k1", "s3://b/k2", "s3://b/k3");
    assertThat(handler.categoryCounts())
        .containsEntry(FailureCategory.NOT_FOUND, 1L)
        .containsEntry(FailureCategory.THROTTLED, 1L)
        .containsEntry(FailureCategory.TRANSIENT, 1L);
  }

  @Test
  public void exceptionThrownAppliesToEveryKeyInBatch() {
    S3Client client = mock(S3Client.class);
    S3Exception thrown =
        (S3Exception)
            S3Exception.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("SlowDown").build())
                .statusCode(503)
                .build();
    when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(thrown);

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = io(client)) {
      assertThatThrownBy(() -> fileIO.deleteFiles(List.of("s3://b/k1", "s3://b/k2"), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactlyInAnyOrder("s3://b/k1", "s3://b/k2");
    assertThat(handler.failures())
        .allSatisfy(
            f -> {
              assertThat(f.category()).isEqualTo(FailureCategory.THROTTLED);
              assertThat(f.rawErrorCode()).isEqualTo("SlowDown");
              assertThat(f.cause()).isSameAs(thrown);
            });
  }

  @Test
  public void noFailuresWhenResponseIsClean() {
    S3Client client = mock(S3Client.class);
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(DeleteObjectsResponse.builder().build());

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = io(client)) {
      fileIO.deleteFiles(List.of("s3://b/k1", "s3://b/k2"), handler);
    }

    assertThat(handler.failures()).isEmpty();
  }

  @Test
  public void mixedSuccessAndFailureOnlyReportsFailedKey() {
    S3Client client = mock(S3Client.class);
    // Single batch: only k2 comes back as an error; k1 and k3 succeeded.
    DeleteObjectsResponse response =
        DeleteObjectsResponse.builder()
            .errors(S3Error.builder().key("k2").code("AccessDenied").build())
            .build();
    when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(response);

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = io(client)) {
      assertThatThrownBy(
              () -> fileIO.deleteFiles(List.of("s3://b/k1", "s3://b/k2", "s3://b/k3"), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    assertThat(handler.paths()).containsExactly("s3://b/k2");
    FileFailure failure = handler.failures().get(0);
    assertThat(failure.category()).isEqualTo(FailureCategory.AUTH);
    assertThat(failure.rawErrorCode()).isEqualTo("AccessDenied");
  }

  @Test
  public void failedBatchDoesNotPreventRemainingBatches() {
    S3Client client = mock(S3Client.class);
    S3Exception thrown =
        (S3Exception)
            S3Exception.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode("SlowDown").build())
                .statusCode(503)
                .build();
    // Batch size 1 -> two single-key batches. One batch's request throws; the other returns
    // clean. Both batches must be attempted, and only the failing one is reported.
    when(client.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenThrow(thrown)
        .thenReturn(DeleteObjectsResponse.builder().build());

    CapturingFailureHandler handler = new CapturingFailureHandler();
    try (S3FileIO fileIO = new S3FileIO(() -> client)) {
      fileIO.initialize(Map.of(S3FileIOProperties.DELETE_BATCH_SIZE, "1"));
      assertThatThrownBy(() -> fileIO.deleteFiles(List.of("s3://b/k1", "s3://b/k2"), handler))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessageContaining("Failed to delete");
    }

    // Both batches were attempted even though one threw.
    verify(client, times(2)).deleteObjects(any(DeleteObjectsRequest.class));
    assertThat(handler.failures()).hasSize(1);
    FileFailure failure = handler.failures().get(0);
    assertThat(failure.path()).isIn("s3://b/k1", "s3://b/k2");
    assertThat(failure.category()).isEqualTo(FailureCategory.THROTTLED);
    assertThat(failure.rawErrorCode()).isEqualTo("SlowDown");
    assertThat(failure.cause()).isSameAs(thrown);
  }
}
