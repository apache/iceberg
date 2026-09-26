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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.StorageCredential;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.S3Error;

class TestS3FileIOBulkDelete {
  private S3Client root;
  private S3Client first;
  private S3Client second;

  @BeforeEach
  void createClients() {
    root = mock(S3Client.class);
    first = mock(S3Client.class);
    second = mock(S3Client.class);
    MockClientFactory.clients = Map.of("root", root, "first", first, "second", second);
    for (S3Client client : MockClientFactory.clients.values()) {
      when(client.deleteObjects(any(DeleteObjectsRequest.class)))
          .thenReturn(DeleteObjectsResponse.builder().build());
    }
  }

  @Test
  void partialBatchUsesVendedClient() {
    try (S3FileIO fileIO = fileIO()) {
      fileIO.deleteFiles(List.of("s3://bucket/a/one"));
    }

    assertRequests(first, Map.of("bucket", Set.of("a/one")));
    verify(root, never()).deleteObjects(any(DeleteObjectsRequest.class));
    verify(second, never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  void exactBatchUsesVendedClient() {
    try (S3FileIO fileIO = fileIO()) {
      fileIO.deleteFiles(List.of("s3://bucket/a/one", "s3://bucket/a/two"));
    }

    assertRequests(first, Map.of("bucket", Set.of("a/one", "a/two")));
    verify(root, never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  void remainderUsesVendedClient() {
    try (S3FileIO fileIO = fileIO()) {
      fileIO.deleteFiles(List.of("s3://bucket/a/one", "s3://bucket/a/two", "s3://bucket/a/three"));
    }

    assertRequests(
        first, Map.of("bucket", Set.of("a/one", "a/two")), Map.of("bucket", Set.of("a/three")));
    verify(root, never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  void mixedPrefixesUseSeparateClients() {
    try (S3FileIO fileIO = fileIO()) {
      fileIO.deleteFiles(
          List.of(
              "s3://bucket/a/one", "s3://bucket/b/one", "s3://bucket/b/two", "s3://bucket/a/two"));
    }

    assertRequests(first, Map.of("bucket", Set.of("a/one", "a/two")));
    assertRequests(second, Map.of("bucket", Set.of("b/one", "b/two")));
    verify(root, never()).deleteObjects(any(DeleteObjectsRequest.class));
  }

  @Test
  void differentBucketsUseSeparateRequests() {
    try (S3FileIO fileIO = fileIO()) {
      fileIO.deleteFiles(List.of("s3://other/one", "s3://another/two"));
    }

    assertRequests(root, Map.of("other", Set.of("one")), Map.of("another", Set.of("two")));
  }

  @Test
  void partialS3FailureIsCounted() {
    when(first.deleteObjects(any(DeleteObjectsRequest.class)))
        .thenReturn(
            DeleteObjectsResponse.builder().errors(S3Error.builder().key("a/one").build()).build());

    try (S3FileIO fileIO = fileIO()) {
      assertThatThrownBy(
              () -> fileIO.deleteFiles(List.of("s3://bucket/a/one", "s3://bucket/a/two")))
          .isInstanceOf(BulkDeletionFailureException.class)
          .hasMessage("Failed to delete 1 files")
          .satisfies(
              failure ->
                  assertThat(((BulkDeletionFailureException) failure).numberFailedObjects())
                      .isEqualTo(1));
    }

    assertRequests(first, Map.of("bucket", Set.of("a/one", "a/two")));
  }

  private S3FileIO fileIO() {
    S3FileIO fileIO = new S3FileIO();
    fileIO.setCredentials(
        List.of(
            StorageCredential.create(
                "s3://bucket/a/",
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, "first",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret")),
            StorageCredential.create(
                "s3://bucket/b/",
                Map.of(
                    S3FileIOProperties.ACCESS_KEY_ID, "second",
                    S3FileIOProperties.SECRET_ACCESS_KEY, "secret"))));
    fileIO.initialize(
        Map.of(
            S3FileIOProperties.CLIENT_FACTORY,
            MockClientFactory.class.getName(),
            S3FileIOProperties.DELETE_BATCH_SIZE,
            "2"));
    return fileIO;
  }

  @SafeVarargs
  private final void assertRequests(S3Client client, Map<String, Set<String>>... expected) {
    ArgumentCaptor<DeleteObjectsRequest> requests =
        ArgumentCaptor.forClass(DeleteObjectsRequest.class);
    verify(client, org.mockito.Mockito.times(expected.length)).deleteObjects(requests.capture());
    assertThat(requests.getAllValues())
        .map(
            request ->
                Map.of(
                    request.bucket(),
                    request.delete().objects().stream()
                        .map(object -> object.key())
                        .collect(Collectors.toSet())))
        .containsExactlyInAnyOrder(expected);
  }

  public static class MockClientFactory implements S3FileIOAwsClientFactory {
    private static Map<String, S3Client> clients;
    private S3Client client;

    @Override
    public S3Client s3() {
      return client;
    }

    @Override
    public S3AsyncClient s3Async() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      String accessKeyId = properties.get(S3FileIOProperties.ACCESS_KEY_ID);
      client = accessKeyId == null ? clients.get("root") : clients.get(accessKeyId);
    }
  }
}
