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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.http.HttpHeaders;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestRESTTableOperationsFreshness {

  private static final String TABLE_PATH = "v1/namespaces/ns/tables/tbl";
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  private RESTClient client;
  private FileIO io;
  private TableMetadata metadata;
  private Set<Endpoint> endpoints;

  @BeforeEach
  public void before() {
    client = mock(RESTClient.class);
    io = mock(FileIO.class);
    metadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            "s3://bucket/table",
            ImmutableMap.of());
    endpoints = ImmutableSet.of(Endpoint.V1_LOAD_TABLE, Endpoint.V1_UPDATE_TABLE);
  }

  @Test
  public void refreshSendsIfNoneMatchWhenETagAvailable() {
    // First refresh returns a response with ETag
    LoadTableResponse firstResponse = loadTableResponse(metadata);
    ArgumentCaptor<Map<String, String>> headersCaptor = headersCaptor();

    mockGetWithETag(firstResponse, "etag-123");

    RESTTableOperations ops =
        new RESTTableOperations(client, TABLE_PATH, ImmutableMap::of, io, metadata, endpoints);
    ops.refresh();

    // Second refresh should send If-None-Match
    mockGetReturningNull();

    ops.refresh();

    // Verify the second call included If-None-Match
    org.mockito.Mockito.verify(client, org.mockito.Mockito.times(2))
        .get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            headersCaptor.capture(),
            any(Consumer.class),
            any(Consumer.class));

    Map<String, String> secondCallHeaders = headersCaptor.getAllValues().get(1);
    assertThat(secondCallHeaders).containsEntry(HttpHeaders.IF_NONE_MATCH, "etag-123");
  }

  @Test
  public void refreshReturnsCurrentMetadataOn304() {
    LoadTableResponse firstResponse = loadTableResponse(metadata);
    mockGetWithETag(firstResponse, "etag-456");

    RESTTableOperations ops =
        new RESTTableOperations(client, TABLE_PATH, ImmutableMap::of, io, metadata, endpoints);
    TableMetadata afterFirstRefresh = ops.refresh();

    // Second refresh returns null (304 Not Modified)
    mockGetReturningNull();

    TableMetadata afterSecondRefresh = ops.refresh();
    assertThat(afterSecondRefresh).isSameAs(afterFirstRefresh);
  }

  @Test
  public void refreshDoesNotSendIfNoneMatchWithoutETag() {
    // Response without ETag header
    LoadTableResponse response = loadTableResponse(metadata);
    mockGetWithETag(response, null);

    RESTTableOperations ops =
        new RESTTableOperations(client, TABLE_PATH, ImmutableMap::of, io, metadata, endpoints);
    ops.refresh();

    ArgumentCaptor<Map<String, String>> headersCaptor = headersCaptor();
    org.mockito.Mockito.verify(client)
        .get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            headersCaptor.capture(),
            any(Consumer.class),
            any(Consumer.class));

    assertThat(headersCaptor.getValue()).doesNotContainKey(HttpHeaders.IF_NONE_MATCH);
  }

  @Test
  public void commitCapturesETag() {
    // Set up initial state
    LoadTableResponse refreshResponse = loadTableResponse(metadata);
    mockGetWithETag(refreshResponse, null);

    RESTTableOperations ops =
        new RESTTableOperations(client, TABLE_PATH, ImmutableMap::of, io, metadata, endpoints);
    ops.refresh();

    // Commit returns a response with ETag
    TableMetadata updatedMetadata =
        TableMetadata.newTableMetadata(
            SCHEMA,
            org.apache.iceberg.PartitionSpec.unpartitioned(),
            "s3://bucket/table/v2",
            ImmutableMap.of());
    LoadTableResponse commitResponse = loadTableResponse(updatedMetadata);
    mockPostWithETag(commitResponse, "etag-from-commit");

    ops.commit(metadata, updatedMetadata);

    // Next refresh should send If-None-Match with the commit ETag
    mockGetReturningNull();
    ops.refresh();

    ArgumentCaptor<Map<String, String>> headersCaptor = headersCaptor();
    org.mockito.Mockito.verify(client, org.mockito.Mockito.times(2))
        .get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            headersCaptor.capture(),
            any(Consumer.class),
            any(Consumer.class));

    Map<String, String> refreshAfterCommitHeaders = headersCaptor.getAllValues().get(1);
    assertThat(refreshAfterCommitHeaders)
        .containsEntry(HttpHeaders.IF_NONE_MATCH, "etag-from-commit");
  }

  @Test
  public void refreshMergesReadHeaders() {
    Map<String, String> baseHeaders = ImmutableMap.of("Authorization", "Bearer token");
    LoadTableResponse response = loadTableResponse(metadata);
    mockGetWithETag(response, "etag-789");

    RESTTableOperations ops =
        new RESTTableOperations(client, TABLE_PATH, () -> baseHeaders, io, metadata, endpoints);
    ops.refresh();

    // Second refresh should merge read headers with If-None-Match
    mockGetReturningNull();
    ops.refresh();

    ArgumentCaptor<Map<String, String>> headersCaptor = headersCaptor();
    org.mockito.Mockito.verify(client, org.mockito.Mockito.times(2))
        .get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            headersCaptor.capture(),
            any(Consumer.class),
            any(Consumer.class));

    Map<String, String> secondCallHeaders = headersCaptor.getAllValues().get(1);
    assertThat(secondCallHeaders)
        .containsEntry("Authorization", "Bearer token")
        .containsEntry(HttpHeaders.IF_NONE_MATCH, "etag-789");
  }

  @SuppressWarnings("unchecked")
  private void mockGetWithETag(LoadTableResponse response, String eTag) {
    when(client.get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            any(Map.class),
            any(Consumer.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<Map<String, String>> responseHeaders = invocation.getArgument(5);
              if (eTag != null) {
                responseHeaders.accept(ImmutableMap.of(HttpHeaders.ETAG, eTag));
              } else {
                responseHeaders.accept(ImmutableMap.of());
              }
              return response;
            });
  }

  @SuppressWarnings("unchecked")
  private void mockGetReturningNull() {
    when(client.get(
            eq(TABLE_PATH),
            eq(ImmutableMap.of()),
            eq(LoadTableResponse.class),
            any(Map.class),
            any(Consumer.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<Map<String, String>> responseHeaders = invocation.getArgument(5);
              responseHeaders.accept(ImmutableMap.of());
              return null;
            });
  }

  @SuppressWarnings("unchecked")
  private void mockPostWithETag(LoadTableResponse response, String eTag) {
    when(client.post(
            eq(TABLE_PATH),
            any(),
            eq(LoadTableResponse.class),
            any(Supplier.class),
            any(Consumer.class),
            any(Consumer.class)))
        .thenAnswer(
            invocation -> {
              Consumer<Map<String, String>> responseHeaders = invocation.getArgument(5);
              if (eTag != null) {
                responseHeaders.accept(ImmutableMap.of(HttpHeaders.ETAG, eTag));
              } else {
                responseHeaders.accept(ImmutableMap.of());
              }
              return response;
            });
  }

  @SuppressWarnings("unchecked")
  private static ArgumentCaptor<Map<String, String>> headersCaptor() {
    return ArgumentCaptor.forClass(Map.class);
  }

  private static LoadTableResponse loadTableResponse(TableMetadata tableMetadata) {
    return LoadTableResponse.builder().withTableMetadata(tableMetadata).build();
  }
}
