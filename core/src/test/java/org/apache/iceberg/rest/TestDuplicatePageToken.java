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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.util.function.Consumer;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class TestDuplicatePageToken {

  @Test
  @SuppressWarnings("unchecked")
  public void testListNamespacesThrowsOnDuplicatePageToken() {
    RESTClient mockClient = Mockito.mock(RESTClient.class);

    // withAuthSession is a default method that returns this; stub it since Mockito
    // does not execute default methods by default
    when(mockClient.withAuthSession(any())).thenReturn(mockClient);

    // Config response with namespace listing endpoint enabled
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withEndpoints(ImmutableList.of(Endpoint.V1_LIST_NAMESPACES))
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ConfigResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(configResponse);

    // Namespace listing: always return the same page token "stuck"
    ListNamespacesResponse dupResponse =
        ListNamespacesResponse.builder()
            .add(Namespace.of("demo"))
            .nextPageToken("stuck")
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ListNamespacesResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(dupResponse);

    RESTCatalog catalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(), (config) -> mockClient);
    catalog.initialize("test", ImmutableMap.of());

    assertThatThrownBy(() -> catalog.listNamespaces())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicate page token");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListTablesThrowsOnDuplicatePageToken() {
    RESTClient mockClient = Mockito.mock(RESTClient.class);

    when(mockClient.withAuthSession(any())).thenReturn(mockClient);

    // Config response with table listing endpoint enabled
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withEndpoints(ImmutableList.of(Endpoint.V1_LIST_TABLES))
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ConfigResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(configResponse);

    // Table listing: always return the same page token "stuck"
    ListTablesResponse dupResponse =
        ListTablesResponse.builder()
            .add(TableIdentifier.of(Namespace.of("demo"), "t1"))
            .nextPageToken("stuck")
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ListTablesResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(dupResponse);

    RESTCatalog catalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(), (config) -> mockClient);
    catalog.initialize("test", ImmutableMap.of());

    assertThatThrownBy(() -> catalog.listTables(Namespace.of("demo")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicate page token");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListViewsThrowsOnDuplicatePageToken() {
    RESTClient mockClient = Mockito.mock(RESTClient.class);

    when(mockClient.withAuthSession(any())).thenReturn(mockClient);

    // Config response with view listing endpoint enabled
    ConfigResponse configResponse =
        ConfigResponse.builder()
            .withEndpoints(ImmutableList.of(Endpoint.V1_LIST_VIEWS))
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ConfigResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(configResponse);

    // View listing: always return the same page token "stuck"
    ListTablesResponse dupResponse =
        ListTablesResponse.builder()
            .add(TableIdentifier.of(Namespace.of("demo"), "v1"))
            .nextPageToken("stuck")
            .build();
    when(mockClient.get(
            anyString(),
            anyMap(),
            Mockito.eq(ListTablesResponse.class),
            anyMap(),
            any(Consumer.class)))
        .thenReturn(dupResponse);

    RESTCatalog catalog =
        new RESTCatalog(
            SessionCatalog.SessionContext.createEmpty(), (config) -> mockClient);
    catalog.initialize("test", ImmutableMap.of());

    assertThatThrownBy(() -> catalog.listViews(Namespace.of("demo")))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("duplicate page token");
  }
}