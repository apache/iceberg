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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

public class TestRESTCatalog {

  private static final String CATALOG_NAME = "rest";

  private RESTClient restClient;
  private RESTCatalog restCatalog;

  @Before
  public void before() {
    restClient = Mockito.mock(RESTClient.class);
    restCatalog = new RESTCatalog();
    restCatalog.initialize(CATALOG_NAME, restClient, null /* io */);
  }

  @Test
  @Ignore
  public void listTablesV1() {
    Namespace namespace = Namespace.of("hank");
    List<TableIdentifier> tables = ImmutableList.of(TableIdentifier.of(namespace, "foo"));
    ListTablesResponse resp = ListTablesResponse.builder()
        .addAll(tables).build();

    String path = "namespaces/" + namespace + "/tables";
    Mockito.when(restClient.get(eq(path), any())).thenReturn(resp);

    ListTablesResponse listMock = restClient.get(path, ListTablesResponse.class);
    verify(restClient).get(eq(path), any());
    Assert.assertTrue("Mocked get call should return the correct list tables response",
        listMock.identifiers().containsAll(resp.identifiers()));
  }

  @Test
  public void testListTablesV2() {
    Namespace ns = Namespace.of("db1");
    List<TableIdentifier> tables = ImmutableList.of("t1", "t2", "t3")
        .stream()
        .map(tbl -> TableIdentifier.of(ns, tbl))
        .collect(Collectors.toList());
    String path = "namespaces/" + RESTUtil.asURLVariable(ns) + "/tables";
    Mockito.doReturn(
        ListTablesResponse
            .builder()
            .addAll(tables)
            .build())
        .when(restClient).get(Mockito.eq(path), Mockito.eq(ListTablesResponse.class), Mockito.any());
    Assert.assertEquals(
        Sets.newHashSet(tables),
        Sets.newHashSet(restCatalog.listTables(ns))
    );
  }

  @Test
  @Ignore
  public void dropTable() {
  }

  @Test
  @Ignore
  public void renameTable() {
  }

  @Test
  @Ignore
  public void loadTable() {
  }

  @Test
  public void testCreateNamespace200() {
    Namespace ns = Namespace.of("namespace");
    Map<String, String> props = ImmutableMap.of("owner", "Hank");
    String path = "namespaces";
    CreateNamespaceRequest req = CreateNamespaceRequest.builder()
        .withNamespace(ns).setProperties(props).build();
    Consumer<ErrorResponse> errorHandler = ErrorHandlers.namespaceErrorHandler();

    Mockito
        .doReturn(
            CreateNamespaceResponse
                .builder()
                .withNamespace(ns)
                .setProperties(props)
                .build())
        .when(restClient)
        .post(
            Mockito.eq(path),
            Mockito.eq(req),
            Mockito.eq(CreateNamespaceRequest.class),
            Mockito.same(errorHandler));

    Consumer<ErrorResponse> spyErrorHandler = Mockito.spy(errorHandler);
    Mockito.spy(restCatalog).createNamespace(ns, props);
    Mockito.verifyNoInteractions(spyErrorHandler);
  }

  @Test
  @Ignore
  public void listNamespaces() {
  }

  @Test
  @Ignore
  public void loadNamespaceMetadata() {
  }

  @Test
  @Ignore
  public void dropNamespace() {
  }

  @Test
  @Ignore
  public void removeProperties() {
  }
}
