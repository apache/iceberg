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
package org.apache.iceberg.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

class TestGlueCatalogIdInterceptor {

  private static final String CATALOG_ID = "testCatalogId";
  private static final ExecutionAttributes EMPTY_ATTRS = ExecutionAttributes.builder().build();

  @Test
  void getDatabaseRequest() {
    SdkRequest result = intercept(GetDatabaseRequest.builder().name("db").build());
    assertThat(((GetDatabaseRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void getDatabasesRequest() {
    SdkRequest result = intercept(GetDatabasesRequest.builder().build());
    assertThat(((GetDatabasesRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void createDatabaseRequest() {
    SdkRequest result = intercept(CreateDatabaseRequest.builder().build());
    assertThat(((CreateDatabaseRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void deleteDatabaseRequest() {
    SdkRequest result = intercept(DeleteDatabaseRequest.builder().name("db").build());
    assertThat(((DeleteDatabaseRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void updateDatabaseRequest() {
    SdkRequest result = intercept(UpdateDatabaseRequest.builder().name("db").build());
    assertThat(((UpdateDatabaseRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void getTableRequest() {
    SdkRequest result = intercept(GetTableRequest.builder().databaseName("db").name("t").build());
    assertThat(((GetTableRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void getTablesRequest() {
    SdkRequest result = intercept(GetTablesRequest.builder().databaseName("db").build());
    assertThat(((GetTablesRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void createTableRequest() {
    SdkRequest result = intercept(CreateTableRequest.builder().databaseName("db").build());
    assertThat(((CreateTableRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void updateTableRequest() {
    SdkRequest result = intercept(UpdateTableRequest.builder().databaseName("db").build());
    assertThat(((UpdateTableRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void deleteTableRequest() {
    SdkRequest result =
        intercept(DeleteTableRequest.builder().databaseName("db").name("t").build());
    assertThat(((DeleteTableRequest) result).catalogId()).isEqualTo(CATALOG_ID);
  }

  @Test
  void nonGlueRequestPassedThrough() {
    SdkRequest nonGlueRequest = mock(SdkRequest.class);
    GlueCatalogIdInterceptor interceptor = new GlueCatalogIdInterceptor(CATALOG_ID);
    Context.ModifyRequest ctx = mock(Context.ModifyRequest.class);
    when(ctx.request()).thenReturn(nonGlueRequest);
    assertThat(interceptor.modifyRequest(ctx, EMPTY_ATTRS)).isSameAs(nonGlueRequest);
  }

  @Test
  void invalidCatalogIdRejected() {
    assertThatThrownBy(() -> new GlueCatalogIdInterceptor(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid catalog id");

    assertThatThrownBy(() -> new GlueCatalogIdInterceptor(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid catalog id");
  }

  private static SdkRequest intercept(SdkRequest request) {
    GlueCatalogIdInterceptor interceptor = new GlueCatalogIdInterceptor(CATALOG_ID);
    Context.ModifyRequest context = mock(Context.ModifyRequest.class);
    when(context.request()).thenReturn(request);
    return interceptor.modifyRequest(context, EMPTY_ATTRS);
  }
}
