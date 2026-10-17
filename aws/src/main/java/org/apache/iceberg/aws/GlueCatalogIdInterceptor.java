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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.interceptor.Context.ModifyRequest;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GlueRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

class GlueCatalogIdInterceptor implements ExecutionInterceptor {

  private final String catalogId;

  GlueCatalogIdInterceptor(String catalogId) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(catalogId), "Invalid catalog id: null or empty");
    this.catalogId = catalogId;
  }

  @Override
  public SdkRequest modifyRequest(ModifyRequest context, ExecutionAttributes executionAttributes) {
    SdkRequest request = context.request();
    if (!(request instanceof GlueRequest)) {
      return request;
    }

    if (request instanceof GetDatabaseRequest) {
      return ((GetDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof GetDatabasesRequest) {
      return ((GetDatabasesRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof CreateDatabaseRequest) {
      return ((CreateDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof DeleteDatabaseRequest) {
      return ((DeleteDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof UpdateDatabaseRequest) {
      return ((UpdateDatabaseRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof GetTableRequest) {
      return ((GetTableRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof GetTablesRequest) {
      return ((GetTablesRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof CreateTableRequest) {
      return ((CreateTableRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof UpdateTableRequest) {
      return ((UpdateTableRequest) request).toBuilder().catalogId(catalogId).build();
    } else if (request instanceof DeleteTableRequest) {
      return ((DeleteTableRequest) request).toBuilder().catalogId(catalogId).build();
    } else {
      throw new IllegalArgumentException("Unexpected request: " + request.getClass().getName());
    }
  }
}
