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
package org.apache.iceberg.rest.responses;

import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;

/**
 * Discriminated response for the universal relation load endpoint. The {@link #objectType()} field
 * indicates whether the loaded relation is a table or a view, and exactly one of {@link
 * #tableResponse()} or {@link #viewResponse()} is populated.
 */
public class LoadRelationResponse implements RESTResponse {

  private CatalogObjectType objectType;
  private LoadTableResponse tableResponse;
  private LoadViewResponse viewResponse;

  public LoadRelationResponse() {
    // Required for Jackson deserialization
  }

  private LoadRelationResponse(
      CatalogObjectType objectType,
      LoadTableResponse tableResponse,
      LoadViewResponse viewResponse) {
    this.objectType = objectType;
    this.tableResponse = tableResponse;
    this.viewResponse = viewResponse;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(objectType, "Invalid object-type: null");
    switch (objectType) {
      case TABLE:
        Preconditions.checkArgument(
            tableResponse != null, "Invalid table response: null for object-type table");
        break;
      case VIEW:
        Preconditions.checkArgument(
            viewResponse != null, "Invalid view response: null for object-type view");
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported object-type: %s", objectType));
    }
  }

  public CatalogObjectType objectType() {
    return objectType;
  }

  public LoadTableResponse tableResponse() {
    return tableResponse;
  }

  public LoadViewResponse viewResponse() {
    return viewResponse;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("objectType", objectType)
        .add("tableResponse", tableResponse)
        .add("viewResponse", viewResponse)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private CatalogObjectType objectType;
    private LoadTableResponse tableResponse;
    private LoadViewResponse viewResponse;

    private Builder() {}

    public Builder withObjectType(CatalogObjectType type) {
      this.objectType = type;
      return this;
    }

    public Builder withTableResponse(LoadTableResponse table) {
      this.objectType = CatalogObjectType.TABLE;
      this.tableResponse = table;
      return this;
    }

    public Builder withViewResponse(LoadViewResponse view) {
      this.objectType = CatalogObjectType.VIEW;
      this.viewResponse = view;
      return this;
    }

    public LoadRelationResponse build() {
      LoadRelationResponse response =
          new LoadRelationResponse(objectType, tableResponse, viewResponse);
      response.validate();
      return response;
    }
  }
}
