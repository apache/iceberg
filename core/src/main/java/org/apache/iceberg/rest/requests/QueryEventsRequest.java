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
package org.apache.iceberg.rest.requests;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.CatalogObject;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.CatalogObjectUuid;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.rest.operations.OperationType;

/** Standard request body for querying events. */
public class QueryEventsRequest {
  private final String pageToken;
  private final Integer pageSize;
  private final Long afterTimestampMs;
  private final List<OperationType> operationTypes;
  private final List<CatalogObject> catalogObjectsByName;
  private final List<CatalogObjectUuid> catalogObjectsById;
  private final List<CatalogObjectType> objectTypes;
  private final Map<String, String> customFilters;

  public QueryEventsRequest(
      String pageToken,
      Integer pageSize,
      Long afterTimestampMs,
      List<OperationType> operationTypes,
      List<CatalogObject> catalogObjectsByName,
      List<CatalogObjectUuid> catalogObjectsById,
      List<CatalogObjectType> objectTypes,
      Map<String, String> customFilters) {
    this.pageToken = pageToken;
    this.pageSize = pageSize;
    this.afterTimestampMs = afterTimestampMs;
    this.operationTypes = operationTypes;
    this.catalogObjectsByName = catalogObjectsByName;
    this.catalogObjectsById = catalogObjectsById;
    this.objectTypes = objectTypes;
    this.customFilters = customFilters;
  }

  public QueryEventsRequest() {
    this(null, null, null, null, null, null, null, null);
  }

  public String pageToken() {
    return pageToken;
  }

  public Integer pageSize() {
    return pageSize;
  }

  public Long afterTimestampMs() {
    return afterTimestampMs;
  }

  public List<OperationType> operationTypes() {
    return operationTypes;
  }

  public List<CatalogObject> catalogObjectsByName() {
    return catalogObjectsByName;
  }

  public List<CatalogObjectUuid> catalogObjectsById() {
    return catalogObjectsById;
  }

  public List<CatalogObjectType> objectTypes() {
    return objectTypes;
  }

  public Map<String, String> customFilters() {
    return customFilters;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("pageToken", pageToken)
        .add("pageSize", pageSize)
        .add("afterTimestampMs", afterTimestampMs)
        .add("operationTypes", operationTypes)
        .add("catalogObjectsByName", catalogObjectsByName)
        .add("catalogObjectsById", catalogObjectsById)
        .add("objectTypes", objectTypes)
        .add("customFilters", customFilters)
        .toString();
  }
}
