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
package org.apache.iceberg.events.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Map;

public class QueryEventsRequest {

  @JsonProperty("next-page-token")
  private String nextPageToken;

  @JsonProperty("page-size")
  private Integer pageSize;

  @JsonProperty("after-timestamp-ms")
  private Long afterTimestampMs;

  @JsonProperty("after-sequence")
  private Long afterSequence;

  @JsonProperty("operation-types")
  private List<OperationType> operationTypes;

  @JsonProperty("users")
  private List<String> users;

  @JsonProperty("actors")
  private List<Actor> actors;

  @JsonProperty("catalog-objects")
  private List<CatalougObjectReference> catalogObjects;

  @JsonProperty("custom-filters")
  private Map<String, Object> customFilters;

  // Getters and setters
  public String getNextPageToken() {
    return nextPageToken;
  }

  public void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  public void setPageSize(Integer pageSize) {
    this.pageSize = pageSize;
  }

  public Long getAfterTimestampMs() {
    return afterTimestampMs;
  }

  public void setAfterTimestampMs(Long afterTimestampMs) {
    this.afterTimestampMs = afterTimestampMs;
  }

  public Long getAfterSequence() {
    return afterSequence;
  }

  public void setAfterSequence(Long afterSequence) {
    this.afterSequence = afterSequence;
  }

  public List<OperationType> getOperationTypes() {
    return operationTypes;
  }

  public void setOperationTypes(List<OperationType> operationTypes) {
    this.operationTypes = operationTypes;
  }

  public List<String> getUsers() {
    return users;
  }

  public void setUsers(List<String> users) {
    this.users = users;
  }

  public List<Actor> getActors() {
    return actors;
  }

  public void setActors(List<Actor> actors) {
    this.actors = actors;
  }

  public List<CatalougObjectReference> getCatalogObjects() {
    return catalogObjects;
  }

  public void setCatalogObjects(List<CatalougObjectReference> catalogObjects) {
    this.catalogObjects = catalogObjects;
  }

  public Map<String, Object> getCustomFilters() {
    return customFilters;
  }

  public void setCustomFilters(Map<String, Object> customFilters) {
    this.customFilters = customFilters;
  }
}
