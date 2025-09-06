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

import java.util.List;

public class GetEventsRequest {

  private String nextPageToken;
  private Integer pageSize;
  private Long afterTimestampMs;
  private Long afterSequence;
  private List<OperationType> operationTypes;
  private List<String> users;
  private List<Actor> actors;
  private List<Object> catalogObjects; // NamespaceReference, TableReference, ViewReference
  private Object customFilters;

  // Getters and Setters
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

  public List<Object> getCatalogObjects() {
    return catalogObjects;
  }

  public void setCatalogObjects(List<Object> catalogObjects) {
    this.catalogObjects = catalogObjects;
  }

  public Object getCustomFilters() {
    return customFilters;
  }

  public void setCustomFilters(Object customFilters) {
    this.customFilters = customFilters;
  }
}
