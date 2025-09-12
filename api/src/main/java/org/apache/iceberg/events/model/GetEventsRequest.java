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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;

public class GetEventsRequest implements RESTRequest {

  private String nextPageToken;
  private Integer pageSize;
  private Long afterTimestampMs;
  private Long afterSequence;
  private List<OperationType> operationTypes;
  private List<String> users;
  private List<Actor> actors;
  private List<CatalogObject> catalogObjects;
  private Map<String, String> customFilters;

  public String getNextPageToken() {
    return nextPageToken;
  }

  public Integer getPageSize() {
    return pageSize;
  }

  public Long getAfterTimestampMs() {
    return afterTimestampMs;
  }

  public Long getAfterSequence() {
    return afterSequence;
  }

  public List<OperationType> getOperationTypes() {
    return operationTypes;
  }

  public List<String> getUsers() {
    return users;
  }

  public List<Actor> getActors() {
    return actors;
  }

  public List<CatalogObject> getCatalogObjects() {
    return catalogObjects;
  }

  public Map<String, String> getCustomFilters() {
    return customFilters;
  }

  @Override
  public void validate() {
    if (pageSize != null) {
      Preconditions.checkArgument(pageSize > 0, "Page size must be positive");
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("nextPageToken", nextPageToken)
        .add("pageSize", pageSize)
        .add("afterTimestampMs", afterTimestampMs)
        .add("afterSequence", afterSequence)
        .add("operationTypes", operationTypes)
        .add("users", users)
        .add("actors", actors)
        .add("catalogObjects", catalogObjects)
        .add("customFilters", customFilters)
        .toString();
  }
}
