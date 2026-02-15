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
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.iceberg.index.IndexMetadata;
import org.apache.iceberg.index.IndexOperations;
import org.apache.iceberg.index.IndexRequirement;
import org.apache.iceberg.index.IndexUpdate;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.UpdateIndexRequest;
import org.apache.iceberg.rest.responses.LoadIndexResponse;

/**
 * REST implementation of {@link IndexOperations}.
 *
 * <p>This class communicates with a REST catalog server to perform index metadata operations such
 * as loading, refreshing, and committing updates.
 */
class RESTIndexOperations implements IndexOperations {
  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> readHeaders;
  private final Supplier<Map<String, String>> mutationHeaders;
  private final Set<Endpoint> endpoints;
  private IndexMetadata current;

  RESTIndexOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      IndexMetadata current,
      Set<Endpoint> endpoints) {
    this(client, path, headers, headers, current, endpoints);
  }

  RESTIndexOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaders,
      IndexMetadata current,
      Set<Endpoint> endpoints) {
    Preconditions.checkArgument(null != current, "Invalid index metadata: null");
    this.client = client;
    this.path = path;
    this.readHeaders = readHeaders;
    this.mutationHeaders = mutationHeaders;
    this.current = current;
    this.endpoints = endpoints;
  }

  @Override
  public IndexMetadata current() {
    return current;
  }

  @Override
  public IndexMetadata refresh() {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_INDEX);
    return updateCurrentMetadata(
        client.get(path, LoadIndexResponse.class, readHeaders, ErrorHandlers.indexErrorHandler()));
  }

  @Override
  public void commit(IndexMetadata base, IndexMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_INDEX);
    // this is only used for updating index metadata
    Preconditions.checkState(base != null, "Invalid base metadata: null");

    // Get updates from the metadata changes
    List<IndexUpdate> updates = metadata.changes();

    // Skip the commit if there are no changes
    if (updates.isEmpty()) {
      return;
    }

    // Build requirements based on the base metadata
    List<IndexRequirement> requirements =
        ImmutableList.of(new IndexRequirement.AssertIndexUUID(base.uuid()));

    UpdateIndexRequest request = UpdateIndexRequest.create(null, requirements, updates);

    LoadIndexResponse response =
        client.post(
            path,
            request,
            LoadIndexResponse.class,
            mutationHeaders,
            ErrorHandlers.indexCommitHandler());

    updateCurrentMetadata(response);
  }

  private IndexMetadata updateCurrentMetadata(LoadIndexResponse response) {
    if (!Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = response.metadata();
    }

    return current;
  }
}
