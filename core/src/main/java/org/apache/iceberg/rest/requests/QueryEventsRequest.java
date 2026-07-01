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
import javax.annotation.Nullable;
import org.apache.iceberg.rest.RESTRequest;
import org.immutables.value.Value;

/**
 * A request to query catalog change events.
 *
 * <p>All fields are optional and serve as filters. If no filters are provided, all events are
 * returned.
 */
@Value.Immutable
public interface QueryEventsRequest extends RESTRequest {

  /** Continuation token to resume fetching events from a previous request. */
  @Nullable
  String continuationToken();

  /** Maximum number of events to return in a single response. */
  @Nullable
  Integer pageSize();

  /** Timestamp in milliseconds to start consuming events from (inclusive). */
  @Nullable
  Long afterTimestampMs();

  /** Filter events by the type of operation. */
  @Nullable
  List<String> operationTypes();

  /** Filter events by catalog objects referenced by name (namespaces, tables, views). */
  @Nullable
  List<List<String>> catalogObjectsByName();

  /** Filter events by catalog objects referenced by UUID. */
  @Nullable
  List<CatalogObjectUuid> catalogObjectsById();

  /** Filter events by the type of catalog object (namespace, table, view). */
  @Nullable
  List<String> objectTypes();

  /** Implementation-specific filter extensions. */
  @Nullable
  Map<String, Object> customFilters();

  @Override
  default void validate() {}

  /** Identifies a catalog object by UUID and type. */
  @Value.Immutable
  interface CatalogObjectUuid {
    String uuid();

    String type();
  }
}
