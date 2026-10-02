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
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.events.CatalogObjectType;
import org.immutables.value.Value;

/** REST request body for querying catalog events. */
@Value.Immutable
public interface QueryEventsRequest extends RESTRequest {
  @Nullable
  String continuationToken();

  @Nullable
  Integer pageSize();

  @Nullable
  Long sinceTimestampMs();

  @Nullable
  List<String> operationTypes();

  @Nullable
  List<CatalogObjectIdentifier> catalogObjectsByName();

  @Nullable
  List<String> catalogObjectsByUuid();

  @Nullable
  List<CatalogObjectType> objectTypes();

  @Nullable
  Map<String, String> customFilters();

  @Override
  default void validate() {
    Integer pageSize = pageSize();
    if (pageSize != null) {
      Preconditions.checkArgument(pageSize >= 1, "Invalid page-size: %s (must be >= 1)", pageSize);
    }
  }

  @Value.Check
  default void check() {
    validate();
  }
}
