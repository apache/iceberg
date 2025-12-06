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
import org.apache.iceberg.catalog.CatalogObject;
import org.apache.iceberg.catalog.CatalogObjectType;
import org.apache.iceberg.catalog.CatalogObjectUuid;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.events.operations.OperationType;
import org.immutables.value.Value;

/** Standard request body for querying events. */
@Value.Immutable
public interface QueryEventsRequest extends RESTRequest {
  @Nullable
  String pageToken();

  @Nullable
  Integer pageSize();

  @Nullable
  Long afterTimestampMs();

  List<OperationType> operationTypes();

  List<CatalogObject> catalogObjectsByName();

  List<CatalogObjectUuid> catalogObjectsById();

  List<CatalogObjectType> objectTypes();

  Map<String, String> customFilters();

  @Override
  default void validate() {
    // nothing to validate as it's not possible to create an invalid instance
  }
}
