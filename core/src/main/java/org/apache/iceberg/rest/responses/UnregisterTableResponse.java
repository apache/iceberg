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

import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTResponse;
import org.immutables.value.Value;

/**
 * Represents the response when a table is successfully unregistered from a catalog.
 *
 * <p>The response carries the table's last metadata location and the corresponding table metadata
 * so that the underlying (still present) files can be registered in another catalog.
 */
@Value.Immutable
public interface UnregisterTableResponse extends RESTResponse {
  String metadataLocation();

  TableMetadata metadata();

  @Override
  default void validate() {
    Preconditions.checkArgument(metadataLocation() != null, "Invalid metadata location: null");
    Preconditions.checkArgument(metadata() != null, "Invalid metadata: null");
  }
}
