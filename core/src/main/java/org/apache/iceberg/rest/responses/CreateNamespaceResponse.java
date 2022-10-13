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

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTResponse;
import org.immutables.value.Value;

/** Represents a REST response for a request to create a namespace / database. */
@Value.Immutable
@Value.Style(builder = "newBuilder")
public interface CreateNamespaceResponse extends RESTResponse {

  Namespace namespace();

  @Value.Default
  default Map<String, String> properties() {
    return ImmutableMap.of();
  }

  @Override
  default void validate() {
    // nothing to do here as it's not possible to create an invalid CreateNamespaceResponse
  }

  /**
   * @deprecated Will be removed in 1.2.0, use {@link ImmutableCreateNamespaceResponse#newBuilder()}
   *     directly.
   */
  @Deprecated
  static CreateNamespaceResponse.Builder builder() {
    return new CreateNamespaceResponse.Builder();
  }

  class Builder {
    private final ImmutableCreateNamespaceResponse.Builder builder;

    private Builder() {
      builder = ImmutableCreateNamespaceResponse.newBuilder();
    }

    public CreateNamespaceResponse.Builder withNamespace(Namespace ns) {
      builder.namespace(ns);
      return this;
    }

    public CreateNamespaceResponse.Builder setProperties(Map<String, String> props) {
      builder.properties(props);
      return this;
    }

    public CreateNamespaceResponse build() {
      return builder.build();
    }
  }
}
