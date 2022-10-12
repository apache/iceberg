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

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTRequest;

/**
 * A REST request to create a namespace, with an optional set of properties.
 *
 * @deprecated Will be removed in 1.2.0 - Use {@link NamespaceCreateRequest}
 */
@Deprecated
public class CreateNamespaceRequest implements RESTRequest {

  private Namespace namespace;
  private Map<String, String> properties;

  public CreateNamespaceRequest() {
    // Needed for Jackson Deserialization.
  }

  private CreateNamespaceRequest(Namespace namespace, Map<String, String> properties) {
    this.namespace = namespace;
    this.properties = properties;
    validate();
  }

  @Override
  public void validate() {
    Preconditions.checkArgument(namespace != null, "Invalid namespace: null");
  }

  public Namespace namespace() {
    return namespace;
  }

  public Map<String, String> properties() {
    return properties != null ? properties : ImmutableMap.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("namespace", namespace)
        .add("properties", properties)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final ImmutableNamespaceCreateRequest.Builder builder;

    private Builder() {
      builder = ImmutableNamespaceCreateRequest.builder();
    }

    public Builder withNamespace(Namespace ns) {
      builder.namespace(ns);
      return this;
    }

    public Builder setProperties(Map<String, String> props) {
      builder.properties(props);
      return this;
    }

    public CreateNamespaceRequest build() {
      ImmutableNamespaceCreateRequest build = builder.build();
      return new CreateNamespaceRequest(build.namespace(), build.properties());
    }
  }
}
