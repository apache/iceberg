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

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Represents a REST request to create a namespace / database.
 *
 * Possible example POST body (outer format object possibly only used on Response):
 *   { "data": { "namespace": ["prod", "accounting"], properties: {} }}
 */
public class CreateNamespaceRequest {

  private Namespace namespace;
  private Map<String, String> properties;

  private CreateNamespaceRequest() {
  }

  private CreateNamespaceRequest(String[] namespaceLevels, Map<String, String> properties) {
    this.namespace = Namespace.of(namespaceLevels);
    this.properties = properties;
  }

  public Namespace getNamespace() {
    return namespace;
  }

  public void setNamespace(Namespace namespace) {
    this.namespace = namespace;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private ImmutableList.Builder<String> namespaceBuilder;
    private final ImmutableMap.Builder<String, String> propertiesBuilder;

    public Builder() {
      this.namespaceBuilder = ImmutableList.builder();
      this.propertiesBuilder = ImmutableMap.builder();
    }

    public Builder withNamespace(Namespace namespace) {
      this.namespaceBuilder.add(namespace.levels());
      return this;
    }

    public Builder withProperties(Map<String, String> properties) {
      if (properties != null) {
        propertiesBuilder.putAll(properties);
      }
      return this;
    }

    public Builder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    public CreateNamespaceRequest build() {
      String[] namespaceLevels = namespaceBuilder.build().toArray(new String[0]);
      Preconditions.checkState(namespaceLevels.length > 0,
          "Cannot create a CreateNamespaceRequest with an empty namespace.");
      return new CreateNamespaceRequest(namespaceLevels, propertiesBuilder.build());

    }
  }
}
