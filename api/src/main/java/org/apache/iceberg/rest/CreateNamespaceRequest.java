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

import java.io.Serializable;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Represents a REST request to create a namespace / database.
 */
public class CreateNamespaceRequest implements Serializable {

  // TODO - Use protected so users can extend this for their own impls. Or an interface.
  //        Currently anything but private causes an error.
  private String namespaceName;
  private Map<String, String> properties;

  private CreateNamespaceRequest() {

  }

  private CreateNamespaceRequest(String namespaceName, Map<String, String> properties) {
    this.namespaceName = namespaceName;
    this.properties = properties;
  }

  /**
   * Name of the database to create.
   */
  String getNamespaceName() {
    return namespaceName;
  }

  void setNamespaceName(String name) {
    this.namespaceName = name;
  }

  Map<String, String> getProperties() {
    return ImmutableMap.copyOf(properties);
  }

  void setProperties(Map<String, String>  properties) {
    this.properties = properties;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String namespaceName;
    private final ImmutableMap.Builder<String, String> propertiesBuilder;

    public Builder() {
      this.propertiesBuilder = ImmutableMap.builder();
    }

    public Builder withNamespaceName(String name) {
      this.namespaceName = name;
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
      Preconditions.checkNotNull(namespaceName, "Cannot build CreateNamespaceRequest with a null namespaceName");
      return new CreateNamespaceRequest(namespaceName, propertiesBuilder.build());

    }
  }
}
