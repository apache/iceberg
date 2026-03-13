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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.Credential;

/**
 * A REST response that is used when a table is successfully loaded.
 *
 * <p>This class is used whenever the response to a request is a table's requested metadata and the
 * associated location of its metadata, to reduce code duplication. This includes using this class
 * as the response for {@link org.apache.iceberg.rest.requests.CreateTableRequest}, including when
 * that request is used to commit an already staged table creation as part of a transaction.
 */
public class LoadTableResponse implements RESTResponse {

  private String metadataLocation;
  private TableMetadata metadata;
  private Map<String, String> config;
  private TableMetadata metadataWithLocation;
  private List<Credential> credentials;

  public LoadTableResponse() {
    // Required for Jackson deserialization
  }

  private LoadTableResponse(
      String metadataLocation,
      TableMetadata metadata,
      Map<String, String> config,
      List<Credential> credentials) {
    this.metadataLocation = metadataLocation;
    this.metadata = metadata;
    this.config = config;
    this.credentials = credentials;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(metadata, "Invalid metadata: null");
  }

  public String metadataLocation() {
    return metadataLocation;
  }

  public TableMetadata tableMetadata() {
    if (null == metadataWithLocation) {
      this.metadataWithLocation =
          TableMetadata.buildFrom(metadata).withMetadataLocation(metadataLocation).build();
    }

    return metadataWithLocation;
  }

  public Map<String, String> config() {
    return config != null ? config : ImmutableMap.of();
  }

  public List<Credential> credentials() {
    return credentials != null ? credentials : ImmutableList.of();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("metadataLocation", metadataLocation)
        .add("metadata", metadata)
        .add("config", config)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String metadataLocation;
    private TableMetadata metadata;
    private final Map<String, String> config = Maps.newHashMap();
    private final List<Credential> credentials = Lists.newArrayList();

    private Builder() {}

    public Builder withTableMetadata(TableMetadata tableMetadata) {
      this.metadataLocation = tableMetadata.metadataFileLocation();
      this.metadata = tableMetadata;
      return this;
    }

    public Builder addConfig(String property, String value) {
      config.put(property, value);
      return this;
    }

    public Builder addAllConfig(Map<String, String> properties) {
      config.putAll(properties);
      return this;
    }

    public Builder addCredential(Credential credential) {
      credentials.add(credential);
      return this;
    }

    public Builder addAllCredentials(List<Credential> credentialsToAdd) {
      credentials.addAll(credentialsToAdd);
      return this;
    }

    public LoadTableResponse build() {
      Preconditions.checkNotNull(metadata, "Invalid metadata: null");
      return new LoadTableResponse(metadataLocation, metadata, config, credentials);
    }
  }
}
