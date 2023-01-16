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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTResponse;

/**
 * Represents a response to requesting server-side provided configuration for the REST catalog. This
 * allows client provided values to be overridden by the server or defaulted if not provided by the
 * client.
 *
 * <p>The catalog properties, with overrides and defaults applied, should be used to configure the
 * catalog and for all subsequent requests after this initial config request.
 *
 * <p>Configuration from the server consists of two sets of key/value pairs.
 *
 * <ul>
 *   <li>defaults - properties that should be used as default configuration
 *   <li>overrides - properties that should be used to override client configuration
 * </ul>
 */
public class ConfigResponse implements RESTResponse {

  private Map<String, String> defaults;
  private Map<String, String> overrides;

  public ConfigResponse() {
    // Required for Jackson deserialization
  }

  private ConfigResponse(Map<String, String> defaults, Map<String, String> overrides) {
    this.defaults = defaults;
    this.overrides = overrides;
    validate();
  }

  @Override
  public void validate() {}

  /**
   * Properties that should be used as default configuration. {@code defaults} have the lowest
   * priority and should be applied before the client provided configuration.
   *
   * @return properties that should be used as default configuration
   */
  public Map<String, String> defaults() {
    return defaults != null ? defaults : ImmutableMap.of();
  }

  /**
   * Properties that should be used to override client configuration. {@code overrides} have the
   * highest priority and should be applied after defaults and any client-provided configuration
   * properties.
   *
   * @return properties that should be given higher precedence than any client provided input
   */
  public Map<String, String> overrides() {
    return overrides != null ? overrides : ImmutableMap.of();
  }

  /**
   * Merge client-provided config with server side provided configuration to return a single
   * properties map which will be used for instantiating and configuring the REST catalog.
   *
   * @param clientProperties - Client provided configuration
   * @return Merged configuration, with precedence in the order overrides, then client properties,
   *     and then defaults.
   */
  public Map<String, String> merge(Map<String, String> clientProperties) {
    Preconditions.checkNotNull(
        clientProperties,
        "Cannot merge client properties with server-provided properties. Invalid client configuration: null");
    Map<String, String> merged = defaults != null ? Maps.newHashMap(defaults) : Maps.newHashMap();
    merged.putAll(clientProperties);

    if (overrides != null) {
      merged.putAll(overrides);
    }

    return ImmutableMap.copyOf(Maps.filterValues(merged, Objects::nonNull));
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("defaults", defaults)
        .add("overrides", overrides)
        .toString();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private final Map<String, String> defaults;
    private final Map<String, String> overrides;

    private Builder() {
      this.defaults = Maps.newHashMap();
      this.overrides = Maps.newHashMap();
    }

    public Builder withDefault(String key, String value) {
      Preconditions.checkNotNull(key, "Invalid default property: null");
      defaults.put(key, value);
      return this;
    }

    public Builder withOverride(String key, String value) {
      Preconditions.checkNotNull(key, "Invalid override property: null");
      overrides.put(key, value);
      return this;
    }

    /** Adds the passed in map entries to the existing `defaults` of this Builder. */
    public Builder withDefaults(Map<String, String> defaultsToAdd) {
      Preconditions.checkNotNull(defaultsToAdd, "Invalid default properties map: null");
      Preconditions.checkArgument(
          !defaultsToAdd.containsKey(null), "Invalid default property: null");
      defaults.putAll(defaultsToAdd);
      return this;
    }

    /** Adds the passed in map entries to the existing `overrides` of this Builder. */
    public Builder withOverrides(Map<String, String> overridesToAdd) {
      Preconditions.checkNotNull(overridesToAdd, "Invalid override properties map: null");
      Preconditions.checkArgument(
          !overridesToAdd.containsKey(null), "Invalid override property: null");
      overrides.putAll(overridesToAdd);
      return this;
    }

    public ConfigResponse build() {
      return new ConfigResponse(defaults, overrides);
    }
  }
}
