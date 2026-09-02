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
package org.apache.iceberg.aws;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;

record ProxyConfiguration(
    String scheme,
    String endpoint,
    String username,
    String password,
    Boolean useSystemPropertyValues,
    Boolean useEnvironmentVariableValues) {

  private static final String DEFAULT_SCHEME = "http";

  static ProxyConfiguration create(Map<String, String> properties) {
    return new ProxyConfiguration(
        PropertyUtil.propertyAsString(
            properties, HttpClientProperties.PROXY_SCHEME, DEFAULT_SCHEME),
        PropertyUtil.propertyAsString(properties, HttpClientProperties.PROXY_ENDPOINT, null),
        PropertyUtil.propertyAsString(properties, HttpClientProperties.PROXY_USERNAME, null),
        PropertyUtil.propertyAsString(properties, HttpClientProperties.PROXY_PASSWORD, null),
        PropertyUtil.propertyAsNullableBoolean(
            properties, HttpClientProperties.PROXY_USE_SYSTEM_PROPERTY_VALUES),
        PropertyUtil.propertyAsNullableBoolean(
            properties, HttpClientProperties.PROXY_USE_ENVIRONMENT_VARIABLE_VALUES));
  }

  boolean isConfigured() {
    return endpoint != null
        || useSystemPropertyValues != null
        || useEnvironmentVariableValues != null;
  }

  void addToCacheKey(Map<String, Object> keyComponents) {
    keyComponents.put("proxyScheme", scheme);
    keyComponents.put("proxyEndpoint", endpoint);
    keyComponents.put("proxyUsername", username);
    keyComponents.put("proxyPassword", password);
    keyComponents.put("proxyUseSystemPropertyValues", useSystemPropertyValues);
    keyComponents.put("proxyUseEnvironmentVariableValues", useEnvironmentVariableValues);
  }

  // for Apache client
  software.amazon.awssdk.http.apache.ProxyConfiguration toApache() {
    software.amazon.awssdk.http.apache.ProxyConfiguration.Builder builder =
        software.amazon.awssdk.http.apache.ProxyConfiguration.builder();
    if (endpoint != null) {
      builder.scheme(scheme).endpoint(URI.create(endpoint)).username(username).password(password);
    }
    if (useSystemPropertyValues != null) {
      builder.useSystemPropertyValues(useSystemPropertyValues);
    }
    if (useEnvironmentVariableValues != null) {
      builder.useEnvironmentVariableValues(useEnvironmentVariableValues);
    }
    return builder.build();
  }

  // build for url client
  software.amazon.awssdk.http.urlconnection.ProxyConfiguration toUrlConnection() {
    software.amazon.awssdk.http.urlconnection.ProxyConfiguration.Builder builder =
        software.amazon.awssdk.http.urlconnection.ProxyConfiguration.builder();
    if (endpoint != null) {
      builder.scheme(scheme).endpoint(URI.create(endpoint)).username(username).password(password);
    }
    if (useSystemPropertyValues != null) {
      builder.useSystemPropertyValues(useSystemPropertyValues);
    }
    if (useEnvironmentVariableValues != null) {
      builder.useEnvironmentVariablesValues(useEnvironmentVariableValues);
    }
    return builder.build();
  }
}
