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
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.ProxyConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;

class UrlConnectionHttpClientConfigurations extends BaseHttpClientConfigurations {

  private Long httpClientUrlConnectionConnectionTimeoutMs;
  private Long httpClientUrlConnectionSocketTimeoutMs;
  private String proxyEndpoint;

  private UrlConnectionHttpClientConfigurations() {}

  @Override
  protected SdkHttpClient buildHttpClient() {
    final UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder =
        UrlConnectionHttpClient.builder();
    configureUrlConnectionHttpClientBuilder(urlConnectionHttpClientBuilder);
    return urlConnectionHttpClientBuilder.build();
  }

  private void initialize(Map<String, String> httpClientProperties) {
    this.httpClientUrlConnectionConnectionTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.URLCONNECTION_CONNECTION_TIMEOUT_MS);
    this.httpClientUrlConnectionSocketTimeoutMs =
        PropertyUtil.propertyAsNullableLong(
            httpClientProperties, HttpClientProperties.URLCONNECTION_SOCKET_TIMEOUT_MS);
    this.proxyEndpoint =
        PropertyUtil.propertyAsString(
            httpClientProperties, HttpClientProperties.PROXY_ENDPOINT, null);
  }

  @VisibleForTesting
  void configureUrlConnectionHttpClientBuilder(
      UrlConnectionHttpClient.Builder urlConnectionHttpClientBuilder) {
    if (httpClientUrlConnectionConnectionTimeoutMs != null) {
      urlConnectionHttpClientBuilder.connectionTimeout(
          Duration.ofMillis(httpClientUrlConnectionConnectionTimeoutMs));
    }
    if (httpClientUrlConnectionSocketTimeoutMs != null) {
      urlConnectionHttpClientBuilder.socketTimeout(
          Duration.ofMillis(httpClientUrlConnectionSocketTimeoutMs));
    }
    if (proxyEndpoint != null) {
      urlConnectionHttpClientBuilder.proxyConfiguration(
          ProxyConfiguration.builder().endpoint(URI.create(proxyEndpoint)).build());
    }
  }

  /**
   * Generate a cache key based on HTTP client configuration. This ensures clients with identical
   * configurations share the same HTTP client instance.
   */
  @Override
  protected String generateHttpClientCacheKey() {
    Map<String, Object> keyComponents = Maps.newTreeMap(); // TreeMap for consistent ordering

    keyComponents.put("type", "urlconnection");
    keyComponents.put("connectionTimeoutMs", httpClientUrlConnectionConnectionTimeoutMs);
    keyComponents.put("socketTimeoutMs", httpClientUrlConnectionSocketTimeoutMs);
    keyComponents.put("proxyEndpoint", proxyEndpoint);

    return keyComponents.entrySet().stream()
        .map(entry -> entry.getKey() + "=" + Objects.toString(entry.getValue(), "null"))
        .collect(Collectors.joining(",", "urlconnection[", "]"));
  }

  public static UrlConnectionHttpClientConfigurations create(
      Map<String, String> httpClientProperties) {
    UrlConnectionHttpClientConfigurations configurations =
        new UrlConnectionHttpClientConfigurations();
    configurations.initialize(httpClientProperties);
    return configurations;
  }
}
