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

import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * Base class for HTTP client configurations that provides managed HTTP client lifecycle with
 * reference counting.
 *
 * <p>This class encapsulates the interaction with {@link HttpClientCache} to ensure HTTP clients
 * are properly shared and their lifecycle managed via reference counting. Subclasses are
 * responsible for providing configuration-specific cache keys and building the appropriate HTTP
 * client type (Apache, UrlConnection, etc.).
 */
abstract class BaseHttpClientConfigurations {

  private static final HttpClientCache CACHE = HttpClientCache.instance();

  /**
   * Generate a unique cache key based on the HTTP client configuration. The cache key is used to
   * determine whether HTTP clients can be shared across different factory instances.
   *
   * <p>Implementations should include all configuration parameters that affect HTTP client behavior
   * (timeouts, connection settings, proxy configuration, etc.) to ensure clients are only shared
   * when they have identical configurations.
   *
   * @return a unique string representing this HTTP client configuration
   */
  protected abstract String generateHttpClientCacheKey();

  /**
   * Build the actual HTTP client instance based on the configuration. This method is called only
   * when a new HTTP client needs to be created (i.e., when no cached client exists for the given
   * cache key).
   *
   * @return a configured {@link SdkHttpClient} instance
   */
  protected abstract SdkHttpClient buildHttpClient();

  /**
   * Configure the AWS client builder with a managed HTTP client.
   *
   * <p>This method obtains a managed HTTP client from the cache using the configuration-specific
   * cache key. If a client with the same configuration already exists in the cache, it will be
   * reused with an incremented reference count. Otherwise, a new client will be built and cached.
   *
   * @param awsClientBuilder the AWS client builder to configure
   * @param <T> the type of AWS client builder
   */
  public <T extends AwsSyncClientBuilder> void configureHttpClientBuilder(T awsClientBuilder) {
    String cacheKey = generateHttpClientCacheKey();

    SdkHttpClient managedHttpClient = CACHE.getOrCreateClient(cacheKey, this::buildHttpClient);

    awsClientBuilder.httpClient(managedHttpClient);
  }
}
