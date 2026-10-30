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

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.ExecutableHttpRequest;
import software.amazon.awssdk.http.HttpExecuteRequest;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * A cache that manages the lifecycle of shared HTTP clients for AWS SDK v2 using reference
 * counting. Package-private - only accessed via {@link BaseHttpClientConfigurations}.
 */
final class HttpClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(HttpClientCache.class);

  private final ConcurrentMap<String, ManagedHttpClient> clients = Maps.newConcurrentMap();
  private static volatile HttpClientCache instance;

  static HttpClientCache instance() {
    if (instance == null) {
      synchronized (HttpClientCache.class) {
        if (instance == null) {
          instance = new HttpClientCache();
        }
      }
    }
    return instance;
  }

  /**
   * Get or create a managed HTTP client for the given configuration. Each call increments the
   * reference count for the client and returns a ref-counted wrapper.
   *
   * @param clientKey unique key identifying the client configuration
   * @param clientFactory factory to create the HTTP client if not cached
   * @return a ref-counted HTTP client wrapper
   */
  SdkHttpClient getOrCreateClient(String clientKey, Supplier<SdkHttpClient> clientFactory) {
    ManagedHttpClient managedClient =
        clients.computeIfAbsent(
            clientKey,
            key -> {
              LOG.debug("Creating new managed HTTP client for key: {}", key);
              SdkHttpClient httpClient = clientFactory.get();
              return new ManagedHttpClient(httpClient, key);
            });
    // Return the cached ref-counted wrapper
    return managedClient.acquire();
  }

  /**
   * Release a reference to the HTTP client. When the reference count reaches zero, the client is
   * closed and removed from the cache.
   *
   * @param clientKey the key identifying the client to release
   */
  void releaseClient(String clientKey) {
    ManagedHttpClient managedClient = clients.get(clientKey);
    if (null != managedClient && managedClient.release()) {
      clients.remove(clientKey, managedClient);
    }
  }

  @VisibleForTesting
  Map<String, ManagedHttpClient> clients() {
    return Collections.unmodifiableMap(clients);
  }

  @VisibleForTesting
  void clear() {
    clients.values().forEach(ManagedHttpClient::close);
    clients.clear();
  }

  /**
   * Managed HTTP client wrapper that provides reference counting for lifecycle management. The HTTP
   * client is closed when the reference count reaches zero.
   */
  static class ManagedHttpClient implements SdkHttpClient {
    private final SdkHttpClient httpClient;
    private final String clientKey;
    private volatile int refCount = 0;
    private boolean closed = false;

    ManagedHttpClient(SdkHttpClient httpClient, String clientKey) {
      this.httpClient = httpClient;
      this.clientKey = clientKey;
      LOG.debug("Created managed HTTP client: key={}", clientKey);
    }

    /**
     * Acquire a reference to the HTTP client, incrementing the reference count.
     *
     * @return the ref-counted wrapper client
     * @throws IllegalStateException if the client has already been closed
     */
    synchronized ManagedHttpClient acquire() {
      if (closed) {
        throw new IllegalStateException("Cannot acquire closed HTTP client: " + clientKey);
      }
      refCount++;
      LOG.debug("Acquired HTTP client: key={}, refCount={}", clientKey, refCount);
      return this;
    }

    /**
     * Release a reference to the HTTP client, decrementing the reference count. If the count
     * reaches zero, the client is closed.
     *
     * @return true if the client was closed, false otherwise
     */
    synchronized boolean release() {
      if (closed) {
        LOG.warn("Attempted to release already closed HTTP client: key={}", clientKey);
        return false;
      }

      refCount--;
      LOG.debug("Released HTTP client: key={}, refCount={}", clientKey, refCount);
      if (refCount == 0) {
        return closeHttpClient();
      } else if (refCount < 0) {
        LOG.warn(
            "HTTP client reference count went negative key={}, refCount={}", clientKey, refCount);
        refCount = 0;
      }
      return false;
    }

    @VisibleForTesting
    SdkHttpClient httpClient() {
      return httpClient;
    }

    /**
     * Close the HTTP client if not already closed.
     *
     * @return true if the client was closed by this call, false if already closed
     */
    private boolean closeHttpClient() {
      if (!closed) {
        closed = true;
        LOG.debug("Closing HTTP client: key={}", clientKey);
        try {
          httpClient.close();
        } catch (Exception e) {
          LOG.error("Failed to close HTTP client: key={}", clientKey, e);
        }
        return true;
      }
      return false;
    }

    @VisibleForTesting
    int refCount() {
      return refCount;
    }

    @VisibleForTesting
    boolean isClosed() {
      return closed;
    }

    @Override
    public ExecutableHttpRequest prepareRequest(HttpExecuteRequest request) {
      return httpClient.prepareRequest(request);
    }

    @Override
    public String clientName() {
      return httpClient.clientName();
    }

    @Override
    public void close() {
      HttpClientCache.instance().releaseClient(clientKey);
    }
  }
}
