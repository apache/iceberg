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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * A registry that manages the lifecycle of shared HTTP clients for AWS SDK v2 using reference
 * counting. HTTP clients are closed when their reference count reaches zero.
 */
class ManagedHttpClientRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedHttpClientRegistry.class);

  private final ConcurrentHashMap<String, ManagedHttpClient> clientMap;

  private static volatile ManagedHttpClientRegistry instance;

  static ManagedHttpClientRegistry getInstance() {
    if (instance == null) {
      synchronized (ManagedHttpClientRegistry.class) {
        if (instance == null) {
          instance = new ManagedHttpClientRegistry();
        }
      }
    }
    return instance;
  }

  private ManagedHttpClientRegistry() {
    this.clientMap = new ConcurrentHashMap<>();
  }

  /**
   * Get or create a managed HTTP client for the given configuration. Each call increments the
   * reference count for the client.
   *
   * @param clientKey unique key identifying the client configuration
   * @param clientFactory factory to create the HTTP client if not cached
   * @param properties configuration properties for this client
   * @return a managed HTTP client with incremented reference count
   */
  SdkHttpClient getOrCreateClient(
      String clientKey, Supplier<SdkHttpClient> clientFactory, Map<String, String> properties) {
    ManagedHttpClient managedClient =
        clientMap.computeIfAbsent(
            clientKey,
            k -> {
              LOG.debug("Creating new managed HTTP client for key: {}", k);
              SdkHttpClient httpClient = clientFactory.get();
              return new ManagedHttpClient(httpClient, k);
            });
    return managedClient.acquire();
  }

  /**
   * Release a reference to the HTTP client. When the reference count reaches zero, the client is
   * closed and removed from the registry.
   *
   * @param clientKey the key identifying the client to release
   */
  void releaseClient(String clientKey) {
    ManagedHttpClient managedClient = clientMap.get(clientKey);
    if (managedClient != null) {
      if (managedClient.release()) {
        // Client was closed, remove from map
        clientMap.remove(clientKey, managedClient);
      }
    }
  }

  @VisibleForTesting
  ConcurrentHashMap<String, ManagedHttpClient> getClientMap() {
    return clientMap;
  }

  @VisibleForTesting
  void shutdown() {
    clientMap.values().forEach(ManagedHttpClient::forceClose);
    clientMap.clear();
  }

  /**
   * Managed HTTP client wrapper that provides reference counting for lifecycle management. The HTTP
   * client is closed when the reference count reaches zero.
   */
  static class ManagedHttpClient {
    private final SdkHttpClient httpClient;
    private final String clientKey;
    private final AtomicInteger refCount = new AtomicInteger(0);
    private volatile boolean closed = false;

    ManagedHttpClient(SdkHttpClient httpClient, String clientKey) {
      this.httpClient = httpClient;
      this.clientKey = clientKey;
      LOG.debug("Created managed HTTP client: key={}", clientKey);
    }

    /**
     * Acquire a reference to the HTTP client, incrementing the reference count.
     *
     * @return the underlying HTTP client
     * @throws IllegalStateException if the client has already been closed
     */
    SdkHttpClient acquire() {
      if (closed) {
        throw new IllegalStateException("Cannot acquire closed HTTP client: " + clientKey);
      }
      int count = refCount.incrementAndGet();
      LOG.debug("Acquired HTTP client: key={}, refCount={}", clientKey, count);
      return httpClient;
    }

    /**
     * Release a reference to the HTTP client, decrementing the reference count. If the count
     * reaches zero, the client is closed.
     *
     * @return true if the client was closed, false otherwise
     */
    boolean release() {
      int count = refCount.decrementAndGet();
      LOG.debug("Released HTTP client: key={}, refCount={}", clientKey, count);
      if (count == 0) {
        return close();
      } else if (count < 0) {
        LOG.warn("HTTP client reference count went negative: key={}, refCount={}", clientKey, count);
      }
      return false;
    }

    /**
     * Close the HTTP client if not already closed.
     *
     * @return true if the client was closed by this call, false if already closed
     */
    private boolean close() {
      if (!closed) {
        synchronized (this) {
          if (!closed) {
            closed = true;
            LOG.debug("Closing HTTP client: key={}", clientKey);
            try {
              httpClient.close();
              return true;
            } catch (Exception e) {
              LOG.warn("Error closing HTTP client: key={}", clientKey, e);
            }
          }
        }
      }
      return false;
    }

    /** Force close the HTTP client regardless of reference count (for testing/shutdown). */
    void forceClose() {
      close();
    }

    @VisibleForTesting
    int getRefCount() {
      return refCount.get();
    }

    @VisibleForTesting
    boolean isClosed() {
      return closed;
    }
  }
}
