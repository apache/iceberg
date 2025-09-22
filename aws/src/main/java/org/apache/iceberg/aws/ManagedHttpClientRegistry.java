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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.lang.ref.Cleaner;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.http.SdkHttpClient;

/**
 * A registry that manages the lifecycle of shared HTTP clients for AWS SDK v2. Resources are
 * cleaned up when garbage collected.
 */
public class ManagedHttpClientRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedHttpClientRegistry.class);
  private static final Cleaner CLEANER = Cleaner.create();

  private final Cache<String, ManagedHttpClient> clientCache;

  private static volatile ManagedHttpClientRegistry instance;

  public static ManagedHttpClientRegistry getInstance() {
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
    this.clientCache = Caffeine.newBuilder().build();
  }

  /**
   * Get or create a managed HTTP client for the given configuration.
   *
   * @param clientKey unique key identifying the client configuration
   * @param clientFactory factory to create the HTTP client if not cached
   * @param properties configuration properties for this client
   * @return a managed HTTP client that handles proper cleanup via Cleaner
   */
  public SdkHttpClient getOrCreateClient(
      String clientKey, Supplier<SdkHttpClient> clientFactory, Map<String, String> properties) {
    return clientCache
        .get(
            clientKey,
            k -> {
              LOG.debug("Creating new managed HTTP client for key: {}", k);
              SdkHttpClient httpClient = clientFactory.get();
              return new ManagedHttpClient(httpClient, k, properties);
            })
        .getHttpClient();
  }

  @VisibleForTesting
  Cache<String, ManagedHttpClient> getClientCache() {
    return clientCache;
  }

  @VisibleForTesting
  void shutdown() {
    clientCache.invalidateAll();
    clientCache.cleanUp();
  }

  /**
   * Managed HTTP client wrapper that provides cleanup using java.lang.ref.Cleaner. The Cleaner
   * ensures resources are eventually closed when the wrapper is garbage collected.
   */
  static class ManagedHttpClient implements AutoCloseable {
    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private final SdkHttpClient httpClient;
    private final String clientKey;
    private final long instanceId;
    private final Cleaner.Cleanable cleanable;

    ManagedHttpClient(SdkHttpClient httpClient, String clientKey, Map<String, String> properties) {
      this.httpClient = httpClient;
      this.clientKey = clientKey;
      this.instanceId = INSTANCE_COUNTER.incrementAndGet();

      // Register Cleaner for GC-based cleanup
      this.cleanable = CLEANER.register(this, new CleanupAction(httpClient, clientKey, instanceId));

      LOG.debug("Created managed HTTP client: key={}, instanceId={}", clientKey, instanceId);
    }

    SdkHttpClient getHttpClient() {
      return httpClient;
    }

    @Override
    public void close() {
      LOG.debug("Closing managed HTTP client: key={}, instanceId={}", clientKey, instanceId);
      try {
        cleanable.clean();
      } catch (Exception e) {
        LOG.warn(
            "Error closing managed HTTP client: key={}, instanceId={}", clientKey, instanceId, e);
      }
    }

    /**
     * Cleanup action for Cleaner - runs on separate thread when object is garbage collected.
     * Following java.lang.ref.Cleaner best practices.
     */
    private static class CleanupAction implements Runnable {
      private final SdkHttpClient httpClient;
      private final String clientKey;
      private final long instanceId;

      CleanupAction(SdkHttpClient httpClient, String clientKey, long instanceId) {
        this.httpClient = httpClient;
        this.clientKey = clientKey;
        this.instanceId = instanceId;
      }

      @Override
      public void run() {
        LOG.debug("Cleaner cleanup for HTTP client: key={}, instanceId={}", clientKey, instanceId);
        try {
          httpClient.close();
        } catch (Exception e) {
          LOG.warn(
              "Error in Cleaner cleanup for HTTP client: key={}, instanceId={}",
              clientKey,
              instanceId,
              e);
        }
      }
    }
  }
}
