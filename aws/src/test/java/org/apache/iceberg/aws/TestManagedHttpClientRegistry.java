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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.benmanes.caffeine.cache.Cache;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestManagedHttpClientRegistry {

  @Mock private SdkHttpClient mockHttpClient1;
  @Mock private SdkHttpClient mockHttpClient2;
  @Mock private Supplier<SdkHttpClient> mockFactory1;
  @Mock private Supplier<SdkHttpClient> mockFactory2;

  private ManagedHttpClientRegistry registry;

  @BeforeEach
  public void before() {
    MockitoAnnotations.openMocks(this);
    registry = ManagedHttpClientRegistry.getInstance();

    when(mockFactory1.get()).thenReturn(mockHttpClient1);
    when(mockFactory2.get()).thenReturn(mockHttpClient2);
  }

  @Test
  public void testSingletonPattern() {
    ManagedHttpClientRegistry instance1 = ManagedHttpClientRegistry.getInstance();
    ManagedHttpClientRegistry instance2 = ManagedHttpClientRegistry.getInstance();

    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  public void testClientCaching() {
    Map<String, String> properties = Maps.newHashMap();
    String cacheKey = "test-key";

    // First call should create client
    SdkHttpClient client1 = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
    verify(mockFactory1, times(1)).get();

    // Second call with same key should return cached client
    SdkHttpClient client2 = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
    verify(mockFactory1, times(1)).get(); // Factory should not be called again

    assertThat(client1).isSameAs(client2);
  }

  @Test
  public void testDifferentKeysCreateDifferentClients() {
    Map<String, String> properties = Maps.newHashMap();
    String cacheKey1 = "test-key-1";
    String cacheKey2 = "test-key-2";

    SdkHttpClient client1 = registry.getOrCreateClient(cacheKey1, mockFactory1, properties);
    SdkHttpClient client2 = registry.getOrCreateClient(cacheKey2, mockFactory2, properties);

    verify(mockFactory1, times(1)).get();
    verify(mockFactory2, times(1)).get();

    assertThat(client1).isNotSameAs(client2);
  }

  @Test
  public void testManagedHttpClientCleanup() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    Map<String, String> properties = Maps.newHashMap();

    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        new ManagedHttpClientRegistry.ManagedHttpClient(mockClient, "test-key", properties);

    // Verify the wrapped client is returned
    assertThat(managedClient.getHttpClient()).isSameAs(mockClient);

    // Verify cleanup calls close on the underlying client
    managedClient.close();
    verify(mockClient, times(1)).close();
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    Map<String, String> properties = Maps.newHashMap();
    String cacheKey = "concurrent-test-key";
    int threadCount = 10;
    Thread[] threads = new Thread[threadCount];
    SdkHttpClient[] results = new SdkHttpClient[threadCount];

    // Create multiple threads that access the same cache key
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      threads[i] =
          new Thread(
              () -> {
                results[index] = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
              });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify factory was called only once (proper caching under concurrency)
    verify(mockFactory1, times(1)).get();

    // Verify all threads got the same client instance
    SdkHttpClient expectedClient = results[0];
    for (int i = 1; i < threadCount; i++) {
      assertThat(results[i]).isSameAs(expectedClient);
    }
  }

  @Test
  public void testRegistryShutdown() {
    Cache<String, ManagedHttpClientRegistry.ManagedHttpClient> cache = registry.getClientCache();
    Map<String, String> properties = Maps.newHashMap();

    // Create some clients
    registry.getOrCreateClient("key1", mockFactory1, properties);
    registry.getOrCreateClient("key2", mockFactory2, properties);

    // Verify clients were cached
    assertThat(cache.estimatedSize()).isGreaterThan(0);

    // Shutdown should clean up the cache
    registry.shutdown();

    // Cache should be empty after shutdown
    assertThat(cache.estimatedSize()).isEqualTo(0);
  }
}
