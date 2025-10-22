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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
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
    // Clean up any existing clients from previous tests
    registry.shutdown();

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

    // First call should create client and increment ref count
    SdkHttpClient client1 = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
    verify(mockFactory1, times(1)).get();

    // Second call with same key should return cached client and increment ref count again
    SdkHttpClient client2 = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
    verify(mockFactory1, times(1)).get(); // Factory should not be called again

    assertThat(client1).isSameAs(client2);

    // Verify reference count is 2
    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        registry.getClientMap().get(cacheKey);
    assertThat(managedClient.getRefCount()).isEqualTo(2);
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
  public void testReferenceCountingAndCleanup() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    String cacheKey = "test-key";

    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        new ManagedHttpClientRegistry.ManagedHttpClient(mockClient, cacheKey);

    // Acquire twice
    WrappedSdkHttpClient client1 = managedClient.acquire();
    WrappedSdkHttpClient client2 = managedClient.acquire();

    assertThat(client1).isSameAs(client2);
    assertThat(managedClient.getRefCount()).isEqualTo(2);

    // First release should not close
    managedClient.release();
    assertThat(managedClient.getRefCount()).isEqualTo(1);
    assertThat(managedClient.isClosed()).isFalse();
    verify(mockClient, times(0)).close();

    // Second release should close
    managedClient.release();
    assertThat(managedClient.getRefCount()).isEqualTo(0);
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close();
  }

  @Test
  public void testAcquireAfterCloseThrows() {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    String cacheKey = "test-key";

    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        new ManagedHttpClientRegistry.ManagedHttpClient(mockClient, cacheKey);

    // Acquire and release to close
    managedClient.acquire();
    managedClient.release();

    assertThat(managedClient.isClosed()).isTrue();

    // Trying to acquire a closed client should throw
    assertThatThrownBy(managedClient::acquire)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Cannot acquire closed HTTP client");
  }

  @Test
  public void testReleaseRemovesFromRegistry() {
    Map<String, String> properties = Maps.newHashMap();
    String cacheKey = "test-key";

    // Create client (refCount = 1)
    SdkHttpClient client1 = registry.getOrCreateClient(cacheKey, mockFactory1, properties);
    assertThat(client1).isNotNull();

    ConcurrentMap<String, ManagedHttpClientRegistry.ManagedHttpClient> clientMap =
        registry.getClientMap();
    assertThat(clientMap).containsKey(cacheKey);

    // Verify ref count is 1
    assertThat(clientMap.get(cacheKey).getRefCount()).isEqualTo(1);

    // Release (refCount = 0, should close and remove)
    registry.releaseClient(cacheKey);

    // Client should be removed from map after close
    assertThat(clientMap).doesNotContainKey(cacheKey);
    verify(mockHttpClient1, times(1)).close();
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

    // Verify reference count equals number of threads
    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        registry.getClientMap().get(cacheKey);
    assertThat(managedClient.getRefCount()).isEqualTo(threadCount);
  }

  @Test
  public void testRegistryShutdown() {
    ConcurrentMap<String, ManagedHttpClientRegistry.ManagedHttpClient> clientMap =
        registry.getClientMap();
    Map<String, String> properties = Maps.newHashMap();

    // Create some clients
    registry.getOrCreateClient("key1", mockFactory1, properties);
    registry.getOrCreateClient("key2", mockFactory2, properties);

    // Verify clients were stored
    assertThat(clientMap.size()).isGreaterThan(0);

    // Shutdown should clean up the map
    registry.shutdown();

    // Map should be empty after shutdown
    assertThat(clientMap.size()).isEqualTo(0);

    // Both clients should be closed
    verify(mockHttpClient1, times(1)).close();
    verify(mockHttpClient2, times(1)).close();
  }

  @Test
  public void testDoubleReleaseDoesNotCauseNegativeRefCount() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    String cacheKey = "test-key";

    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        new ManagedHttpClientRegistry.ManagedHttpClient(mockClient, cacheKey);

    // Acquire once
    WrappedSdkHttpClient client = managedClient.acquire();
    assertThat(managedClient.getRefCount()).isEqualTo(1);

    // First release should close the client (refCount goes to 0)
    boolean closed = managedClient.release();
    assertThat(closed).isTrue();
    assertThat(managedClient.getRefCount()).isEqualTo(0);
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close();

    // Second release on already closed client should be a no-op
    // The closed flag prevents decrement, so refCount stays at 0
    boolean closedAgain = managedClient.release();
    assertThat(closedAgain).isFalse();
    assertThat(managedClient.getRefCount()).isEqualTo(0); // Should still be 0, not negative
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close(); // Close should not be called again
  }

  @Test
  public void testMultipleReleasesAfterClose() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    String cacheKey = "test-key";

    ManagedHttpClientRegistry.ManagedHttpClient managedClient =
        new ManagedHttpClientRegistry.ManagedHttpClient(mockClient, cacheKey);

    // Acquire once
    managedClient.acquire();
    assertThat(managedClient.getRefCount()).isEqualTo(1);

    // Release to close
    managedClient.release();
    assertThat(managedClient.isClosed()).isTrue();
    assertThat(managedClient.getRefCount()).isEqualTo(0);

    // Try releasing multiple more times (simulating a bug in caller code)
    for (int i = 0; i < 5; i++) {
      boolean result = managedClient.release();
      assertThat(result).isFalse(); // Should return false, not try to close again
      assertThat(managedClient.getRefCount()).isEqualTo(0); // RefCount should never go negative
    }

    // Close should only have been called once
    verify(mockClient, times(1)).close();
  }
}
