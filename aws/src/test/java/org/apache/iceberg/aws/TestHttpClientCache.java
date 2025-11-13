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

import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import org.apache.iceberg.aws.HttpClientCache.WrappedSdkHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestHttpClientCache {

  @Mock private SdkHttpClient mockHttpClient1;
  @Mock private SdkHttpClient mockHttpClient2;
  @Mock private Supplier<SdkHttpClient> mockFactory1;
  @Mock private Supplier<SdkHttpClient> mockFactory2;

  private HttpClientCache cache;

  @BeforeEach
  public void before() {
    MockitoAnnotations.openMocks(this);
    cache = HttpClientCache.getInstance();
    // Clean up any existing clients from previous tests
    cache.shutdown();

    when(mockFactory1.get()).thenReturn(mockHttpClient1);
    when(mockFactory2.get()).thenReturn(mockHttpClient2);
  }

  @Test
  public void testSingletonPattern() {
    HttpClientCache instance1 = HttpClientCache.getInstance();
    HttpClientCache instance2 = HttpClientCache.getInstance();

    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  public void testClientCaching() {
    final String cacheKey = "test-key";

    // First call should create client and increment ref count
    SdkHttpClient client1 = cache.getOrCreateClient(cacheKey, mockFactory1);
    verify(mockFactory1, times(1)).get();

    // Second call with same key should return cached client and increment ref count again
    SdkHttpClient client2 = cache.getOrCreateClient(cacheKey, mockFactory1);
    verify(mockFactory1, times(1)).get(); // Factory should not be called again

    assertThat(client1).isSameAs(client2);

    // Verify reference count is 2
    HttpClientCache.ManagedHttpClient managedClient = cache.clientMap().get(cacheKey);
    assertThat(managedClient.refCount()).isEqualTo(2);
  }

  @Test
  public void testDifferentKeysCreateDifferentClients() {
    final String cacheKey1 = "test-key-1";
    final String cacheKey2 = "test-key-2";

    SdkHttpClient client1 = cache.getOrCreateClient(cacheKey1, mockFactory1);
    SdkHttpClient client2 = cache.getOrCreateClient(cacheKey2, mockFactory2);

    verify(mockFactory1, times(1)).get();
    verify(mockFactory2, times(1)).get();

    assertThat(client1).isNotSameAs(client2);
  }

  @Test
  public void testReferenceCountingAndCleanup() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    HttpClientCache.ManagedHttpClient managedClient =
        new HttpClientCache.ManagedHttpClient(mockClient, cacheKey);

    // Acquire twice
    WrappedSdkHttpClient client1 = managedClient.acquire();
    WrappedSdkHttpClient client2 = managedClient.acquire();

    assertThat(client1).isSameAs(client2);
    assertThat(managedClient.refCount()).isEqualTo(2);

    // First release should not close
    managedClient.release();
    assertThat(managedClient.refCount()).isEqualTo(1);
    assertThat(managedClient.isClosed()).isFalse();
    verify(mockClient, times(0)).close();

    // Second release should close
    managedClient.release();
    assertThat(managedClient.refCount()).isEqualTo(0);
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close();
  }

  @Test
  public void testAcquireAfterCloseThrows() {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    HttpClientCache.ManagedHttpClient managedClient =
        new HttpClientCache.ManagedHttpClient(mockClient, cacheKey);

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
    final String cacheKey = "test-key";

    // Create client (refCount = 1)
    SdkHttpClient client1 = cache.getOrCreateClient(cacheKey, mockFactory1);
    assertThat(client1).isNotNull();

    ConcurrentMap<String, HttpClientCache.ManagedHttpClient> clientMap = cache.clientMap();
    assertThat(clientMap).containsKey(cacheKey);

    // Verify ref count is 1
    assertThat(clientMap.get(cacheKey).refCount()).isEqualTo(1);

    // Release (refCount = 0, should close and remove)
    cache.releaseClient(cacheKey);

    // Client should be removed from map after close
    assertThat(clientMap).doesNotContainKey(cacheKey);
    verify(mockHttpClient1, times(1)).close();
  }

  @Test
  public void testConcurrentAccess() throws InterruptedException {
    final String cacheKey = "concurrent-test-key";
    int threadCount = 10;
    Thread[] threads = new Thread[threadCount];
    SdkHttpClient[] results = new SdkHttpClient[threadCount];

    // Create multiple threads that access the same cache key
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      threads[i] =
          new Thread(
              () -> {
                results[index] = cache.getOrCreateClient(cacheKey, mockFactory1);
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
    HttpClientCache.ManagedHttpClient managedClient = cache.clientMap().get(cacheKey);
    assertThat(managedClient.refCount()).isEqualTo(threadCount);
  }

  @Test
  public void testRegistryShutdown() {
    ConcurrentMap<String, HttpClientCache.ManagedHttpClient> clientMap = cache.clientMap();

    // Create some clients
    cache.getOrCreateClient("key1", mockFactory1);
    cache.getOrCreateClient("key2", mockFactory2);

    // Verify clients were stored
    assertThat(clientMap.size()).isGreaterThan(0);

    // Shutdown should clean up the map
    cache.shutdown();

    // Map should be empty after shutdown
    assertThat(clientMap.size()).isEqualTo(0);

    // Both clients should be closed
    verify(mockHttpClient1, times(1)).close();
    verify(mockHttpClient2, times(1)).close();
  }

  @Test
  public void testDoubleReleaseDoesNotCauseNegativeRefCount() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    HttpClientCache.ManagedHttpClient managedClient =
        new HttpClientCache.ManagedHttpClient(mockClient, cacheKey);

    // Acquire once
    WrappedSdkHttpClient client = managedClient.acquire();
    assertThat(managedClient.refCount()).isEqualTo(1);

    // First release should close the client (refCount goes to 0)
    boolean closed = managedClient.release();
    assertThat(closed).isTrue();
    assertThat(managedClient.refCount()).isEqualTo(0);
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close();

    // Second release on already closed client should be a no-op
    // The closed flag prevents decrement, so refCount stays at 0
    boolean closedAgain = managedClient.release();
    assertThat(closedAgain).isFalse();
    assertThat(managedClient.refCount()).isEqualTo(0); // Should still be 0, not negative
    assertThat(managedClient.isClosed()).isTrue();
    verify(mockClient, times(1)).close(); // Close should not be called again
  }

  @Test
  public void testMultipleReleasesAfterClose() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    HttpClientCache.ManagedHttpClient managedClient =
        new HttpClientCache.ManagedHttpClient(mockClient, cacheKey);

    // Acquire once
    managedClient.acquire();
    assertThat(managedClient.refCount()).isEqualTo(1);

    // Release to close
    managedClient.release();
    assertThat(managedClient.isClosed()).isTrue();
    assertThat(managedClient.refCount()).isEqualTo(0);

    // Try releasing multiple more times (simulating a bug in caller code)
    for (int i = 0; i < 5; i++) {
      boolean result = managedClient.release();
      assertThat(result).isFalse(); // Should return false, not try to close again
      assertThat(managedClient.refCount()).isEqualTo(0); // RefCount should never go negative
    }

    // Close should only have been called once
    verify(mockClient, times(1)).close();
  }
}
