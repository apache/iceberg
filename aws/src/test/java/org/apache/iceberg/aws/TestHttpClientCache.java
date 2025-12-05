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
import java.util.function.Supplier;
import org.apache.iceberg.aws.HttpClientCache.ManagedHttpClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.http.SdkHttpClient;

public class TestHttpClientCache {

  @Mock private SdkHttpClient httpClient1;
  @Mock private SdkHttpClient httpClient2;
  @Mock private Supplier<SdkHttpClient> httpClientFactory1;
  @Mock private Supplier<SdkHttpClient> httpClientFactory2;

  private HttpClientCache cache;

  @BeforeEach
  public void before() {
    MockitoAnnotations.openMocks(this);
    cache = HttpClientCache.instance();
    // Clean up any existing clients from previous tests
    cache.clear();

    when(httpClientFactory1.get()).thenReturn(httpClient1);
    when(httpClientFactory2.get()).thenReturn(httpClient2);
  }

  @Test
  public void singletonPattern() {
    HttpClientCache instance1 = HttpClientCache.instance();
    HttpClientCache instance2 = HttpClientCache.instance();

    assertThat(instance1).isSameAs(instance2);
  }

  @Test
  public void clientCaching() {
    final String cacheKey = "test-key";

    // First call should create client and increment ref count
    SdkHttpClient client1 = cache.getOrCreateClient(cacheKey, httpClientFactory1);
    verify(httpClientFactory1, times(1)).get();

    // Second call with same key should return cached client and increment ref count again
    SdkHttpClient client2 = cache.getOrCreateClient(cacheKey, httpClientFactory1);
    verify(httpClientFactory1, times(1)).get(); // Factory should not be called again

    assertThat(client1).isSameAs(client2);

    // Verify reference count is 2
    ManagedHttpClient managedClient = cache.clients().get(cacheKey);
    assertThat(managedClient.refCount()).isEqualTo(2);
  }

  @Test
  public void differentKeysCreateDifferentClients() {
    SdkHttpClient client1 = cache.getOrCreateClient("test-key-1", httpClientFactory1);
    SdkHttpClient client2 = cache.getOrCreateClient("test-key-2", httpClientFactory2);

    verify(httpClientFactory1, times(1)).get();
    verify(httpClientFactory2, times(1)).get();

    assertThat(client1).isNotSameAs(client2);
  }

  @Test
  public void referenceCountingAndCleanup() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    ManagedHttpClient managedClient = new ManagedHttpClient(mockClient, cacheKey);

    // Acquire twice
    ManagedHttpClient client1 = managedClient.acquire();
    ManagedHttpClient client2 = managedClient.acquire();

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
  public void acquireAfterCloseThrows() {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    ManagedHttpClient managedClient = new ManagedHttpClient(mockClient, cacheKey);

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
  public void releaseRemovesFromRegistry() {
    final String cacheKey = "test-key";

    // Create client (refCount = 1)
    SdkHttpClient client1 = cache.getOrCreateClient(cacheKey, httpClientFactory1);
    assertThat(client1).isNotNull();

    Map<String, ManagedHttpClient> clients = cache.clients();
    assertThat(clients).containsKey(cacheKey);

    // Verify ref count is 1
    assertThat(clients.get(cacheKey).refCount()).isEqualTo(1);

    // Release (refCount = 0, should close and remove)
    cache.releaseClient(cacheKey);

    // Client should be removed from map after close
    assertThat(clients).doesNotContainKey(cacheKey);
    verify(httpClient1, times(1)).close();
  }

  @Test
  public void concurrentAccess() throws InterruptedException {
    final String cacheKey = "concurrent-test-key";
    int threadCount = 10;
    Thread[] threads = new Thread[threadCount];
    SdkHttpClient[] results = new SdkHttpClient[threadCount];

    // Create multiple threads that access the same cache key
    for (int i = 0; i < threadCount; i++) {
      final int index = i;
      threads[i] =
          new Thread(() -> results[index] = cache.getOrCreateClient(cacheKey, httpClientFactory1));
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
    verify(httpClientFactory1, times(1)).get();

    // Verify all threads got the same client instance
    SdkHttpClient expectedClient = results[0];
    for (int i = 1; i < threadCount; i++) {
      assertThat(results[i]).isSameAs(expectedClient);
    }

    // Verify reference count equals number of threads
    ManagedHttpClient managedClient = cache.clients().get(cacheKey);
    assertThat(managedClient.refCount()).isEqualTo(threadCount);
  }

  @Test
  public void registryClear() {
    Map<String, ManagedHttpClient> clients = cache.clients();

    // Create some clients
    cache.getOrCreateClient("key1", httpClientFactory1);
    cache.getOrCreateClient("key2", httpClientFactory2);

    // Verify clients were stored
    assertThat(clients).hasSize(2);

    // Shutdown should clean up the map
    cache.clear();

    // Map should be empty after shutdown
    assertThat(clients).isEmpty();

    // Both clients should be closed
    verify(httpClient1, times(1)).close();
    verify(httpClient2, times(1)).close();
  }

  @Test
  public void doubleReleaseDoesNotCauseNegativeRefCount() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    ManagedHttpClient managedClient = new ManagedHttpClient(mockClient, cacheKey);

    // Acquire once
    ManagedHttpClient client = managedClient.acquire();
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
  public void multipleReleasesAfterClose() throws Exception {
    SdkHttpClient mockClient = mock(SdkHttpClient.class);
    final String cacheKey = "test-key";

    ManagedHttpClient managedClient = new ManagedHttpClient(mockClient, cacheKey);

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
