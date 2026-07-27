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
package org.apache.iceberg.rest.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class TestAuthSessionCache {

  @Test
  void cachedHitsAndMisses() {
    AuthSessionCache cache =
        new AuthSessionCache(Duration.ofHours(1), Runnable::run, System::nanoTime);
    AuthSession session1 = Mockito.mock(AuthSession.class);
    AuthSession session2 = Mockito.mock(AuthSession.class);

    @SuppressWarnings("unchecked")
    Function<String, AuthSession> loader = Mockito.mock(Function.class);
    Mockito.when(loader.apply("key1")).thenReturn(session1);
    Mockito.when(loader.apply("key2")).thenReturn(session2);

    AuthSession session = cache.cachedSession("key1", loader);
    assertThat(session).isNotNull().isSameAs(session1);

    session = cache.cachedSession("key1", loader);
    assertThat(session).isNotNull().isSameAs(session1);

    session = cache.cachedSession("key2", loader);
    assertThat(session).isNotNull().isSameAs(session2);

    session = cache.cachedSession("key2", loader);
    assertThat(session).isNotNull().isSameAs(session2);

    Mockito.verify(loader, times(1)).apply("key1");
    Mockito.verify(loader, times(1)).apply("key2");

    assertThat(cache.sessionCache().asMap()).hasSize(2);
    cache.close();
    assertThat(cache.sessionCache().asMap()).isEmpty();

    Mockito.verify(session1).close();
    Mockito.verify(session2).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void cacheEviction() {
    AtomicLong ticker = new AtomicLong(0);
    AuthSessionCache cache = new AuthSessionCache(Duration.ofHours(1), Runnable::run, ticker::get);
    AuthSession session1 = Mockito.mock(AuthSession.class);

    Function<String, AuthSession> loader = Mockito.mock(Function.class);
    Mockito.when(loader.apply("key1")).thenReturn(session1);

    AuthSession session = cache.cachedSession("key1", loader);
    assertThat(session).isNotNull().isSameAs(session1);

    Mockito.verify(loader, times(1)).apply("key1");
    Mockito.verify(session1, never()).close();

    ticker.set(TimeUnit.HOURS.toNanos(1));
    cache.sessionCache().cleanUp();
    Mockito.verify(session1).close();

    cache.close();
  }
}
