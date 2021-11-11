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

package org.apache.iceberg.util;

import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link RemovalListener} that keeps a queue of the cache entries
 * that are expired by a {@link com.github.benmanes.caffeine.cache.Cache}.
 *
 * <p>
 * Useful for testing that caches are properly removing (and retaining) values.
 * <p>
 *
 * See also {@link FakeTicker} for controlling the passage of time when expiring based on time.
 */
// TODO - Consider removing this since I haven't used it yet.
public class CountingCacheRemovalListener<K, V> extends ConcurrentLinkedQueue<K>
    implements RemovalListener<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(CountingCacheRemovalListener.class);
  private final AtomicInteger count = new AtomicInteger();
  private volatile RemovalCause lastRemovalCause;

  @Override
  public void onRemoval(K key, V value, RemovalCause removalCause) {
    count.incrementAndGet();
    LOG.info("Cache removal for entry {} -> {} due to {}", key, value, removalCause);
    lastRemovalCause = removalCause;
    offer(key);
  }

  public int countEvicted() {
    return count.get();
  }

  public RemovalCause lastRemovalCause() {
    return lastRemovalCause;
  }

  public static <K, V> CountingCacheRemovalListener<K, V> newInstance() {
    return new CountingCacheRemovalListener<>();
  }
}

