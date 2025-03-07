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
package org.apache.iceberg.vortex;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToLongFunction;

public final class PrefetchingIterator<T> implements Iterator<T>, AutoCloseable {
  // Global condition variable shared between the prefetcher and consumer threads,
  // to coordinate wake ups for when the buffer may no longer be full.
  private static final Object CONDITION = new Object();

  private final BlockingQueue<T> fetched = new LinkedBlockingQueue<>();
  private final Thread producerThread;
  private final Iterator<T> delegate;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicLong bufferBytes = new AtomicLong(0);
  private final long maxBufferSize;
  private final ToLongFunction<T> sizeFunc;

  PrefetchingIterator(Iterator<T> delegate, long maxBufferSize, ToLongFunction<T> sizeFunc) {
    this.delegate = delegate;
    this.maxBufferSize = maxBufferSize;
    this.sizeFunc = sizeFunc;
    this.producerThread = new Thread(this::prefetchLoop, "vortex-prefetch-thread");
    producerThread.setDaemon(true);
    producerThread.start();
  }

  private void prefetchLoop() {
    try {
      while (!closed.get() && delegate.hasNext()) {
        while (bufferBytes.get() > maxBufferSize) {
          synchronized (CONDITION) {
            CONDITION.wait();
          }
        }
        T nextElem = delegate.next();
        long elemSize = sizeFunc.applyAsLong(nextElem);
        bufferBytes.addAndGet(elemSize);
        fetched.put(nextElem);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Prefetching interrupted", e);
    } catch (Exception e) {
      throw new RuntimeException("Prefetching failed", e);
    } finally {
      closed.set(true);
    }
  }

  @Override
  public boolean hasNext() {
    // If the prefetcher is not finished, then we could be waiting for
    // a fetched item.
    while (!closed.get()) {
      if (!fetched.isEmpty()) {
        return true;
      }
    }
    // If the prefetcher is finished, then we can examine fetched and immediately return a result.
    return !fetched.isEmpty();
  }

  @Override
  public T next() {
    // We assume that this has been called after hasNext() returned true, so it is
    // safe to call take() without checking if the queue maybe be empty.
    try {
      T nextElem = this.fetched.take();
      long elemSize = sizeFunc.applyAsLong(nextElem);
      bufferBytes.addAndGet(-elemSize);
      // Notify the producer that it may now be able to add more items to the queue.
      synchronized (CONDITION) {
        CONDITION.notify();
      }
      return nextElem;
    } catch (InterruptedException e) {
      throw new RuntimeException("Prefetch queue take interrupted", e);
    }
  }

  @Override
  public void close() {
    closed.set(true);
    producerThread.interrupt();
  }
}
