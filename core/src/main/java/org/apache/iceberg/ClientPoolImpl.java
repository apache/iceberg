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
package org.apache.iceberg;

import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.Closeable;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ClientPoolImpl<C, E extends Exception>
    implements Closeable, ClientPool<C, E> {
  private static final Logger LOG = LoggerFactory.getLogger(ClientPoolImpl.class);

  private final int poolSize;
  private final Deque<C> clients;
  private final Class<? extends E> reconnectExc;
  private final Object signal = new Object();
  private final boolean retryByDefault;
  private final int maxRetries;

  private volatile int currentSize;
  private boolean closed;

  private int connectionRetryWaitPeriodMs = 1000;

  public ClientPoolImpl(int poolSize, Class<? extends E> reconnectExc, boolean retryByDefault) {
    this(poolSize, reconnectExc, retryByDefault, 1);
  }

  public ClientPoolImpl(
      int poolSize,
      Class<? extends E> reconnectExc,
      boolean retryByDefault,
      int maxConnectionRetries) {
    this.poolSize = poolSize;
    this.reconnectExc = reconnectExc;
    this.clients = new ArrayDeque<>(poolSize);
    this.currentSize = 0;
    this.closed = false;
    this.retryByDefault = retryByDefault;
    this.maxRetries = maxConnectionRetries;
  }

  @Override
  public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
    return run(action, retryByDefault);
  }

  @Override
  public <R> R run(Action<R, C, E> action, boolean retry) throws E, InterruptedException {
    AtomicReference<C> clientReference = new AtomicReference<>();
    clientReference.set(get());

    RetryPolicy<Object> retryPolicy =
        RetryPolicy.builder()
            .handle(reconnectExc)
            .withMaxRetries(retry ? maxRetries : 0)
            .withDelay(Duration.ofMillis(connectionRetryWaitPeriodMs))
            .onRetry(
                exc -> {
                  C currentClient = clientReference.get();
                  clientReference.set(reconnect(currentClient));
                })
            .build();

    try {
      AtomicReference<R> result = new AtomicReference<>();
      Failsafe.with(retryPolicy).run(() -> result.set(action.run(clientReference.get())));
      return result.get();
    } finally {
      release(clientReference.get());
    }
  }

  protected abstract C newClient();

  protected abstract C reconnect(C client);

  protected boolean isConnectionException(Exception exc) {
    return reconnectExc.isInstance(exc);
  }

  protected abstract void close(C client);

  @Override
  public void close() {
    this.closed = true;
    try {
      while (currentSize > 0) {
        if (!clients.isEmpty()) {
          synchronized (this) {
            if (!clients.isEmpty()) {
              C client = clients.removeFirst();
              close(client);
              currentSize -= 1;
            }
          }
        }
        if (clients.isEmpty() && currentSize > 0) {
          // wake every second in case this missed the signal
          synchronized (signal) {
            signal.wait(1000);
          }
        }
      }

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted while shutting down pool. Some clients may not be closed.", e);
    }
  }

  private C get() throws InterruptedException {
    Preconditions.checkState(!closed, "Cannot get a client from a closed pool");
    while (true) {
      if (!clients.isEmpty() || currentSize < poolSize) {
        synchronized (this) {
          if (!clients.isEmpty()) {
            return clients.removeFirst();
          } else if (currentSize < poolSize) {
            C client = newClient();
            currentSize += 1;
            return client;
          }
        }
      }
      synchronized (signal) {
        // wake every second in case this missed the signal
        signal.wait(1000);
      }
    }
  }

  private void release(C client) {
    synchronized (this) {
      clients.addFirst(client);
    }
    synchronized (signal) {
      signal.notify();
    }
  }

  @VisibleForTesting
  Deque<C> clients() {
    return clients;
  }

  public int poolSize() {
    return poolSize;
  }

  public boolean isClosed() {
    return closed;
  }
}
