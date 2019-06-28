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

package org.apache.iceberg.hive;

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Deque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ClientPool<C, E extends Exception> implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(ClientPool.class);

  private final int poolSize;
  private final Deque<C> clients;
  private final Class<? extends E> reconnectExc;
  private final Object signal = new Object();
  private volatile int currentSize;
  private boolean closed;
  private int runs = 0;

  ClientPool(int poolSize, Class<? extends E> reconnectExc) {
    this.poolSize = poolSize;
    this.reconnectExc = reconnectExc;
    this.clients = new ArrayDeque<>(poolSize);
    this.currentSize = 0;
    this.closed = false;
  }

  interface Action<R, C, E extends Exception> {
    R run(C client) throws E;
  }

  public <R> R run(Action<R, C, E> action) throws E, InterruptedException {
    runs += 1;
    C client = get();
    try {
      return action.run(client);

    } catch (Exception exc) {
      if (reconnectExc.isInstance(exc)) {
        try {
          client = reconnect(client);
        } catch (Exception ignored) {
          // if reconnection throws any exception, rethrow the original failure
          throw reconnectExc.cast(exc);
        }

        return action.run(client);
      }

      throw exc;

    } finally {
      release(client);
    }
  }

  protected abstract C newClient();

  protected abstract C reconnect(C client);

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
            currentSize += 1;
            return newClient();
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
}
