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

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("ShutdownHook")
public class ManagedThreadPools {
  private static final Logger LOG = LoggerFactory.getLogger(ManagedThreadPools.class);
  private static final List<PoolEntry> POOLS = Lists.newArrayList();

  private ManagedThreadPools() {}

  private static class PoolEntry {
    private final ExecutorService executor;
    private final long terminationTimeoutMs;

    PoolEntry(ExecutorService executor, long terminationTimeoutMs) {
      this.executor = executor;
      this.terminationTimeoutMs = terminationTimeoutMs;
    }
  }

  public static void add(ExecutorService executor, Duration terminationTimeout) {
    synchronized (POOLS) {
      POOLS.add(new PoolEntry(executor, terminationTimeout.toMillis()));
    }
  }

  public static void remove(ExecutorService executor) {
    synchronized (POOLS) {
      POOLS.removeIf(entry -> entry.executor == executor);
    }
  }

  public static void shutdownAll() {
    synchronized (POOLS) {
      for (PoolEntry entry : POOLS) {
        entry.executor.shutdown();
        if (entry.terminationTimeoutMs > 0
            && !Uninterruptibles.awaitTerminationUninterruptibly(
                entry.executor, entry.terminationTimeoutMs, TimeUnit.MILLISECONDS)) {
          LOG.warn("Timed out waiting for thread pool to terminate");
        }
        entry.executor.shutdownNow();
      }
    }
  }

  static {
    Runtime.getRuntime().addShutdownHook(new Thread(ManagedThreadPools::shutdownAll));
  }

  @VisibleForTesting
  static List<ExecutorService> getPools() {
    synchronized (POOLS) {
      List<ExecutorService> executors = Lists.newArrayList();
      for (PoolEntry entry : POOLS) {
        executors.add(entry.executor);
      }
      return ImmutableList.copyOf(executors);
    }
  }
}
