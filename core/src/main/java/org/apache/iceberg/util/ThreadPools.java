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

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.iceberg.SystemProperties;

public class ThreadPools {

  private ThreadPools() {}

  public static final String WORKER_THREAD_POOL_SIZE_PROP =
      SystemProperties.WORKER_THREAD_POOL_SIZE_PROP;

  public static final int WORKER_THREAD_POOL_SIZE = getPoolSize(
      WORKER_THREAD_POOL_SIZE_PROP,
      Runtime.getRuntime().availableProcessors());

  private static final ExecutorService WORKER_POOL = MoreExecutors.getExitingExecutorService(
      (ThreadPoolExecutor) Executors.newFixedThreadPool(
          WORKER_THREAD_POOL_SIZE,
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("iceberg-worker-pool-%d")
              .build()));

  /**
   * Return an {@link ExecutorService} that uses the "worker" thread-pool.
   * <p>
   * The size of the worker pool limits the number of tasks concurrently reading manifests in the
   * base table implementation across all concurrent planning operations.
   * <p>
   * The size of this thread-pool is controlled by the Java system property
   * {@code iceberg.worker.num-threads}.
   *
   * @return an {@link ExecutorService} that uses the worker pool
   */
  public static ExecutorService getWorkerPool() {
    return WORKER_POOL;
  }

  private static int getPoolSize(String systemProperty, int defaultSize) {
    String value = System.getProperty(systemProperty);
    if (value != null) {
      try {
        return Integer.parseUnsignedInt(value);
      } catch (NumberFormatException e) {
        // will return the default
      }
    }
    return defaultSize;
  }
}
