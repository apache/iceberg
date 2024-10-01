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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;

public class ThreadPools {

  private ThreadPools() {}

  /**
   * @deprecated Use {@link SystemConfigs#WORKER_THREAD_POOL_SIZE WORKER_THREAD_POOL_SIZE} instead;
   *     will be removed in 2.0.0
   */
  @Deprecated
  public static final String WORKER_THREAD_POOL_SIZE_PROP =
      SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey();

  public static final int WORKER_THREAD_POOL_SIZE = SystemConfigs.WORKER_THREAD_POOL_SIZE.value();

  private static final ExecutorService WORKER_POOL =
      newExitingWorkerPool("iceberg-worker-pool", WORKER_THREAD_POOL_SIZE);

  public static final int DELETE_WORKER_THREAD_POOL_SIZE =
      SystemConfigs.DELETE_WORKER_THREAD_POOL_SIZE.value();

  private static final ExecutorService DELETE_WORKER_POOL =
      newExitingWorkerPool("iceberg-delete-worker-pool", DELETE_WORKER_THREAD_POOL_SIZE);

  /**
   * Return an {@link ExecutorService} that uses the "worker" thread-pool.
   *
   * <p>The size of the worker pool limits the number of tasks concurrently reading manifests in the
   * base table implementation across all concurrent planning operations.
   *
   * <p>The size of this thread-pool is controlled by the Java system property {@code
   * iceberg.worker.num-threads}.
   *
   * @return an {@link ExecutorService} that uses the worker pool
   */
  public static ExecutorService getWorkerPool() {
    return WORKER_POOL;
  }

  /**
   * Return an {@link ExecutorService} that uses the "delete worker" thread-pool.
   *
   * <p>The size of this worker pool limits the number of tasks concurrently reading delete files
   * within a single JVM. If there are multiple threads loading deletes, all of them will share this
   * worker pool by default.
   *
   * <p>The size of this thread-pool is controlled by the Java system property {@code
   * iceberg.worker.delete-num-threads}.
   *
   * @return an {@link ExecutorService} that uses the delete worker pool
   */
  public static ExecutorService getDeleteWorkerPool() {
    return DELETE_WORKER_POOL;
  }

  /**
   * Creates a fixed-size thread pool that uses daemon threads. The pool is wrapped with {@link
   * MoreExecutors#getExitingExecutorService(ThreadPoolExecutor)}, which registers a shutdown hook
   * to ensure the pool terminates when the JVM exits. <b>Important:</b> Even if the pool is
   * explicitly shut down using {@link ExecutorService#shutdown()}, the shutdown hook is <i>not</i>
   * removed. This can lead to accumulation of shutdown hooks if this method is used repeatedly for
   * short-lived thread pools.
   *
   * <p>For clarity and to avoid potential issues with shutdown hook accumulation, prefer using
   * either {@link #newExitingWorkerPool(String, int)} or {@link #newFixedThreadPool(String, int)},
   * depending on the intended lifecycle of the thread pool.
   *
   * @deprecated will be removed in 2.0.0. Use {@link #newExitingWorkerPool(String, int)} for
   *     long-lived thread pools that require a shutdown hook, or {@link #newFixedThreadPool(String,
   *     int)} for short-lived thread pools where you manage the lifecycle.
   */
  @Deprecated
  public static ExecutorService newWorkerPool(String namePrefix) {
    return newExitingWorkerPool(namePrefix, WORKER_THREAD_POOL_SIZE);
  }

  /**
   * Creates a fixed-size thread pool that uses daemon threads. The pool is wrapped with {@link
   * MoreExecutors#getExitingExecutorService(ThreadPoolExecutor)}, which registers a shutdown hook
   * to ensure the pool terminates when the JVM exits. <b>Important:</b> Even if the pool is
   * explicitly shut down using {@link ExecutorService#shutdown()}, the shutdown hook is <i>not</i>
   * removed. This can lead to accumulation of shutdown hooks if this method is used repeatedly for
   * short-lived thread pools.
   *
   * <p>For clarity and to avoid potential issues with shutdown hook accumulation, prefer using
   * either {@link #newExitingWorkerPool(String, int)} or {@link #newFixedThreadPool(String, int)},
   * depending on the intended lifecycle of the thread pool.
   *
   * @deprecated will be removed in 2.0.0. Use {@link #newExitingWorkerPool(String, int)} for
   *     long-lived thread pools that require a shutdown hook, or {@link #newFixedThreadPool(String,
   *     int)} for short-lived thread pools where you manage the lifecycle.
   */
  @Deprecated
  public static ExecutorService newWorkerPool(String namePrefix, int poolSize) {
    return newExitingWorkerPool(namePrefix, poolSize);
  }

  /**
   * Creates a fixed-size thread pool that uses daemon threads and registers a shutdown hook to
   * ensure the pool terminates when the JVM exits. This is suitable for long-lived thread pools
   * that should be automatically cleaned up on JVM shutdown.
   */
  public static ExecutorService newExitingWorkerPool(String namePrefix, int poolSize) {
    return MoreExecutors.getExitingExecutorService(
        (ThreadPoolExecutor) newFixedThreadPool(namePrefix, poolSize));
  }

  /** Creates a fixed-size thread pool that uses daemon threads. */
  public static ExecutorService newFixedThreadPool(String namePrefix, int poolSize) {
    return Executors.newFixedThreadPool(poolSize, newDaemonThreadFactory(namePrefix));
  }

  /**
   * Create a new {@link ScheduledExecutorService} with the given name and pool size.
   *
   * <p>Threads used by this service will be daemon threads.
   *
   * @param namePrefix a base name for threads in the executor service's thread pool
   * @param poolSize max number of threads to use
   * @return an executor service
   */
  public static ScheduledExecutorService newScheduledPool(String namePrefix, int poolSize) {
    return new ScheduledThreadPoolExecutor(poolSize, newDaemonThreadFactory(namePrefix));
  }

  private static ThreadFactory newDaemonThreadFactory(String namePrefix) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(namePrefix + "-%d").build();
  }
}
