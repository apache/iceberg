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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.SystemConfigs;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadPools {
  private static final Logger LOG = LoggerFactory.getLogger(ThreadPools.class);

  private ThreadPools() {}

  /**
   * @deprecated Use {@link SystemConfigs#WORKER_THREAD_POOL_SIZE} instead. will be removed in
   *     1.12.0
   */
  @Deprecated
  public static final String WORKER_THREAD_POOL_SIZE_PROP =
      SystemConfigs.WORKER_THREAD_POOL_SIZE.propertyKey();

  private static final Duration DEFAULT_SHUTDOWN_TIMEOUT = Duration.ofSeconds(120);

  public static final int WORKER_THREAD_POOL_SIZE = SystemConfigs.WORKER_THREAD_POOL_SIZE.value();

  private static final ThreadPoolManager THREAD_POOL_MANAGER = new ThreadPoolManager();

  private static final ExecutorService WORKER_POOL =
      newExitingWorkerPool("iceberg-worker-pool", WORKER_THREAD_POOL_SIZE);

  public static final int DELETE_WORKER_THREAD_POOL_SIZE =
      SystemConfigs.DELETE_WORKER_THREAD_POOL_SIZE.value();

  private static final ExecutorService DELETE_WORKER_POOL =
      newExitingWorkerPool("iceberg-delete-worker-pool", DELETE_WORKER_THREAD_POOL_SIZE);

  public static final int AUTH_REFRESH_THREAD_POOL_SIZE =
      SystemConfigs.AUTH_REFRESH_THREAD_POOL_SIZE.value();

  private static Thread shutdownHook;

  static {
    initShutdownHook();
  }

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
   * A shared {@link ScheduledExecutorService} that REST catalogs can use for refreshing their
   * authentication data.
   */
  public static ScheduledExecutorService authRefreshPool() {
    return AuthRefreshPoolHolder.INSTANCE;
  }

  private static class AuthRefreshPoolHolder {
    private static final ScheduledExecutorService INSTANCE =
        ThreadPools.newExitingScheduledPool(
            "auth-session-refresh", AUTH_REFRESH_THREAD_POOL_SIZE, Duration.ZERO);
  }

  /**
   * Creates a fixed-size thread pool that uses daemon threads.
   *
   * <p>For clarity and to avoid potential issues with shutdown hook accumulation, prefer using
   * either {@link #newExitingWorkerPool(String, int)} or {@link #newFixedThreadPool(String, int)},
   * depending on the intended lifecycle of the thread pool.
   *
   * @deprecated will be removed in 1.12.0. Use {@link #newExitingWorkerPool(String, int)} for
   *     long-lived thread pools that require a shutdown hook, or {@link #newFixedThreadPool(String,
   *     int)} for short-lived thread pools where you manage the lifecycle.
   */
  @Deprecated
  public static ExecutorService newWorkerPool(String namePrefix) {
    return newExitingWorkerPool(namePrefix, WORKER_THREAD_POOL_SIZE);
  }

  /**
   * Creates a fixed-size thread pool that uses daemon threads.
   *
   * <p>For clarity and to avoid potential issues with shutdown hook accumulation, prefer using
   * either {@link #newExitingWorkerPool(String, int)} or {@link #newFixedThreadPool(String, int)},
   * depending on the intended lifecycle of the thread pool.
   *
   * @deprecated will be removed in 1.12.0. Use {@link #newExitingWorkerPool(String, int)} for
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
    ExecutorService service =
        Executors.unconfigurableExecutorService(newFixedThreadPool(namePrefix, poolSize));
    THREAD_POOL_MANAGER.addThreadPool(service, DEFAULT_SHUTDOWN_TIMEOUT);
    return service;
  }

  /**
   * Force manual shutdown of the thread pools created via the {@link #newExitingWorkerPool(String,
   * int)}.
   *
   * <p>This method allows: (1) to stop thread pools manually, to avoid leaks in hot-reload
   * environments; (2) opt-out of the standard shutdown mechanism to manage graceful service stops
   * (and commit the last pending files, if the client application needs to react to shutdown hooks
   * on its own).
   *
   * <p>Please only call this method at the end of the intended usage of the library, and NEVER
   * before, as this method will stop thread pools required for normal library workflows.
   */
  public static void shutdownThreadPools() {
    THREAD_POOL_MANAGER.shutdownAll();
    removeShutdownHook();
  }

  /**
   * Initialize a shutdown hook to stop the thread pools created via the {@link
   * #newExitingWorkerPool(String, int)}.
   */
  @SuppressWarnings("ShutdownHook")
  static synchronized void initShutdownHook() {
    if (shutdownHook == null) {
      shutdownHook =
          Executors.defaultThreadFactory()
              .newThread(
                  new Runnable() {
                    @Override
                    public void run() {
                      shutdownHook = null;
                      shutdownThreadPools();
                    }
                  });

      try {
        shutdownHook.setName("DelayedShutdownHook-iceberg");
      } catch (SecurityException e) {
        LOG.warn("Cannot set thread name for the shutdown hook", e);
      }

      try {
        Runtime.getRuntime().addShutdownHook(shutdownHook);
      } catch (SecurityException e) {
        LOG.warn("Cannot install a shutdown hook for thread pools clean up", e);
      }
    }
  }

  /**
   * Stop the shutdown hook for the thread pools created via the {@link
   * #newExitingWorkerPool(String, int)}.
   *
   * <p>Thread pools can still be stopped manually via the {@link #shutdownThreadPools()} method.
   */
  @SuppressWarnings("ShutdownHook")
  public static synchronized void removeShutdownHook() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (SecurityException e) {
        LOG.warn("Cannot remove the shutdown hook for thread pools clean up", e);
      }

      shutdownHook = null;
    }
  }

  /**
   * Check if the shutdown hook has been registered.
   *
   * @return true if the shutdown hook is registered, false otherwise
   */
  @VisibleForTesting
  static synchronized boolean isShutdownHookRegistered() {
    return shutdownHook != null;
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

  /**
   * Create a new {@link ScheduledExecutorService} with the given name and pool size.
   *
   * <p>Threads used by this service will be daemon threads.
   *
   * <p>The service registers a shutdown hook to ensure that it terminates when the JVM exits. This
   * is suitable for long-lived thread pools that should be automatically cleaned up on JVM
   * shutdown.
   */
  public static synchronized ScheduledExecutorService newExitingScheduledPool(
      String namePrefix, int poolSize, Duration terminationTimeout) {
    ScheduledExecutorService service =
        Executors.unconfigurableScheduledExecutorService(newScheduledPool(namePrefix, poolSize));
    THREAD_POOL_MANAGER.addThreadPool(service, terminationTimeout);
    return service;
  }

  private static ThreadFactory newDaemonThreadFactory(String namePrefix) {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(namePrefix + "-%d").build();
  }

  /** Manages the lifecycle of thread pools that need to be shut down gracefully. */
  @VisibleForTesting
  static class ThreadPoolManager {
    private final List<ExecutorServiceWithTimeout> threadPoolsToShutdown = Lists.newArrayList();

    /**
     * Add an executor service to the list of thread pools to be shut down.
     *
     * @param service the executor service to add
     * @param timeout the timeout for shutdown operations
     */
    @VisibleForTesting
    synchronized void addThreadPool(ExecutorService service, Duration timeout) {
      threadPoolsToShutdown.add(new ExecutorServiceWithTimeout(service, timeout));
    }

    /** Shut down all registered thread pools. */
    @VisibleForTesting
    synchronized void shutdownAll() {
      long startTime = System.nanoTime();
      List<ExecutorServiceWithTimeout> pendingShutdown = Lists.newArrayList();

      for (ExecutorServiceWithTimeout item : threadPoolsToShutdown) {
        item.getService().shutdown();
        pendingShutdown.add(item);
      }

      threadPoolsToShutdown.clear();

      for (ExecutorServiceWithTimeout item : pendingShutdown) {
        long timeElapsed = System.nanoTime() - startTime;
        long remainingTime = item.getTimeout().toNanos() - timeElapsed;
        if (remainingTime > 0) {

          try {
            if (!item.service.awaitTermination(remainingTime, TimeUnit.NANOSECONDS)) {
              item.getService().shutdownNow();
            }
          } catch (InterruptedException e) {
            LOG.warn("Interrupted while shutting down, ignoring", e);
          }

        } else {
          item.getService().shutdownNow();
        }
      }
    }
  }

  private static class ExecutorServiceWithTimeout {
    private ExecutorService service;
    private Duration timeout;

    private ExecutorServiceWithTimeout(ExecutorService service, Duration timeout) {
      this.service = service;
      this.timeout = timeout;
    }

    private ExecutorService getService() {
      return service;
    }

    private Duration getTimeout() {
      return timeout;
    }
  }
}
