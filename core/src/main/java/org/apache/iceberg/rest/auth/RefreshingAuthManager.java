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

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AuthManager} that provides machinery for refreshing authentication data asynchronously,
 * using a background thread pool.
 *
 * @deprecated since 1.10.0, will be removed in 1.11.0; use {@link ThreadPools#authRefreshPool()}.
 */
@Deprecated
public abstract class RefreshingAuthManager implements AuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshingAuthManager.class);

  private final String executorNamePrefix;
  private boolean keepRefreshed = true;
  private volatile ScheduledExecutorService refreshExecutor;

  protected RefreshingAuthManager(String executorNamePrefix) {
    this.executorNamePrefix = executorNamePrefix;
  }

  public void keepRefreshed(boolean keep) {
    this.keepRefreshed = keep;
  }

  @Override
  public void close() {
    ScheduledExecutorService service = refreshExecutor;
    this.refreshExecutor = null;
    if (service != null) {
      List<Runnable> tasks = service.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });

      try {
        if (!service.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Timed out waiting for refresh executor to terminate");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for refresh executor to terminate", e);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Nullable
  protected ScheduledExecutorService refreshExecutor() {
    if (!keepRefreshed) {
      return null;
    }

    if (refreshExecutor == null) {
      synchronized (this) {
        if (refreshExecutor == null) {
          this.refreshExecutor = ThreadPools.newScheduledPool(executorNamePrefix, 1);
        }
      }
    }

    return refreshExecutor;
  }
}
