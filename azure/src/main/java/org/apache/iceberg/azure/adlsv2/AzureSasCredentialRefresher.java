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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.credential.AzureSasCredential;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;

public class AzureSasCredentialRefresher {
  private final Supplier<Pair<String, Long>> sasTokenWithExpirationSupplier;
  private final ScheduledExecutorService refreshExecutor;
  private final AzureSasCredential azureSasCredential;

  private static final long MAX_REFRESH_WINDOW_MILLIS = 300_000; // 5 minutes;
  private static final long MIN_REFRESH_WAIT_MILLIS = 10;

  public AzureSasCredentialRefresher(
      Supplier<Pair<String, Long>> sasTokenWithExpirationSupplier,
      ScheduledExecutorService refreshExecutor) {
    this.sasTokenWithExpirationSupplier = sasTokenWithExpirationSupplier;
    this.refreshExecutor = refreshExecutor;
    Pair<String, Long> sasTokenWithExpiration = sasTokenWithExpirationSupplier.get();
    this.azureSasCredential = new AzureSasCredential(sasTokenWithExpiration.getLeft());
    scheduleRefresh(sasTokenWithExpiration.getRight());
  }

  public AzureSasCredential azureSasCredential() {
    return this.azureSasCredential;
  }

  private void scheduleRefresh(Long expireAtMillis) {
    this.refreshExecutor.schedule(
        () -> {
          Pair<String, Long> sasTokenWithExpiration = sasTokenWithExpirationSupplier.get();
          azureSasCredential.update(sasTokenWithExpiration.getLeft());
          if (sasTokenWithExpiration.getRight() != null) {
            this.scheduleRefresh(sasTokenWithExpiration.getRight());
          }
        },
        refreshDelay(expireAtMillis),
        TimeUnit.MILLISECONDS);
  }

  private long refreshDelay(Long expireAtMillis) {
    long expireInMillis = expireAtMillis - System.currentTimeMillis();
    // how much ahead of time to start the request to allow it to complete
    long refreshWindowMillis = Math.min(expireInMillis / 10, MAX_REFRESH_WINDOW_MILLIS);
    // how much time to wait before expiration
    long waitIntervalMillis = expireInMillis - refreshWindowMillis;
    // how much time to actually wait
    return Math.max(waitIntervalMillis, MIN_REFRESH_WAIT_MILLIS);
  }
}
