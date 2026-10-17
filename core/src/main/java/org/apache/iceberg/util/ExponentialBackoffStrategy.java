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

import java.util.concurrent.ThreadLocalRandom;

/**
 * The default {@link BackoffStrategy}: exponential backoff with up to 10% additive jitter.
 *
 * <p>This reproduces the historical, hardcoded {@link Tasks} backoff exactly. It is used whenever
 * no custom strategy is configured, so existing callers see byte-for-byte identical wait times.
 *
 * <p>Immutable and therefore thread-safe.
 */
class ExponentialBackoffStrategy implements BackoffStrategy {
  private final long minSleepTimeMs;
  private final long maxSleepTimeMs;
  private final double scaleFactor;

  ExponentialBackoffStrategy(long minSleepTimeMs, long maxSleepTimeMs, double scaleFactor) {
    this.minSleepTimeMs = minSleepTimeMs;
    this.maxSleepTimeMs = maxSleepTimeMs;
    this.scaleFactor = scaleFactor;
  }

  @Override
  public long computeBackoff(int attempt) {
    int delayMs =
        (int)
            Math.min(minSleepTimeMs * Math.pow(scaleFactor, attempt - 1), (double) maxSleepTimeMs);
    int jitter = ThreadLocalRandom.current().nextInt(Math.max(1, (int) (delayMs * 0.1)));
    return (long) delayMs + jitter;
  }
}
