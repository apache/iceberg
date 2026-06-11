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

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks;

public class CommitRetry {
  public enum RetryExhaustionReason {
    ATTEMPT_LIMIT,
    TIMEOUT,
    ATTEMPT_LIMIT_AND_TIMEOUT
  }

  private CommitRetry() {}

  public static CommitFailedException retryExhaustedException(RetryExhaustedException exhausted) {
    return new CommitFailedException(
        exhausted,
        "Commit failed after exhausting retry budget (%s). %s",
        exhausted.getMessage(),
        recommendation(exhausted.reason()));
  }

  @SuppressWarnings("StatementSwitchToExpressionSwitch")
  private static String recommendation(RetryExhaustionReason reason) {
    switch (reason) {
      case ATTEMPT_LIMIT:
        return String.format("To allow more retry attempts, adjust %s.", COMMIT_NUM_RETRIES);

      case TIMEOUT:
        return String.format(
            "To allow more total retry time, adjust %s.", COMMIT_TOTAL_RETRY_TIME_MS);

      case ATTEMPT_LIMIT_AND_TIMEOUT:
        return String.format(
            "To allow more retry attempts and total retry time, adjust %s and %s.",
            COMMIT_NUM_RETRIES, COMMIT_TOTAL_RETRY_TIME_MS);
    }

    throw new IllegalArgumentException("Unknown retry exhaustion reason: " + reason);
  }


  public static RetryExhaustionReason retryExhaustionReason(
          boolean attemptsExhausted, boolean timeoutExhausted) {
    if (attemptsExhausted && timeoutExhausted) {
      return RetryExhaustionReason.ATTEMPT_LIMIT_AND_TIMEOUT;
    } else if (attemptsExhausted) {
      return RetryExhaustionReason.ATTEMPT_LIMIT;
    } else {
      return RetryExhaustionReason.TIMEOUT;
    }
  }

  public static class RetryExhaustedException extends RuntimeException {
    private final RetryExhaustionReason reason;
    private final int attempts;
    private final int maxAttempts;
    private final long durationMs;
    private final long maxDurationMs;

    public RetryExhaustedException(
            RetryExhaustionReason reason,
            int attempts,
            int maxAttempts,
            long durationMs,
            long maxDurationMs,
            Exception cause) {
      super(
              String.format(
                      "Retry exhausted: reason=%s, attempts=%s, max-attempts=%s, duration-ms=%s, "
                              + "max-duration-ms=%s",
                      reason, attempts, maxAttempts, durationMs, maxDurationMs),
              cause);
      this.reason = reason;
      this.attempts = attempts;
      this.maxAttempts = maxAttempts;
      this.durationMs = durationMs;
      this.maxDurationMs = maxDurationMs;
    }

    public RetryExhaustionReason reason() {
      return reason;
    }

    public int attempts() {
      return attempts;
    }

    public int maxAttempts() {
      return maxAttempts;
    }

    public long durationMs() {
      return durationMs;
    }

    public long maxDurationMs() {
      return maxDurationMs;
    }
  }
}
