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

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;

import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.exceptions.CommitFailedException;

/** Internal helper for converting exhausted commit retries into user-facing commit exceptions. */
public class CommitRetryExceptions {
  private CommitRetryExceptions() {}

  public static CommitFailedException retryExhaustedException(
      Exception cause, Tasks.RetryExhaustionReason reason) {
    if (isRetryableValidationCommitFailure(cause)) {
      return (CommitFailedException) cause;
    }

    return new CommitFailedException(
        cause, "Commit failed after exhausting retries. %s", retryExhaustionMessage(reason));
  }

  public static boolean isRetryableValidationCommitFailure(Exception cause) {
    return cause instanceof CommitFailedException
        && cause.getCause() instanceof RetryableValidationException;
  }

  private static String retryExhaustionMessage(Tasks.RetryExhaustionReason reason) {
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
}
