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
  static final String RETRYABLE_VALIDATION_FAILURE_PREFIX = "Validation failed, please retry:";
  private static final String REST_RETRYABLE_VALIDATION_FAILURE_PREFIX =
      "Commit failed: " + RETRYABLE_VALIDATION_FAILURE_PREFIX;
  private static final String REQUIREMENT_FAILURE_PREFIX = "Requirement failed:";
  private static final String REST_REQUIREMENT_FAILURE_PREFIX =
      "Commit failed: " + REQUIREMENT_FAILURE_PREFIX;

  private CommitRetryExceptions() {}

  public static CommitFailedException retryExhaustedException(
      Exception cause, Tasks.RetryExhaustionReason reason) {
    if (shouldKeepOriginalCommitFailure(cause)) {
      return (CommitFailedException) cause;
    }

    return new CommitFailedException(
        cause, "Commit failed after exhausting retries. %s", retryExhaustionMessage(reason));
  }

  public static boolean isRetryableValidationCommitFailure(Exception cause) {
    return cause instanceof CommitFailedException
        && cause.getCause() instanceof RetryableValidationException;
  }

  // Keep specific commit failures as the primary error instead of replacing them with retry tuning.
  private static boolean shouldKeepOriginalCommitFailure(Exception cause) {
    return isRetryableValidationCommitFailure(cause)
        || (cause instanceof CommitFailedException
            && (isRestRetryableValidationFailure(cause.getMessage())
                || isRequirementFailureMessage(cause.getMessage())));
  }

  private static boolean isRestRetryableValidationFailure(String message) {
    return message != null && message.startsWith(REST_RETRYABLE_VALIDATION_FAILURE_PREFIX);
  }

  private static boolean isRequirementFailureMessage(String message) {
    return message != null
        && (message.startsWith(REQUIREMENT_FAILURE_PREFIX)
            || message.startsWith(REST_REQUIREMENT_FAILURE_PREFIX));
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
