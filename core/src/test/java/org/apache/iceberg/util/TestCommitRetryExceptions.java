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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.jupiter.api.Test;

class TestCommitRetryExceptions {

  @Test
  void retryExhaustedExceptionRecommendsNumRetriesForAttemptLimit() {
    CommitFailedException original = new CommitFailedException("failed");

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);

    assertThat(wrapped).isInstanceOf(CommitFailedException.class);
    assertThat(wrapped).hasMessageContaining(TableProperties.COMMIT_NUM_RETRIES);
    assertThat(wrapped).hasMessageNotContaining(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS);
    assertThat(wrapped.getCause()).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionRecommendsTotalTimeoutForTimeout() {
    CommitFailedException original = new CommitFailedException("failed");

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.TIMEOUT);

    assertThat(wrapped).isInstanceOf(CommitFailedException.class);
    assertThat(wrapped).hasMessageContaining(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS);
    assertThat(wrapped).hasMessageNotContaining(TableProperties.COMMIT_NUM_RETRIES);
    assertThat(wrapped.getCause()).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionRecommendsBothPropertiesForAttemptLimitAndTimeout() {
    CommitFailedException original = new CommitFailedException("failed");

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT_AND_TIMEOUT);

    assertThat(wrapped).isInstanceOf(CommitFailedException.class);
    assertThat(wrapped)
        .hasMessageContaining(TableProperties.COMMIT_NUM_RETRIES)
        .hasMessageContaining(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS);
    assertThat(wrapped.getCause()).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionPreservesRetryableValidationFailures() {
    CommitFailedException original =
        new CommitFailedException(
            new RetryableValidationException("stale values"),
            "Commit failed: %s stale values",
            CommitRetryExceptions.RETRYABLE_VALIDATION_FAILURE_PREFIX);

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);

    assertThat(wrapped).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionPreservesRestRetryableValidationMessages() {
    CommitFailedException original =
        new CommitFailedException(
            "Commit failed: %s stale values",
            CommitRetryExceptions.RETRYABLE_VALIDATION_FAILURE_PREFIX);

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);

    assertThat(wrapped).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionKeepsRequirementFailureAsPrimaryError() {
    CommitFailedException original =
        new CommitFailedException(
            "Requirement failed: last assigned field id changed: expected id 2 != 3");

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);

    assertThat(wrapped).isSameAs(original);
  }

  @Test
  void retryExhaustedExceptionKeepsRestRequirementFailureAsPrimaryError() {
    String message =
        "Commit failed: Requirement failed: last assigned field id changed: expected id %d != %d";
    CommitFailedException original = new CommitFailedException(message, 2, 3);

    CommitFailedException wrapped =
        CommitRetryExceptions.retryExhaustedException(
            original, Tasks.RetryExhaustionReason.ATTEMPT_LIMIT);

    assertThat(wrapped).isSameAs(original);
  }
}
