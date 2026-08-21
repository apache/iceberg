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

import com.google.errorprone.annotations.FormatMethod;
import org.apache.iceberg.RetryableValidationException;
import org.apache.iceberg.exceptions.CommitFailedException;

/** Internal helper for commit failure messages that must be recognized across commit paths. */
public class CommitFailureMessages {
  public static final String REQUIREMENT_FAILURE_PREFIX = "Requirement failed:";
  public static final String RETRYABLE_VALIDATION_FAILURE_PREFIX =
      "Validation failed, please retry:";

  private static final String REST_COMMIT_FAILURE_PREFIX = "Commit failed: ";

  private CommitFailureMessages() {}

  @FormatMethod
  public static CommitFailedException requirementFailure(String message, Object... args) {
    return new CommitFailedException(
        "%s %s", REQUIREMENT_FAILURE_PREFIX, String.format(message, args));
  }

  public static CommitFailedException retryableValidationFailure(
      RetryableValidationException cause) {
    return new CommitFailedException(
        cause, "%s %s", RETRYABLE_VALIDATION_FAILURE_PREFIX, cause.getMessage());
  }

  public static boolean isRequirementFailure(String message) {
    return startsWithCommitFailure(message, REQUIREMENT_FAILURE_PREFIX);
  }

  public static boolean isRestRetryableValidationFailure(String message) {
    return startsWithCommitFailure(message, RETRYABLE_VALIDATION_FAILURE_PREFIX);
  }

  private static boolean startsWithCommitFailure(String message, String prefix) {
    return message != null
        && (message.startsWith(prefix) || message.startsWith(REST_COMMIT_FAILURE_PREFIX + prefix));
  }
}
