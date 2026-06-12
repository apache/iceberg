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
package org.apache.iceberg.view;

import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;

import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.Tasks.RetryExhaustedException;

class ViewCommitRetry {
  private ViewCommitRetry() {}

  static CommitFailedException toCommitFailedException(
      RetryExhaustedException ex, int numRetries, int totalTimeoutMs) {
    if (shouldPreserveCommitFailure(ex.getCause())) {
      return (CommitFailedException) ex.getCause();
    }

    if (ex.reason() == RetryExhaustedException.Reason.TIMEOUT_EXCEEDED) {
      return new CommitFailedException(
          ex,
          "Commit failed and retry timeout (%d ms) reached. Consider increasing '%s'",
          totalTimeoutMs,
          COMMIT_TOTAL_RETRY_TIME_MS);
    } else {
      return new CommitFailedException(
          ex,
          "Commit failed and retry limit (%d) reached. Consider increasing '%s'",
          numRetries,
          COMMIT_NUM_RETRIES);
    }
  }

  private static boolean shouldPreserveCommitFailure(Throwable cause) {
    if (!(cause instanceof CommitFailedException)) {
      return false;
    }

    String message = cause.getMessage();
    return message == null || !message.startsWith("Commit failed: view was updated");
  }
}
