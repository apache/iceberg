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
package org.apache.iceberg.io;

/**
 * Best-effort classification of a per-file failure during bulk file operations.
 *
 * <p>FileIO implementations translate native cloud error codes into one of these categories so
 * callers can make retry / alerting decisions without parsing cloud-specific errors. The
 * categorization is advisory; callers that disagree can inspect the raw error code or cause on
 * {@link FileFailure} and override the decision.
 */
public enum FailureCategory {
  /** Authentication or authorization failure. Generally not retryable without intervention. */
  AUTH,
  /** Target object did not exist. Typically safe to treat as success for deletes. */
  NOT_FOUND,
  /** Server-side throttling or rate limiting. Retryable with backoff. */
  THROTTLED,
  /** Transient I/O or server error (timeouts, 5xx). Retryable. */
  TRANSIENT,
  /** Category could not be determined. Caller should inspect the cause. */
  UNKNOWN;

  /**
   * Classifies an HTTP status code using semantics common to object stores.
   *
   * <p>FileIO implementations should prefer cloud-specific error codes where available and fall
   * back to this mapping when only a status code is known.
   *
   * @param status an HTTP status code
   * @return the matching category, or {@link #UNKNOWN} if the status has no mapping
   */
  public static FailureCategory fromHttpStatus(int status) {
    if (status == 401 || status == 403) {
      return AUTH;
    }
    if (status == 404) {
      return NOT_FOUND;
    }
    if (status == 429) {
      return THROTTLED;
    }
    if (status >= 500 && status < 600) {
      return TRANSIENT;
    }
    return UNKNOWN;
  }
}
