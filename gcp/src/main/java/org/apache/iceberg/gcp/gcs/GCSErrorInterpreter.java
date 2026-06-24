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
package org.apache.iceberg.gcp.gcs;

import com.google.cloud.storage.StorageException;
import java.io.IOException;
import org.apache.iceberg.io.FailureCategory;

/**
 * Maps Google Cloud Storage exceptions to a {@link FailureCategory}.
 *
 * <p>The throwable is inspected directly; the cause chain is not walked. Callers that wrap SDK
 * exceptions (e.g. via a custom client supplier) should unwrap before calling.
 */
final class GCSErrorInterpreter {
  private GCSErrorInterpreter() {}

  static FailureCategory categorize(Throwable throwable) {
    if (throwable instanceof StorageException) {
      StorageException se = (StorageException) throwable;
      String reason = se.getReason();
      FailureCategory byReason =
          reason != null ? categorizeByReason(reason) : FailureCategory.UNKNOWN;
      if (byReason != FailureCategory.UNKNOWN) {
        return byReason;
      }
      return FailureCategory.fromHttpStatus(se.getCode());
    }
    if (throwable instanceof IOException) {
      return FailureCategory.TRANSIENT;
    }
    return FailureCategory.UNKNOWN;
  }

  static String rawErrorCode(Throwable throwable) {
    if (throwable instanceof StorageException) {
      return ((StorageException) throwable).getReason();
    }
    return null;
  }

  /**
   * Maps a curated subset of GCS JSON API error reasons to a {@link FailureCategory}.
   *
   * <p>Reasons are free-form strings from the JSON API {@code error.errors[].reason} field ({@code
   * StorageException.getReason()}); there is no enum. Unrecognized reasons fall through to {@link
   * FailureCategory#UNKNOWN} and are then classified by HTTP status.
   *
   * @see <a href="https://cloud.google.com/storage/docs/json_api/v1/status-codes">GCS JSON API
   *     status codes</a>
   */
  private static FailureCategory categorizeByReason(String reason) {
    return switch (reason) {
      case "forbidden", "unauthorized", "authError" -> FailureCategory.AUTH;
      case "notFound" -> FailureCategory.NOT_FOUND;
      case "rateLimitExceeded", "userRateLimitExceeded", "quotaExceeded" ->
          FailureCategory.THROTTLED;
      case "backendError", "internalError" -> FailureCategory.TRANSIENT;
      default -> FailureCategory.UNKNOWN;
    };
  }
}
