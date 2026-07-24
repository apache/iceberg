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
package org.apache.iceberg.aws.s3;

import javax.annotation.Nullable;
import org.apache.iceberg.io.FailureCategory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Maps S3 partial-batch errors and SDK exceptions to a {@link FailureCategory}.
 *
 * <p>This is the cloud-specific half of the bulk-delete failure reporting contract: it converts
 * native S3 error codes into the cloud-agnostic categories that callers consume via {@code
 * FailureHandler}.
 *
 * <p>The {@code categorize(Throwable)} overload inspects the throwable directly and does not walk
 * the cause chain. Callers that wrap SDK exceptions (e.g. via a custom client supplier or async
 * client returning {@code CompletionException}) should unwrap to the underlying SDK exception
 * before calling.
 */
final class S3ErrorInterpreter {
  private S3ErrorInterpreter() {}

  /** Categorize a per-object error from a {@code DeleteObjects} response. */
  static FailureCategory categorize(@Nullable S3Error error) {
    if (error == null || error.code() == null) {
      return FailureCategory.UNKNOWN;
    }
    return categorizeByCode(error.code());
  }

  /** Categorize a thrown exception from a bulk-delete request. */
  static FailureCategory categorize(Throwable throwable) {
    if (throwable instanceof S3Exception) {
      S3Exception s3e = (S3Exception) throwable;
      String code = s3e.awsErrorDetails() != null ? s3e.awsErrorDetails().errorCode() : null;
      if (code != null) {
        return categorizeByCode(code);
      }
      return FailureCategory.fromHttpStatus(s3e.statusCode());
    }
    if (throwable instanceof AwsServiceException ase) {
      if (ase.isThrottlingException()) {
        return FailureCategory.THROTTLED;
      }
      return FailureCategory.fromHttpStatus(ase.statusCode());
    }
    if (throwable instanceof SdkClientException) {
      // Client-side: network timeouts, connection resets, DNS, etc.
      return FailureCategory.TRANSIENT;
    }
    return FailureCategory.UNKNOWN;
  }

  /** Cloud-native error code for a thrown exception, or null if unavailable. */
  @Nullable
  static String rawErrorCode(Throwable throwable) {
    if (throwable instanceof S3Exception) {
      S3Exception s3e = (S3Exception) throwable;
      return s3e.awsErrorDetails() != null ? s3e.awsErrorDetails().errorCode() : null;
    }
    return null;
  }

  /**
   * Maps a curated subset of S3 error codes to a {@link FailureCategory}.
   *
   * <p>Error codes are free-form strings in the AWS SDK (no enum). The authoritative list of codes
   * is the S3 API error reference; unrecognized codes fall through to {@link
   * FailureCategory#UNKNOWN} and are then classified by HTTP status.
   *
   * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html">S3 Error
   *     Responses</a>
   */
  private static FailureCategory categorizeByCode(String code) {
    return switch (code) {
      case "AccessDenied",
              "AllAccessDisabled",
              "InvalidAccessKeyId",
              "SignatureDoesNotMatch",
              "TokenRefreshRequired",
              "ExpiredToken",
              "InvalidToken" ->
          FailureCategory.AUTH;
      case "NoSuchKey", "NoSuchBucket", "NoSuchVersion" -> FailureCategory.NOT_FOUND;
      case "SlowDown",
              "RequestLimitExceeded",
              "TooManyRequests",
              "Throttling",
              "ThrottlingException" ->
          FailureCategory.THROTTLED;
      case "InternalError", "ServiceUnavailable", "RequestTimeout", "RequestTimeTooSkewed" ->
          FailureCategory.TRANSIENT;
      default -> FailureCategory.UNKNOWN;
    };
  }
}
