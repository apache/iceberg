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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.io.FailureCategory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestS3ErrorInterpreter {

  @ParameterizedTest
  @CsvSource({
    "AccessDenied,AUTH",
    "AllAccessDisabled,AUTH",
    "InvalidAccessKeyId,AUTH",
    "SignatureDoesNotMatch,AUTH",
    "TokenRefreshRequired,AUTH",
    "ExpiredToken,AUTH",
    "InvalidToken,AUTH",
    "NoSuchKey,NOT_FOUND",
    "NoSuchBucket,NOT_FOUND",
    "NoSuchVersion,NOT_FOUND",
    "SlowDown,THROTTLED",
    "RequestLimitExceeded,THROTTLED",
    "TooManyRequests,THROTTLED",
    "Throttling,THROTTLED",
    "ThrottlingException,THROTTLED",
    "InternalError,TRANSIENT",
    "ServiceUnavailable,TRANSIENT",
    "RequestTimeout,TRANSIENT",
    "RequestTimeTooSkewed,TRANSIENT",
    "SomeUnknownCode,UNKNOWN"
  })
  public void categorizesS3ErrorByCode(String code, FailureCategory expected) {
    S3Error error = S3Error.builder().code(code).key("k").build();
    assertThat(S3ErrorInterpreter.categorize(error)).isEqualTo(expected);
  }

  @Test
  public void s3ErrorWithNullCodeIsUnknown() {
    assertThat(S3ErrorInterpreter.categorize((S3Error) null)).isEqualTo(FailureCategory.UNKNOWN);
    assertThat(S3ErrorInterpreter.categorize(S3Error.builder().key("k").build()))
        .isEqualTo(FailureCategory.UNKNOWN);
  }

  @ParameterizedTest
  @CsvSource({
    "AccessDenied,AUTH",
    "NoSuchKey,NOT_FOUND",
    "SlowDown,THROTTLED",
    "InternalError,TRANSIENT"
  })
  public void categorizesS3ExceptionByErrorCode(String code, FailureCategory expected) {
    S3Exception ex =
        (S3Exception)
            S3Exception.builder()
                .awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build())
                .build();
    assertThat(S3ErrorInterpreter.categorize((Throwable) ex)).isEqualTo(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "401,AUTH",
    "403,AUTH",
    "404,NOT_FOUND",
    "429,THROTTLED",
    "500,TRANSIENT",
    "503,TRANSIENT",
    "418,UNKNOWN"
  })
  public void s3ExceptionWithoutCodeFallsBackToStatus(int status, FailureCategory expected) {
    S3Exception ex = (S3Exception) S3Exception.builder().statusCode(status).build();
    assertThat(S3ErrorInterpreter.categorize((Throwable) ex)).isEqualTo(expected);
  }

  @Test
  public void awsThrottlingExceptionMapsToThrottled() {
    AwsServiceException throttled =
        AwsServiceException.builder()
            .awsErrorDetails(AwsErrorDetails.builder().errorCode("ThrottlingException").build())
            .statusCode(400)
            .build();
    assertThat(throttled.isThrottlingException()).isTrue();
    assertThat(S3ErrorInterpreter.categorize((Throwable) throttled))
        .isEqualTo(FailureCategory.THROTTLED);
  }

  @Test
  public void awsServiceExceptionFallsBackToStatus() {
    AwsServiceException ex = AwsServiceException.builder().statusCode(503).build();
    assertThat(S3ErrorInterpreter.categorize((Throwable) ex)).isEqualTo(FailureCategory.TRANSIENT);
  }

  @Test
  public void sdkClientExceptionIsTransient() {
    SdkClientException ex = SdkClientException.builder().message("connection reset").build();
    assertThat(S3ErrorInterpreter.categorize((Throwable) ex)).isEqualTo(FailureCategory.TRANSIENT);
  }

  @Test
  public void unrelatedThrowableIsUnknown() {
    assertThat(S3ErrorInterpreter.categorize(new IOException("boom")))
        .isEqualTo(FailureCategory.UNKNOWN);
    assertThat(S3ErrorInterpreter.categorize(new RuntimeException("boom")))
        .isEqualTo(FailureCategory.UNKNOWN);
  }
}
