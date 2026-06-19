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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.cloud.storage.StorageException;
import java.io.IOException;
import org.apache.iceberg.io.FailureCategory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestGCSErrorInterpreter {

  private static StorageException exception(String reason, int code) {
    return new StorageException(code, "test", reason, null);
  }

  @ParameterizedTest
  @CsvSource({
    "forbidden,AUTH",
    "unauthorized,AUTH",
    "authError,AUTH",
    "notFound,NOT_FOUND",
    "rateLimitExceeded,THROTTLED",
    "userRateLimitExceeded,THROTTLED",
    "quotaExceeded,THROTTLED",
    "backendError,TRANSIENT",
    "internalError,TRANSIENT"
  })
  public void categorizesByReason(String reason, FailureCategory expected) {
    assertThat(GCSErrorInterpreter.categorize(exception(reason, 200))).isEqualTo(expected);
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
  public void fallsBackToStatusWhenReasonMissing(int status, FailureCategory expected) {
    assertThat(GCSErrorInterpreter.categorize(exception(null, status))).isEqualTo(expected);
  }

  @Test
  public void unknownReasonFallsBackToStatus() {
    assertThat(GCSErrorInterpreter.categorize(exception("mysteryReason", 503)))
        .isEqualTo(FailureCategory.TRANSIENT);
    assertThat(GCSErrorInterpreter.categorize(exception("mysteryReason", 418)))
        .isEqualTo(FailureCategory.UNKNOWN);
  }

  @Test
  public void ioExceptionIsTransient() {
    assertThat(GCSErrorInterpreter.categorize(new IOException("connection reset")))
        .isEqualTo(FailureCategory.TRANSIENT);
  }

  @Test
  public void unrelatedThrowableIsUnknown() {
    assertThat(GCSErrorInterpreter.categorize(new RuntimeException("boom")))
        .isEqualTo(FailureCategory.UNKNOWN);
  }

  @Test
  public void rawErrorCodeReturnsReasonForStorageException() {
    assertThat(GCSErrorInterpreter.rawErrorCode(exception("notFound", 404))).isEqualTo("notFound");
    assertThat(GCSErrorInterpreter.rawErrorCode(new IOException("x"))).isNull();
  }
}
