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
package org.apache.iceberg.azure.adlsv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpResponse;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.IOException;
import org.apache.iceberg.io.FailureCategory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestADLSErrorInterpreter {

  private static final String ERROR_CODE_HEADER = "x-ms-error-code";

  private static DataLakeStorageException exception(String errorCode, int statusCode) {
    HttpHeaders headers = new HttpHeaders();
    if (errorCode != null) {
      headers.set(ERROR_CODE_HEADER, errorCode);
    }
    HttpResponse response = mock(HttpResponse.class);
    when(response.getHeaders()).thenReturn(headers);
    when(response.getStatusCode()).thenReturn(statusCode);
    return new DataLakeStorageException("test", response, null);
  }

  @ParameterizedTest
  @CsvSource({
    "AuthenticationFailed,AUTH",
    "AuthorizationFailure,AUTH",
    "AuthorizationPermissionMismatch,AUTH",
    "InsufficientAccountPermissions,AUTH",
    "InvalidAuthenticationInfo,AUTH",
    "AccountIsDisabled,AUTH",
    "PathNotFound,NOT_FOUND",
    "BlobNotFound,NOT_FOUND",
    "ContainerNotFound,NOT_FOUND",
    "FilesystemNotFound,NOT_FOUND",
    "ResourceNotFound,NOT_FOUND",
    "ServerBusy,THROTTLED",
    "TooManyRequests,THROTTLED",
    "OperationTimedOut,THROTTLED",
    "InternalError,TRANSIENT",
    "ServiceUnavailable,TRANSIENT"
  })
  public void categorizesByErrorCode(String code, FailureCategory expected) {
    assertThat(ADLSErrorInterpreter.categorize(exception(code, 200))).isEqualTo(expected);
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
  public void fallsBackToStatusWhenErrorCodeMissing(int status, FailureCategory expected) {
    assertThat(ADLSErrorInterpreter.categorize(exception(null, status))).isEqualTo(expected);
  }

  @Test
  public void unknownErrorCodeFallsBackToStatus() {
    assertThat(ADLSErrorInterpreter.categorize(exception("MysteryCode", 503)))
        .isEqualTo(FailureCategory.TRANSIENT);
    assertThat(ADLSErrorInterpreter.categorize(exception("MysteryCode", 418)))
        .isEqualTo(FailureCategory.UNKNOWN);
  }

  @Test
  public void ioExceptionIsTransient() {
    assertThat(ADLSErrorInterpreter.categorize(new IOException("connection reset")))
        .isEqualTo(FailureCategory.TRANSIENT);
  }

  @Test
  public void unrelatedThrowableIsUnknown() {
    assertThat(ADLSErrorInterpreter.categorize(new RuntimeException("boom")))
        .isEqualTo(FailureCategory.UNKNOWN);
  }

  @Test
  public void rawErrorCodeReturnsCodeForDataLakeException() {
    assertThat(ADLSErrorInterpreter.rawErrorCode(exception("PathNotFound", 404)))
        .isEqualTo("PathNotFound");
    assertThat(ADLSErrorInterpreter.rawErrorCode(new IOException("x"))).isNull();
  }
}
