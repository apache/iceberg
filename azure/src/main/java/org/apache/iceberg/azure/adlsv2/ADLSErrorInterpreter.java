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

import com.azure.storage.file.datalake.models.DataLakeStorageException;
import java.io.IOException;
import org.apache.iceberg.io.FailureCategory;

/**
 * Maps ADLS Gen2 / Blob storage exceptions to a {@link FailureCategory}.
 *
 * <p>The throwable is inspected directly; the cause chain is not walked. Callers that wrap SDK
 * exceptions (e.g. via a custom client supplier) should unwrap before calling.
 */
final class ADLSErrorInterpreter {
  private ADLSErrorInterpreter() {}

  static FailureCategory categorize(Throwable throwable) {
    if (throwable instanceof DataLakeStorageException) {
      DataLakeStorageException dse = (DataLakeStorageException) throwable;
      String code = dse.getErrorCode();
      FailureCategory byCode = code != null ? categorizeByCode(code) : FailureCategory.UNKNOWN;
      if (byCode != FailureCategory.UNKNOWN) {
        return byCode;
      }
      return FailureCategory.fromHttpStatus(dse.getStatusCode());
    }
    if (throwable instanceof IOException) {
      return FailureCategory.TRANSIENT;
    }
    return FailureCategory.UNKNOWN;
  }

  static String rawErrorCode(Throwable throwable) {
    if (throwable instanceof DataLakeStorageException) {
      return ((DataLakeStorageException) throwable).getErrorCode();
    }
    return null;
  }

  /**
   * Maps a curated subset of ADLS Gen2 / Blob storage error codes to a {@link FailureCategory}.
   *
   * <p>Error codes are free-form strings ({@code DataLakeStorageException.getErrorCode()}); the
   * datalake SDK ships no error-code enum. Unrecognized codes fall through to {@link
   * FailureCategory#UNKNOWN} and are then classified by HTTP status.
   *
   * @see <a
   *     href="https://learn.microsoft.com/rest/api/storageservices/datalakestorageerrorcodes">Data
   *     Lake Storage error codes</a>
   * @see <a
   *     href="https://learn.microsoft.com/rest/api/storageservices/common-rest-api-error-codes">Common
   *     REST API error codes</a>
   */
  private static FailureCategory categorizeByCode(String code) {
    return switch (code) {
      case "AuthenticationFailed",
              "AuthorizationFailure",
              "AuthorizationPermissionMismatch",
              "InsufficientAccountPermissions",
              "InvalidAuthenticationInfo",
              "AccountIsDisabled" ->
          FailureCategory.AUTH;
      case "PathNotFound",
              "BlobNotFound",
              "ContainerNotFound",
              "FilesystemNotFound",
              "ResourceNotFound" ->
          FailureCategory.NOT_FOUND;
      case "ServerBusy", "TooManyRequests", "OperationTimedOut" -> FailureCategory.THROTTLED;
      case "InternalError", "ServiceUnavailable" -> FailureCategory.TRANSIENT;
      default -> FailureCategory.UNKNOWN;
    };
  }
}
