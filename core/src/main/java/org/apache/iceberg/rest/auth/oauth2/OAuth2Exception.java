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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.ErrorObject;
import java.util.Locale;

/**
 * Thrown when the authorization server replies to a token request with an error.
 *
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.2">RFC 6749 Section 5.2</a>
 */
final class OAuth2Exception extends RuntimeException {

  private final int statusCode;
  private final ErrorObject error;

  OAuth2Exception(int statusCode, ErrorObject error) {
    super(
        String.format(
            Locale.ROOT,
            "OAuth2 token request failed with status %d: %s",
            statusCode,
            error.getDescription() != null ? error.getDescription() : errorCode(error)));
    this.statusCode = statusCode;
    this.error = error;
  }

  /** The HTTP status code of the error response. */
  int statusCode() {
    return statusCode;
  }

  /** The error code, or {@code unknown_error} if the response did not contain one. */
  String errorCode() {
    return errorCode(error);
  }

  /** The error returned by the server. */
  ErrorObject error() {
    return error;
  }

  private static String errorCode(ErrorObject error) {
    // the code is null if the error is not a standard OAuth2 error, e.g. a 404 Not Found
    return error.getCode() != null ? error.getCode() : "unknown_error";
  }
}
