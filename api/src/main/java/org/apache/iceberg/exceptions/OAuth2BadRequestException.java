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
package org.apache.iceberg.exceptions;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;

/**
 * Bad-request exception raised when an OAuth2 token-endpoint response carries one of the {@code
 * 400}-class error codes from RFC 6749 §5.2 (e.g. {@code invalid_request}, {@code invalid_grant},
 * {@code unauthorized_client}, {@code unsupported_grant_type}, {@code invalid_scope}).
 */
public class OAuth2BadRequestException extends BadRequestException implements OAuth2Error {
  private final String errorType;

  @FormatMethod
  public OAuth2BadRequestException(String errorType, @FormatString String message, Object... args) {
    super(message, args);
    this.errorType = errorType;
  }

  @FormatMethod
  public OAuth2BadRequestException(
      String errorType, Throwable cause, @FormatString String message, Object... args) {
    super(cause, message, args);
    this.errorType = errorType;
  }

  @Override
  public String errorType() {
    return errorType;
  }
}
