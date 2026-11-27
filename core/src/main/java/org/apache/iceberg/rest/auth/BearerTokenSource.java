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
package org.apache.iceberg.rest.auth;

import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A source of bearer tokens for {@link BearerAuthManager}.
 *
 * <p>Implementations must never include {@link #token()} in their {@link Object#toString()}
 * representation.
 */
interface BearerTokenSource {

  /** A pattern that validates the token68 grammar from RFC 9110 section 11.2. */
  Pattern TOKEN68 = Pattern.compile("[A-Za-z0-9\\-._~+/]+=*");

  /** Returns the current token; never null. */
  String token();

  /**
   * Validates a raw token and returns it with leading and trailing whitespace removed.
   *
   * <p>Error messages never include the token itself.
   *
   * @throws IllegalArgumentException if the token is empty or not a valid token68 value
   */
  static String validate(String rawToken) {
    Preconditions.checkArgument(rawToken != null, "Invalid bearer token: null");
    String token = rawToken.strip();
    Preconditions.checkArgument(!token.isEmpty(), "Invalid bearer token: empty");
    Preconditions.checkArgument(
        TOKEN68.matcher(token).matches(),
        "Invalid bearer token: contains characters not allowed by RFC 6750");
    return token;
  }
}
