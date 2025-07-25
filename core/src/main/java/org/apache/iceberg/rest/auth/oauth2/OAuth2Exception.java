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

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/** An exception thrown when the OAuth2 authorization server replies with an error. */
public final class OAuth2Exception extends RuntimeException {

  private final int statusCode;
  private final OAuth2Error error;

  public OAuth2Exception(int statusCode, OAuth2Error error) {
    this("OAuth2 request failed: " + error.description().orElseGet(error::code), statusCode, error);
  }

  public OAuth2Exception(String message, int statusCode, OAuth2Error error) {
    super(message);
    this.statusCode = statusCode;
    this.error = error;
  }

  /** The HTTP status code of the response. */
  public int statusCode() {
    return statusCode;
  }

  /** The error object returned by the server. */
  public OAuth2Error error() {
    return error;
  }

  /**
   * An OAuth2 error returned by the server.
   *
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc6749#section-5.2">RFC 6749 Section
   *     5.2</a>
   * @see com.nimbusds.oauth2.sdk.ErrorObject
   */
  @Value.Immutable
  public interface OAuth2Error {

    /** The OAuth2 error code. */
    String code();

    /** The OAuth2 error description, if any. */
    Optional<String> description();

    /** The OAuth2 error URI, if any. */
    Optional<URI> uri();

    /** The custom parameters in the error, or an empty map if not specified. */
    Map<String, String> parameters();
  }
}
