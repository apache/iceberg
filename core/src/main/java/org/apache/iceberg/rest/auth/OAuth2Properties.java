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

public class OAuth2Properties {
  private OAuth2Properties() {}

  /** A Bearer token which will be used for interaction with the server. */
  public static final String TOKEN = "token";

  /** A credential to exchange for a token in the OAuth2 client credentials flow. */
  public static final String CREDENTIAL = "credential";

  /** Token endpoint URI to fetch token from if the Rest Catalog is not the authorization server. */
  public static final String OAUTH2_SERVER_URI = "oauth2-server-uri";

  /**
   * Interval in milliseconds to wait before attempting to exchange the configured catalog Bearer
   * token. By default, token exchange will be attempted after 1 hour.
   */
  public static final String TOKEN_EXPIRES_IN_MS = "token-expires-in-ms";

  public static final long TOKEN_EXPIRES_IN_MS_DEFAULT = 3_600_000; // 1 hour

  /**
   * Controls whether a token should be refreshed if information about its expiration time is
   * available
   */
  public static final String TOKEN_REFRESH_ENABLED = "token-refresh-enabled";

  public static final boolean TOKEN_REFRESH_ENABLED_DEFAULT = true;

  /** Additional scope for OAuth2. */
  public static final String SCOPE = "scope";

  /** Optional param audience for OAuth2. */
  public static final String AUDIENCE = "audience";

  /** Optional param audience for OAuth2. */
  public static final String RESOURCE = "resource";

  /** Scope for OAuth2 flows. */
  public static final String CATALOG_SCOPE = "catalog";

  // token type constants
  public static final String ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token";
  public static final String REFRESH_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:refresh_token";
  public static final String ID_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:id_token";
  public static final String SAML1_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:saml1";
  public static final String SAML2_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:saml2";
  public static final String JWT_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:jwt";

  // error type constants
  public static final String INVALID_REQUEST_ERROR = "invalid_request";
  public static final String INVALID_CLIENT_ERROR = "invalid_client";
  public static final String INVALID_GRANT_ERROR = "invalid_grant";
  public static final String UNAUTHORIZED_CLIENT_ERROR = "unauthorized_client";
  public static final String UNSUPPORTED_GRANT_TYPE_ERROR = "unsupported_grant_type";
  public static final String INVALID_SCOPE_ERROR = "invalid_scope";
}
