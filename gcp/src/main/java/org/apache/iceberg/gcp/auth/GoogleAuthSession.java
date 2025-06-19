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
package org.apache.iceberg.gcp.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.auth.AuthSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An authentication session that uses Google Credentials (typically Application Default
 * Credentials) to obtain an OAuth2 access token and add it to HTTP requests.
 */
class GoogleAuthSession implements AuthSession {
  private static final Logger LOG = LoggerFactory.getLogger(GoogleAuthSession.class);
  private final GoogleCredentials credentials;

  /**
   * Constructs a GoogleAuthSession with the provided GoogleCredentials.
   *
   * @param credentials The GoogleCredentials to use for authentication.
   */
  GoogleAuthSession(GoogleCredentials credentials) {
    Preconditions.checkArgument(credentials != null, "Invalid credentials: null");
    this.credentials = credentials;
  }

  /**
   * Authenticates the given HTTP request by adding an "Authorization: Bearer token" header. The
   * access token is obtained from the GoogleCredentials.
   *
   * @param request The HTTPRequest to authenticate.
   * @return A new HTTPRequest with the added Authorization header.
   * @throws UncheckedIOException if an IOException occurs while refreshing the access token.
   */
  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    try {
      credentials.refreshIfExpired();
      AccessToken token = credentials.getAccessToken();

      if (token != null && token.getTokenValue() != null) {
        HTTPHeaders newHeaders =
            request
                .headers()
                .putIfAbsent(
                    HTTPHeaders.of(
                        HTTPHeaders.HTTPHeader.of(
                            "Authorization", "Bearer " + token.getTokenValue())));
        return newHeaders.equals(request.headers())
            ? request
            : ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
      } else {
        throw new IllegalStateException(
            "Failed to obtain Google access token. Cannot authenticate request.");
      }
    } catch (IOException e) {
      LOG.error("IOException while trying to refresh Google access token", e);
      throw new UncheckedIOException("Failed to refresh Google access token", e);
    }
  }

  /**
   * Closes the session. This is a no-op for GoogleAuthSession as the lifecycle of GoogleCredentials
   * is not managed by this session.
   */
  @Override
  public void close() {
    // No-op
  }
}
