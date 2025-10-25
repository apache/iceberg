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

import javax.annotation.Nullable;
import org.apache.iceberg.rest.HTTPChallenge;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.RESTClient;

/**
 * An authentication session that can be used to authenticate outgoing HTTP requests.
 *
 * <p>Authentication sessions are usually immutable, but may hold resources that need to be released
 * when the session is no longer needed. Implementations should override {@link #close()} to release
 * any resources.
 */
public interface AuthSession extends AutoCloseable {

  /** An empty session that does nothing. */
  AuthSession EMPTY =
      new AuthSession() {
        @Override
        public HTTPRequest authenticate(HTTPRequest request) {
          return request;
        }

        @Override
        public void close() {}
      };

  /**
   * Authenticates the given request and returns a new request with the necessary authentication.
   */
  HTTPRequest authenticate(HTTPRequest request);

  /**
   * Called when the request was challenged (the server returned a 401 response).
   *
   * <p>Implementations may choose to return a new request with updated authentication data, or null
   * if the request should not be retried. The default implementation returns null.
   *
   * <p>If this method returns null, the 401 response will be surfaced to the caller as a {@link
   * org.apache.iceberg.exceptions.NotAuthorizedException}.
   *
   * @param restClient the REST client that sent the request
   * @param request the original request that caused the authentication failure
   * @param challenge the authentication challenge
   * @param retryAttempt the retry attempt number, starting with 1
   * @return a new request with updated authentication headers, or null if the request should not be
   *     retried
   * @see <a href="https://datatracker.ietf.org/doc/html/rfc7235#section-2.1">RFC 7235 Section
   *     2.1</a>
   */
  @Nullable
  default HTTPRequest processChallenge(
      RESTClient restClient, HTTPRequest request, HTTPChallenge challenge, int retryAttempt) {
    return null;
  }

  /**
   * Closes the session and releases any resources. This method is called when the session is no
   * longer needed. Note that since sessions may be cached, this method may not be called
   * immediately after the session is no longer needed, but rather when the session is evicted from
   * the cache, or the cache itself is closed.
   */
  @Override
  void close();
}
