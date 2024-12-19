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

import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.immutables.value.Value;

/**
 * Default implementation of {@link AuthSession}. It authenticates requests by setting the provided
 * headers on the request.
 *
 * <p>Most {@link AuthManager} implementations should make use of this class, unless they need to
 * retain state when creating sessions, or if they need to modify the request in a different way.
 */
@Value.Style(redactedMask = "****")
@Value.Immutable
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface DefaultAuthSession extends AuthSession {

  /** Headers containing authentication data to set on the request. */
  HTTPHeaders headers();

  @Override
  default HTTPRequest authenticate(HTTPRequest request) {
    HTTPHeaders headers = request.headers().putIfAbsent(headers());
    return headers.equals(request.headers())
        ? request
        : ImmutableHTTPRequest.builder().from(request).headers(headers).build();
  }

  @Override
  default void close() {
    // no resources to close
  }

  static DefaultAuthSession of(HTTPHeaders headers) {
    return ImmutableDefaultAuthSession.builder().headers(headers).build();
  }
}
