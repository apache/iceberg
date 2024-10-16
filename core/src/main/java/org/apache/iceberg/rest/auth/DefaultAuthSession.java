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

import java.util.Map;
import org.apache.iceberg.rest.HTTPRequest;
import org.immutables.value.Value;

/**
 * Default implementation of {@link AuthSession} that sets the provided headers on the request.
 *
 * <p>Most {@link AuthManager} implementations should make use of this class, unless they need to
 * retain state when creating sessions.
 */
@Value.Style(redactedMask = "****")
@SuppressWarnings("ImmutablesStyle")
@Value.Immutable
public interface DefaultAuthSession extends AuthSession {

  /** Headers containing authentication data to set on the request. */
  @Value.Redacted
  Map<String, String> headers();

  @Override
  default void authenticate(HTTPRequest request) {
    headers().forEach(request::setHeader);
  }

  static DefaultAuthSession empty() {
    return ImmutableDefaultAuthSession.builder().build();
  }

  static DefaultAuthSession of(String name, String value) {
    return ImmutableDefaultAuthSession.builder().putHeaders(name, value).build();
  }

  static DefaultAuthSession of(Map<String, String> authHeaders) {
    return ImmutableDefaultAuthSession.builder().putAllHeaders(authHeaders).build();
  }
}
