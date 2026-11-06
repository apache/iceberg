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

import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;

/**
 * An {@link AuthSession} that sets the {@code Authorization: Bearer} header on requests, using the
 * current token of a {@link BearerTokenSource}. The token is fetched from the source on every
 * request.
 */
final class BearerAuthSession implements AuthSession {

  private final BearerTokenSource source;

  BearerAuthSession(BearerTokenSource source) {
    this.source = source;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    HTTPHeaders newHeaders =
        request.headers().putIfAbsent(HTTPHeader.of("Authorization", "Bearer " + source.token()));
    return newHeaders.equals(request.headers())
        ? request
        : ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
  }

  @Override
  public void close() {
    // no-op: by design, token sources hold no resources
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("source", source).toString();
  }
}
