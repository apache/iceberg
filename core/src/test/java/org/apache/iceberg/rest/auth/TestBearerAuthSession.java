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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.junit.jupiter.api.Test;

class TestBearerAuthSession {

  private static final HTTPRequest REQUEST =
      ImmutableHTTPRequest.builder()
          .method(HTTPMethod.GET)
          .baseUri(URI.create("https://localhost"))
          .path("path")
          .build();

  @Test
  void authenticate() {
    try (BearerAuthSession session = new BearerAuthSession(new StaticTokenSource("token"))) {
      assertThat(session.authenticate(REQUEST).headers().entries())
          .singleElement()
          .extracting(HTTPHeader::name, HTTPHeader::value)
          .containsExactly("Authorization", "Bearer token");
    }
  }

  @Test
  void authenticatePreservesOtherHeaders() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .from(REQUEST)
            .headers(HTTPHeaders.of(HTTPHeader.of("X-Custom", "value")))
            .build();
    try (BearerAuthSession session = new BearerAuthSession(new StaticTokenSource("token"))) {
      assertThat(session.authenticate(request).headers().entries())
          .extracting(HTTPHeader::name, HTTPHeader::value)
          .containsExactlyInAnyOrder(
              tuple("X-Custom", "value"), tuple("Authorization", "Bearer token"));
    }
  }

  @Test
  void authenticateWithExistingAuthorizationHeader() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .from(REQUEST)
            .headers(HTTPHeaders.of(HTTPHeader.of("Authorization", "Basic dXNlcjpwYXNz")))
            .build();
    try (BearerAuthSession session = new BearerAuthSession(new StaticTokenSource("token"))) {
      assertThat(session.authenticate(request)).isSameAs(request);
    }
  }

  @Test
  void authenticateFetchesTokenOnEveryRequest() {
    AtomicReference<String> token = new AtomicReference<>("token1");
    try (BearerAuthSession session = new BearerAuthSession(token::get)) {
      assertThat(session.authenticate(REQUEST).headers().firstEntry("Authorization"))
          .get()
          .extracting(HTTPHeader::value)
          .isEqualTo("Bearer token1");

      token.set("token2");
      assertThat(session.authenticate(REQUEST).headers().firstEntry("Authorization"))
          .get()
          .extracting(HTTPHeader::value)
          .isEqualTo("Bearer token2");
    }
  }

  @Test
  void toStringDoesNotContainToken() {
    try (BearerAuthSession session = new BearerAuthSession(new StaticTokenSource("s3cr3t"))) {
      assertThat(session.toString()).doesNotContain("s3cr3t");
    }
  }
}
