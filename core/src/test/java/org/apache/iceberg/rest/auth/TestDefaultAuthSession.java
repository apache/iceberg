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

import java.net.URI;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.junit.jupiter.api.Test;

class TestDefaultAuthSession {

  @Test
  void authenticate() {
    try (DefaultAuthSession session =
        DefaultAuthSession.of(HTTPHeaders.of(HTTPHeader.of("Authorization", "s3cr3t")))) {

      HTTPRequest original =
          ImmutableHTTPRequest.builder()
              .method(HTTPMethod.GET)
              .baseUri(URI.create("https://localhost"))
              .path("path")
              .build();

      HTTPRequest authenticated = session.authenticate(original);

      assertThat(authenticated.headers().entries())
          .singleElement()
          .extracting(HTTPHeader::name, HTTPHeader::value)
          .containsExactly("Authorization", "s3cr3t");
    }
  }

  @Test
  void authenticateWithConflictingHeader() {
    try (DefaultAuthSession session =
        DefaultAuthSession.of(HTTPHeaders.of(HTTPHeader.of("Authorization", "s3cr3t")))) {

      HTTPRequest original =
          ImmutableHTTPRequest.builder()
              .method(HTTPMethod.GET)
              .baseUri(URI.create("https://localhost"))
              .path("path")
              .headers(HTTPHeaders.of(HTTPHeader.of("Authorization", "other")))
              .build();

      HTTPRequest authenticated = session.authenticate(original);

      assertThat(authenticated).isSameAs(original);
    }
  }
}
