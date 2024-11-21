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
package org.apache.iceberg.rest;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TestHTTPRequest {

  @Test
  void headers() {
    HTTPRequest request =
        HTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("path")
            .setHeader("name", "value")
            .build();
    assertThat(request.headers("name")).containsExactly("value");
    assertThat(request.headers("nonexistent")).isEmpty();
  }

  @Test
  void containsHeader() {
    HTTPRequest request =
        HTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path("path")
            .headers(Map.of("k1", List.of("v1"), "k2", List.of()))
            .build();
    assertThat(request.containsHeader("k1")).isTrue();
    assertThat(request.containsHeader("k2")).isFalse();
    assertThat(request.containsHeader("k3")).isFalse();
  }

  @Test
  void setHeaderIfAbsent() {
    HTTPRequest.Builder request =
        HTTPRequest.builder()
            .method(HTTPRequest.HTTPMethod.GET)
            .path("path")
            .headers(Map.of("k1", List.of("v1a"), "k2", List.of()));
    request.setHeaderIfAbsent("k1", "v1b");
    request.setHeaderIfAbsent("k2", "v2b");
    request.setHeaderIfAbsent("k3", "v3b");
    assertThat(request.headers("k1")).containsExactly("v1a");
    assertThat(request.headers("k2")).containsExactly("v2b");
    assertThat(request.headers("k3")).containsExactly("v3b");
  }

  @Test
  void addHeader() {
    HTTPRequest.Builder request =
        HTTPRequest.builder()
            .method(HTTPRequest.HTTPMethod.GET)
            .path("path")
            .headers(Map.of("k1", List.of("v1a"), "k2", List.of()));
    request.addHeader("k1", "v1b");
    request.addHeader("k2", "v2b");
    request.addHeader("k3", "v3b");
    assertThat(request.headers("k1")).containsExactly("v1a", "v1b");
    assertThat(request.headers("k2")).containsExactly("v2b");
    assertThat(request.headers("k3")).containsExactly("v3b");
  }

  @Test
  void removeHeaders() {
    HTTPRequest.Builder request =
        HTTPRequest.builder()
            .method(HTTPRequest.HTTPMethod.GET)
            .path("path")
            .headers(Map.of("k1", List.of("v1a"), "k2", List.of()));
    request.removeHeaders("k1");
    request.removeHeaders("k2");
    request.removeHeaders("k3");
    assertThat(request.headers("k1")).isEmpty();
    assertThat(request.headers("k2")).isEmpty();
    assertThat(request.headers("k3")).isEmpty();
  }

  @Test
  void encodedBody() {
    Map<String, String> body = Map.of("key", "value");
    HTTPRequest.Builder builder =
        HTTPRequest.builder()
            .baseUri(URI.create("https://localhost"))
            .path("path")
            .method(HTTPRequest.HTTPMethod.GET)
            .body(body);
    String encodedBody = builder.encodedBody();
    assertThat(encodedBody).isEqualTo("key=value");
    assertThat(builder.build())
        .extracting("body", "encodedBody")
        .containsExactly(body, encodedBody);
    // override the encoded body
    builder.encodedBody("overridden");
    assertThat(builder.build())
        .extracting("body", "encodedBody")
        .containsExactly(body, "overridden");
  }
}
