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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class TestHTTPRequest {

  @ParameterizedTest
  @MethodSource("validRequestUris")
  public void requestUriSuccess(HTTPRequest request, URI expected) {
    assertThat(request.requestUri()).isEqualTo(expected);
  }

  public static Stream<Arguments> validRequestUris() {
    return Stream.of(
        Arguments.of(
            ImmutableHTTPRequest.builder()
                .baseUri(URI.create("http://localhost:8080/foo"))
                .method(HTTPRequest.HTTPMethod.GET)
                .path("v1/namespaces/ns/tables/") // trailing slash should be removed
                .putQueryParameter("pageToken", "1234")
                .putQueryParameter("pageSize", "10")
                .build(),
            URI.create(
                "http://localhost:8080/foo/v1/namespaces/ns/tables?pageToken=1234&pageSize=10")),
        Arguments.of(
            ImmutableHTTPRequest.builder()
                .baseUri(
                    URI.create("http://localhost:8080/foo/")) // trailing slash should be removed
                .method(HTTPRequest.HTTPMethod.GET)
                .path("v1/namespaces/ns/tables/") // trailing slash should be removed
                .putQueryParameter("pageToken", "1234")
                .putQueryParameter("pageSize", "10")
                .build(),
            URI.create(
                "http://localhost:8080/foo/v1/namespaces/ns/tables?pageToken=1234&pageSize=10")),
        Arguments.of(
            ImmutableHTTPRequest.builder()
                .baseUri(URI.create("http://localhost:8080/foo"))
                .method(HTTPRequest.HTTPMethod.GET)
                .path("https://authserver.com/token") // absolute path HTTPS
                .build(),
            URI.create("https://authserver.com/token")),
        Arguments.of(
            ImmutableHTTPRequest.builder()
                .method(HTTPRequest.HTTPMethod.GET)
                .path("http://authserver.com/token") // absolute path HTTP
                .build(),
            URI.create("http://authserver.com/token")),
        Arguments.of(
            ImmutableHTTPRequest.builder()
                .method(HTTPRequest.HTTPMethod.GET)
                // absolute path with trailing slash: should be preserved
                .path("http://authserver.com/token/")
                .build(),
            URI.create("http://authserver.com/token/")));
  }

  @Test
  public void malformedPath() {
    assertThatThrownBy(
            () ->
                ImmutableHTTPRequest.builder()
                    .baseUri(URI.create("http://localhost"))
                    .method(HTTPRequest.HTTPMethod.GET)
                    .path("/v1/namespaces") // wrong leading slash
                    .build())
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Received a malformed path for a REST request: /v1/namespaces. Paths should not start with /");
  }

  @Test
  public void relativePathWithoutBaseUri() {
    assertThatThrownBy(
            () ->
                ImmutableHTTPRequest.builder()
                    .method(HTTPRequest.HTTPMethod.GET)
                    .path("v1/namespaces")
                    .build())
        .isInstanceOf(RESTException.class)
        .hasMessage("Received a request with a relative path and no base URI: v1/namespaces");
  }

  @Test
  public void invalidPath() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.GET)
            .path(" not a valid path") // wrong path
            .build();
    assertThatThrownBy(request::requestUri)
        .isInstanceOf(RESTException.class)
        .hasMessage(
            "Failed to create request URI from base http://localhost/ not a valid path, params {}");
  }

  @Test
  public void encodedBodyJSON() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.POST)
            .path("v1/namespaces/ns")
            .body(
                CreateNamespaceRequest.builder()
                    .withNamespace(Namespace.of("ns"))
                    .setProperties(ImmutableMap.of("prop1", "value1"))
                    .build())
            .build();
    assertThat(request.encodedBody())
        .isEqualTo("{\"namespace\":[\"ns\"],\"properties\":{\"prop1\":\"value1\"}}");
  }

  @Test
  public void encodedBodyJSONInvalid() throws JsonProcessingException {
    ObjectMapper mapper = Mockito.mock(ObjectMapper.class);
    Mockito.when(mapper.writeValueAsString(Mockito.any()))
        .thenThrow(new JsonMappingException(null, "invalid"));
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.POST)
            .path("token")
            .body("invalid")
            .mapper(mapper)
            .build();
    assertThatThrownBy(request::encodedBody)
        .isInstanceOf(RESTException.class)
        .hasMessage("Failed to encode request body: invalid");
  }

  @Test
  public void encodedBodyFormData() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.POST)
            .path("token")
            .body(
                ImmutableMap.of(
                    "grant_type", "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token", "token",
                    "subject_token_type", "urn:ietf:params:oauth:token-type:access_token",
                    "scope", "catalog"))
            .build();
    assertThat(request.encodedBody())
        .isEqualTo(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange&"
                + "subject_token=token&"
                + "subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token&"
                + "scope=catalog");
  }

  @Test
  public void encodedBodyFormDataNullKeysAndValues() {
    Map<String, String> body = Maps.newHashMap();
    body.put(null, "token");
    body.put("scope", null);
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.POST)
            .path("token")
            .body(body)
            .build();
    assertThat(request.encodedBody()).isEqualTo("null=token&scope=null");
  }

  @Test
  public void encodedBodyNull() {
    HTTPRequest request =
        ImmutableHTTPRequest.builder()
            .baseUri(URI.create("http://localhost"))
            .method(HTTPRequest.HTTPMethod.POST)
            .path("token")
            .build();
    assertThat(request.encodedBody()).isNull();
  }
}
