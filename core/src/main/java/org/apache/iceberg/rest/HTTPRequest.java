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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.immutables.value.Value;

/** Represents an HTTP request. */
@Value.Style(redactedMask = "****", depluralize = true)
@Value.Immutable
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface HTTPRequest {

  enum HTTPMethod {
    GET,
    HEAD,
    POST,
    DELETE
  }

  /**
   * Returns the base URI configured at the REST client level. The base URI is used to construct the
   * full {@link #requestUri()}.
   */
  URI baseUri();

  /**
   * Returns the full URI of this request. The URI is constructed from the base URI, path, and query
   * parameters. It cannot be modified directly.
   */
  @Value.Lazy
  default URI requestUri() {
    return RESTUtil.buildRequestUri(this);
  }

  /** Returns the HTTP method of this request. */
  HTTPMethod method();

  /** Returns the path of this request. */
  String path();

  /** Returns the query parameters of this request. */
  Map<String, String> queryParameters();

  /** Returns all the headers of this request. The map is case-sensitive! */
  @Value.Redacted
  Map<String, List<String>> headers();

  /** Returns the header values of the given name. */
  default List<String> headers(String name) {
    return headers().getOrDefault(name, List.of());
  }

  /** Returns whether the request contains a header with the given name. */
  default boolean containsHeader(String name) {
    return !headers(name).isEmpty();
  }

  /** Returns the raw, unencoded request body. */
  @Nullable
  @Value.Redacted
  Object body();

  /** Returns the encoded request body as a string. */
  @Value.Lazy
  @Nullable
  @Value.Redacted
  default String encodedBody() {
    return RESTUtil.encodeRequestBody(this);
  }

  /**
   * Returns the {@link ObjectMapper} to use for encoding the request body. The default is {@link
   * RESTObjectMapper#mapper()}.
   */
  @Value.Default
  default ObjectMapper mapper() {
    return RESTObjectMapper.mapper();
  }

  default HTTPRequest putHeadersIfAbsent(Map<String, String> headers) {
    Map<String, List<String>> newHeaders = Maps.newLinkedHashMap(headers());
    headers.forEach((name, value) -> newHeaders.putIfAbsent(name, List.of(value)));
    return ImmutableHTTPRequest.builder().from(this).headers(newHeaders).build();
  }
}
