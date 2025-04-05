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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.iceberg.exceptions.RESTException;
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
   * full {@link #requestUri()}. May be null if the REST client does not have a base URI and the
   * path is an absolute URI.
   */
  @Nullable
  URI baseUri();

  /**
   * Returns the full URI of this request. The URI is constructed from the base URI, path, and query
   * parameters. It cannot be modified directly.
   */
  @Value.Lazy
  default URI requestUri() {
    String fullPath;
    if (hasAbsolutePath()) {
      // if path is an absolute URI, use it as is
      fullPath = path();
    } else {
      String baseUri = RESTUtil.stripTrailingSlash(baseUri().toString());
      fullPath = RESTUtil.stripTrailingSlash(String.format("%s/%s", baseUri, path()));
    }

    try {
      URIBuilder builder = new URIBuilder(fullPath);
      queryParameters().forEach(builder::addParameter);
      return builder.build();
    } catch (URISyntaxException e) {
      throw new RESTException(
          "Failed to create request URI from base %s, params %s", fullPath, queryParameters());
    }
  }

  /** Returns the HTTP method of this request. */
  HTTPMethod method();

  /** Returns the path of this request. */
  String path();

  /** Returns the query parameters of this request. */
  Map<String, String> queryParameters();

  /** Returns the headers of this request. */
  @Value.Default
  default HTTPHeaders headers() {
    return HTTPHeaders.EMPTY;
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
    Object body = body();
    if (body instanceof Map) {
      return RESTUtil.encodeFormData((Map<?, ?>) body);
    } else if (body != null) {
      try {
        return mapper().writeValueAsString(body);
      } catch (JsonProcessingException e) {
        throw new RESTException(e, "Failed to encode request body: %s", body);
      }
    }
    return null;
  }

  /**
   * Returns the {@link ObjectMapper} to use for encoding the request body. The default is {@link
   * RESTObjectMapper#mapper()}.
   */
  @Value.Default
  default ObjectMapper mapper() {
    return RESTObjectMapper.mapper();
  }

  @Value.Check
  default void check() {
    if (path().startsWith("/")) {
      throw new RESTException(
          "Received a malformed path for a REST request: %s. Paths should not start with /",
          path());
    }

    if (baseUri() == null && !hasAbsolutePath()) {
      throw new RESTException(
          "Received a request with a relative path and no base URI: %s", path());
    }
  }

  private boolean hasAbsolutePath() {
    return path().startsWith("https://") || path().startsWith("http://");
  }
}
