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
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.net.URI;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

/** Represents an HTTP request. */
@Value.Style(
    set = "*",
    put = "set*",
    putAll = "add*",
    create = "new",
    toImmutable = "build",
    redactedMask = "****",
    depluralize = true)
@Value.Modifiable
@Value.Immutable(builder = false)
@ParametersAreNonnullByDefault
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public abstract class HTTPRequest {

  public enum HTTPMethod {
    GET,
    HEAD,
    POST,
    DELETE
  }

  /**
   * Returns the base URI configured at the REST client level. The base URI is used to construct the
   * full {@link #requestUri()}.
   */
  @Value.Parameter(order = 0)
  protected abstract URI baseUri();

  /**
   * Returns the full URI of this request. The URI is constructed from the base URI, path, and query
   * parameters. It cannot be modified directly.
   */
  @Value.Lazy
  public URI requestUri() {
    return RESTUtil.buildRequestUri(this);
  }

  /** Returns the HTTP method of this request. */
  @Value.Parameter(order = 1)
  public abstract HTTPMethod method();

  /** Returns the path of this request. */
  @Value.Parameter(order = 2)
  public abstract String path();

  /** Returns the query parameters of this request. */
  @Value.Parameter(order = 3)
  public abstract Map<String, String> queryParameters();

  /** Returns all the headers of this request. The map is case-sensitive! */
  @Value.Parameter(order = 4)
  @Value.Redacted
  public abstract Map<String, List<String>> headers();

  /** Returns the header values of the given name. */
  public List<String> headers(String name) {
    return headers().getOrDefault(name, List.of());
  }

  /** Returns whether the request contains a header with the given name. */
  public boolean containsHeader(String name) {
    return !headers(name).isEmpty();
  }

  /** Returns the raw, unencoded request body. */
  @Nullable
  @Value.Parameter(order = 5)
  @Value.Redacted
  public abstract Object body();

  /** Returns the encoded request body as a string. */
  @Value.Default
  @Nullable
  @Value.Redacted
  public String encodedBody() {
    return RESTUtil.encodeRequestBody(this);
  }

  /**
   * Returns the {@link ObjectMapper} to use for encoding the request body. The default is {@link
   * RESTObjectMapper#mapper()}.
   */
  @Value.Default
  protected ObjectMapper mapper() {
    return RESTObjectMapper.mapper();
  }

  public static Builder builder() {
    return new Builder();
  }

  /** A modifiable builder for {@link HTTPRequest}. */
  public static class Builder extends ModifiableHTTPRequest {

    /**
     * Sets the header with the given name to the given value, replacing any existing values.
     *
     * @param name the name of the header
     * @param value the value of the header
     * @return {@code this} for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public Builder setHeader(String name, String value) {
      return setHeader(name, List.of(value));
    }

    /**
     * Sets the header with the given name to the given value, if the header is not already set.
     *
     * @param name the name of the header
     * @param value the value of the header
     * @return {@code this} for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public Builder setHeaderIfAbsent(String name, String value) {
      return containsHeader(name) ? this : setHeader(name, value);
    }

    /**
     * Adds the given value to the header with the given name, preserving any existing values.
     *
     * @param name the name of the header
     * @param value the value to add
     * @return {@code this} for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public Builder addHeader(String name, String value) {
      List<String> values =
          ImmutableList.<String>builder().addAll(headers(name)).add(value).build();
      return setHeader(name, values);
    }

    /**
     * Removes all values for the header with the given name.
     *
     * @param name the name of the header
     * @return {@code this} for use in a chained invocation
     */
    @CanIgnoreReturnValue
    public Builder removeHeaders(String name) {
      headers().remove(name);
      return this;
    }

    // method overrides to maintain covariant return type

    @Override
    public Builder from(HTTPRequest instance) {
      super.from(instance);
      return this;
    }

    @Override
    public Builder from(ModifiableHTTPRequest instance) {
      super.from(instance);
      return this;
    }

    @Override
    public Builder baseUri(URI baseUri) {
      super.baseUri(baseUri);
      return this;
    }

    @Override
    public Builder method(HTTPMethod method) {
      super.method(method);
      return this;
    }

    @Override
    public Builder path(String path) {
      super.path(path);
      return this;
    }

    @Override
    public Builder body(@Nullable Object body) {
      super.body(body);
      return this;
    }

    @Override
    public Builder encodedBody(@Nullable String encodedBody) {
      super.encodedBody(encodedBody);
      return this;
    }

    @Override
    public Builder headers(Map<String, ? extends List<String>> entries) {
      super.headers(entries);
      return this;
    }

    @Override
    public Builder addHeaders(Map<String, ? extends List<String>> entries) {
      super.addHeaders(entries);
      return this;
    }

    @Override
    public Builder setHeader(String key, List<String> value) {
      super.setHeader(key, value);
      return this;
    }

    @Override
    public Builder queryParameters(Map<String, ? extends String> entries) {
      super.queryParameters(entries);
      return this;
    }

    @Override
    public Builder addQueryParameters(Map<String, ? extends String> entries) {
      super.addQueryParameters(entries);
      return this;
    }

    @Override
    public Builder setQueryParameter(String key, String value) {
      super.setQueryParameter(key, value);
      return this;
    }

    @Override
    public Builder mapper(ObjectMapper mapper) {
      super.mapper(mapper);
      return this;
    }

    @Override
    public Builder clear() {
      super.clear();
      return this;
    }
  }
}
