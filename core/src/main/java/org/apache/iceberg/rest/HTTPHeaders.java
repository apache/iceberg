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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.immutables.value.Value;

/**
 * Represents a group of HTTP headers.
 *
 * <p>Header name comparison in this class is always case-insensitive, in accordance with RFC 2616.
 *
 * <p>This class exposes methods to convert to and from different representations such as maps and
 * multimap, for easier access and manipulation – especially when dealing with multiple headers with
 * the same name.
 */
@Value.Style(depluralize = true)
@Value.Immutable
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface HTTPHeaders {

  HTTPHeaders EMPTY = of();

  /** Returns all the header entries in this group. */
  Set<HTTPHeader> entries();

  /** Returns all the entries in this group for the given name (case-insensitive). */
  default Set<HTTPHeader> entries(String name) {
    return entries().stream()
        .filter(header -> header.name().equalsIgnoreCase(name))
        .collect(Collectors.toSet());
  }

  /** Returns the first entry in this group for the given name (case-insensitive). */
  default Optional<HTTPHeader> firstEntry(String name) {
    return entries().stream().filter(header -> header.name().equalsIgnoreCase(name)).findFirst();
  }

  /** Returns whether this group contains an entry with the given name (case-insensitive). */
  default boolean contains(String name) {
    return entries().stream().anyMatch(header -> header.name().equalsIgnoreCase(name));
  }

  /**
   * Adds the given header to the current group if no entry with the same name is already present.
   * Returns a new instance with the added header, or the current instance if the header is already
   * present.
   */
  default HTTPHeaders putIfAbsent(HTTPHeader header) {
    Preconditions.checkNotNull(header, "header");
    return contains(header.name())
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addEntry(header).build();
  }

  /**
   * Adds the given headers to the current group if no entries with same names are already present.
   * Returns a new instance with the added headers, or the current instance if all headers are
   * already present.
   */
  default HTTPHeaders putIfAbsent(HTTPHeaders headers) {
    Preconditions.checkNotNull(headers, "headers");
    List<HTTPHeader> newHeaders =
        headers.entries().stream().filter(e -> !contains(e.name())).collect(Collectors.toList());
    return newHeaders.isEmpty()
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addAllEntries(newHeaders).build();
  }

  static HTTPHeaders of(HTTPHeader... headers) {
    return ImmutableHTTPHeaders.builder().addEntries(headers).build();
  }

  static HTTPHeaders of(Map<String, String> headers) {
    return ImmutableHTTPHeaders.builder()
        .entries(
            headers.entrySet().stream()
                .map(e -> HTTPHeader.of(e.getKey(), e.getValue()))
                .collect(Collectors.toList()))
        .build();
  }

  /** Represents an HTTP header as a name-value pair. */
  @Value.Style(redactedMask = "****", depluralize = true)
  @Value.Immutable
  @SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
  interface HTTPHeader {

    String name();

    @Value.Redacted
    String value();

    @Value.Check
    default void check() {
      if (name().isEmpty()) {
        throw new IllegalArgumentException("Header name cannot be empty");
      }
    }

    static HTTPHeader of(String name, String value) {
      return ImmutableHTTPHeader.builder().name(name).value(value).build();
    }
  }
}
