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
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.ListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.Multimap;
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
  List<HTTPHeader> entries();

  /**
   * Returns a map representation of the headers where each header name is mapped to a list of its
   * values. Header names are case-insensitive.
   */
  @Value.Lazy
  default Map<String, List<String>> asMap() {
    return entries().stream()
        .collect(Collectors.groupingBy(h -> h.name().toLowerCase(Locale.ROOT)))
        .values()
        .stream()
        .collect(
            Collectors.toMap(
                headers -> headers.get(0).name(),
                headers -> headers.stream().map(HTTPHeader::value).collect(Collectors.toList())));
  }

  /**
   * Returns a simple map representation of the headers where each header name is mapped to its
   * first value. If a header has multiple values, only the first value is used. Header names are
   * case-insensitive.
   */
  @Value.Lazy
  default Map<String, String> asSimpleMap() {
    return entries().stream()
        .collect(Collectors.groupingBy(h -> h.name().toLowerCase(Locale.ROOT)))
        .values()
        .stream()
        .collect(
            Collectors.toMap(headers -> headers.get(0).name(), headers -> headers.get(0).value()));
  }

  /**
   * Returns a {@link ListMultimap} representation of the headers. Header names are
   * case-insensitive.
   */
  @Value.Lazy
  default ListMultimap<String, String> asMultiMap() {
    return entries().stream()
        .collect(Collectors.groupingBy(h -> h.name().toLowerCase(Locale.ROOT)))
        .values()
        .stream()
        .collect(
            ImmutableListMultimap.flatteningToImmutableListMultimap(
                headers -> headers.get(0).name(),
                headers -> headers.stream().map(HTTPHeader::value)));
  }

  /** Returns all the entries in this group for the given name (case-insensitive). */
  default List<HTTPHeader> entries(String name) {
    return entries().stream()
        .filter(header -> header.name().equalsIgnoreCase(name))
        .collect(Collectors.toList());
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
  default HTTPHeaders addIfAbsent(HTTPHeader header) {
    return contains(header.name())
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addEntry(header).build();
  }

  /**
   * Adds the given headers to the current group if no entries with same names are already present.
   * Returns a new instance with the added headers, or the current instance if all headers are
   * already present.
   */
  default HTTPHeaders addIfAbsent(HTTPHeaders headers) {
    List<HTTPHeader> newHeaders =
        headers.entries().stream().filter(e -> !contains(e.name())).collect(Collectors.toList());
    return newHeaders.isEmpty()
        ? this
        : ImmutableHTTPHeaders.builder().from(this).addAllEntries(newHeaders).build();
  }

  static HTTPHeaders of(HTTPHeader... headers) {
    return ImmutableHTTPHeaders.builder().addEntries(headers).build();
  }

  static HTTPHeaders fromMap(Map<String, ? extends Iterable<String>> headers) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headers.forEach(
        (name, values) -> values.forEach(value -> builder.addEntry(HTTPHeader.of(name, value))));
    return builder.build();
  }

  static HTTPHeaders fromSimpleMap(Map<String, String> headers) {
    ImmutableHTTPHeaders.Builder builder = ImmutableHTTPHeaders.builder();
    headers.forEach((name, value) -> builder.addEntry(HTTPHeader.of(name, value)));
    return builder.build();
  }

  static HTTPHeaders fromMultiMap(Multimap<String, String> headers) {
    return fromMap(headers.asMap());
  }

  /** Represents an HTTP header as a name-value pair. */
  @Value.Style(redactedMask = "****", depluralize = true)
  @Value.Immutable
  @SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
  interface HTTPHeader {

    String name();

    @Value.Redacted
    String value();

    static HTTPHeader of(String name, String value) {
      return ImmutableHTTPHeader.builder().name(name).value(value).build();
    }
  }
}
