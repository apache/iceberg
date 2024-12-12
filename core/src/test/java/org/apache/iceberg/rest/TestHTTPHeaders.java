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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableListMultimap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.junit.jupiter.api.Test;

class TestHTTPHeaders {

  final HTTPHeaders headers =
      HTTPHeaders.of(
          HTTPHeader.of("header1", "value1a"),
          HTTPHeader.of("HEADER1", "value1b"),
          HTTPHeader.of("header2", "value2"));

  @Test
  void asMap() {
    assertThat(headers.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2")));
  }

  @Test
  void asSimpleMap() {
    assertThat(headers.asSimpleMap())
        .isEqualTo(
            Map.of(
                "header1", "value1a",
                "header2", "value2"));
  }

  @Test
  void asMultiMap() {
    assertThat(headers.asMultiMap())
        .isEqualTo(
            ImmutableListMultimap.of(
                "header1", "value1a", "header1", "value1b", "header2", "value2"));
  }

  @Test
  void entries() {
    assertThat(headers.entries("header1"))
        .containsExactly(HTTPHeader.of("header1", "value1a"), HTTPHeader.of("HEADER1", "value1b"));
    assertThat(headers.entries("HEADER1"))
        .containsExactly(HTTPHeader.of("header1", "value1a"), HTTPHeader.of("HEADER1", "value1b"));
    assertThat(headers.entries("header2")).containsExactly(HTTPHeader.of("header2", "value2"));
    assertThat(headers.entries("HEADER2")).containsExactly(HTTPHeader.of("header2", "value2"));
    assertThat(headers.entries("header3")).isEmpty();
    assertThat(headers.entries("HEADER3")).isEmpty();
  }

  @Test
  void contains() {
    assertThat(headers.contains("header1")).isTrue();
    assertThat(headers.contains("HEADER1")).isTrue();
    assertThat(headers.contains("header2")).isTrue();
    assertThat(headers.contains("HEADER2")).isTrue();
    assertThat(headers.contains("header3")).isFalse();
    assertThat(headers.contains("HEADER3")).isFalse();
  }

  @Test
  void addIfAbsentHTTPHeader() {
    HTTPHeaders actual = headers.addIfAbsent(HTTPHeader.of("Header1", "value1c"));
    assertThat(actual).isSameAs(headers);

    actual = headers.addIfAbsent(HTTPHeader.of("header3", "value3"));
    assertThat(actual.asMap())
        .isEqualTo(
            Map.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2"),
                "header3", List.of("value3")));
  }

  @Test
  void addIfAbsentHTTPHeaders() {
    HTTPHeaders actual = headers.addIfAbsent(HTTPHeaders.of(HTTPHeader.of("Header1", "value1c")));
    assertThat(actual).isSameAs(headers);

    actual =
        headers.addIfAbsent(
            ImmutableHTTPHeaders.builder()
                .addEntry(HTTPHeader.of("Header1", "value1c"))
                .addEntry(HTTPHeader.of("header3", "value3"))
                .build());
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addEntries(
                    HTTPHeader.of("header1", "value1a"),
                    HTTPHeader.of("HEADER1", "value1b"),
                    HTTPHeader.of("header2", "value2"),
                    HTTPHeader.of("header3", "value3"))
                .build());
  }

  @Test
  void fromMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMap(
            ImmutableMap.of(
                "header1", List.of("value1a", "value1b"),
                "header2", List.of("value2")));
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addEntry(HTTPHeader.of("header1", "value1a"))
                .addEntry(HTTPHeader.of("header1", "value1b"))
                .addEntry(HTTPHeader.of("header2", "value2"))
                .build());
  }

  @Test
  void fromSimpleMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromSimpleMap(
            ImmutableMap.of(
                "header1", "value1a",
                "header2", "value2"));
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addEntry(HTTPHeader.of("header1", "value1a"))
                .addEntry(HTTPHeader.of("header2", "value2"))
                .build());
  }

  @Test
  void fromMultiMap() {
    HTTPHeaders actual =
        HTTPHeaders.fromMultiMap(
            ImmutableListMultimap.of(
                "header1", "value1a", "header2", "value2", "header1", "value1b"));
    assertThat(actual)
        .isEqualTo(
            ImmutableHTTPHeaders.builder()
                .addEntry(HTTPHeader.of("header1", "value1a"))
                .addEntry(HTTPHeader.of("header1", "value1b"))
                .addEntry(HTTPHeader.of("header2", "value2"))
                .build());
  }
}
