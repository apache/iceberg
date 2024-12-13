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

import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.junit.jupiter.api.Test;

class TestHTTPHeaders {

  private final HTTPHeaders headers =
      HTTPHeaders.of(
          HTTPHeader.of("header1", "value1a"),
          HTTPHeader.of("HEADER1", "value1b"),
          HTTPHeader.of("header2", "value2"));

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
    assertThat(headers.entries(null)).isEmpty();
  }

  @Test
  void contains() {
    assertThat(headers.contains("header1")).isTrue();
    assertThat(headers.contains("HEADER1")).isTrue();
    assertThat(headers.contains("header2")).isTrue();
    assertThat(headers.contains("HEADER2")).isTrue();
    assertThat(headers.contains("header3")).isFalse();
    assertThat(headers.contains("HEADER3")).isFalse();
    assertThat(headers.contains(null)).isFalse();
  }

  @Test
  void withHeaderIfAbsentHTTPHeader() {
    HTTPHeaders actual = headers.withHeaderIfAbsent(HTTPHeader.of("Header1", "value1c"));
    assertThat(actual).isSameAs(headers);

    actual = headers.withHeaderIfAbsent(HTTPHeader.of("header3", "value3"));
    assertThat(actual.entries())
        .containsExactly(
            HTTPHeader.of("header1", "value1a"),
            HTTPHeader.of("HEADER1", "value1b"),
            HTTPHeader.of("header2", "value2"),
            HTTPHeader.of("header3", "value3"));

    assertThatThrownBy(() -> headers.withHeaderIfAbsent((HTTPHeader) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("header");
  }

  @Test
  void withHeaderIfAbsentHTTPHeaders() {
    HTTPHeaders actual =
        headers.withHeaderIfAbsent(HTTPHeaders.of(HTTPHeader.of("Header1", "value1c")));
    assertThat(actual).isSameAs(headers);

    actual =
        headers.withHeaderIfAbsent(
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

    assertThatThrownBy(() -> headers.withHeaderIfAbsent((HTTPHeaders) null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("headers");
  }

  @Test
  void invalidHeader() {
    // invalid input (null name or value)
    assertThatThrownBy(() -> HTTPHeader.of(null, "value1"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("name");
    assertThatThrownBy(() -> HTTPHeader.of("header1", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("value");

    // invalid input (empty name)
    assertThatThrownBy(() -> HTTPHeader.of("", "value1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Header name cannot be empty");
  }
}
