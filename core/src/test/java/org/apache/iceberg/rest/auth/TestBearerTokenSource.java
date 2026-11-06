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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestBearerTokenSource {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "token",
        "AZaz09-._~+/",
        "dG9rZW4=",
        "dG9rZW4==",
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"
      })
  void validToken(String token) {
    assertThat(BearerTokenSource.validate(token)).isEqualTo(token);
  }

  @ParameterizedTest
  @ValueSource(strings = {" token", "token\n", "\ttoken\r\n", "  token  "})
  void validTokenWithSurroundingWhitespace(String token) {
    assertThat(BearerTokenSource.validate(token)).isEqualTo("token");
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "\n", " \t\r\n "})
  void emptyToken(String token) {
    assertThatThrownBy(() -> BearerTokenSource.validate(token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: empty");
  }

  @Test
  void nullToken() {
    assertThatThrownBy(() -> BearerTokenSource.validate(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: null");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "tok en",
        "tok\ten",
        "tok\nen",
        "tok\r\nX-Injected: true",
        "tok\u0000en",
        "tok\u007fen",
        "tök",
        "tok�en",
        "tok,en",
        "tok\"en",
        "=token",
        "tok=en",
        "Bearer token"
      })
  void invalidToken(String token) {
    assertThatThrownBy(() -> BearerTokenSource.validate(token))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer token: contains characters not allowed by RFC 6750");
  }

  @Test
  void staticTokenSource() {
    StaticTokenSource source = new StaticTokenSource(" s3cr3t\n");
    assertThat(source.token()).isEqualTo("s3cr3t");
    assertThat(source.toString()).doesNotContain("s3cr3t");
  }

  @Test
  void staticTokenSourceInvalid() {
    assertThatThrownBy(() -> new StaticTokenSource("s3cr3t\r\n\r\nbody"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageNotContaining("s3cr3t");
  }
}
