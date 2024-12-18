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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestAuthManagers {

  private final PrintStream standardErr = System.err;
  private final ByteArrayOutputStream streamCaptor = new ByteArrayOutputStream();

  @BeforeEach
  public void before() {
    System.setErr(new PrintStream(streamCaptor));
  }

  @AfterEach
  public void after() {
    System.setErr(standardErr);
  }

  @Test
  void noop() {
    try (AuthManager manager = AuthManagers.loadAuthManager("test", Map.of())) {
      assertThat(manager).isInstanceOf(NoopAuthManager.class);
    }
    assertThat(streamCaptor.toString())
        .contains(
            "Loading AuthManager implementation: org.apache.iceberg.rest.auth.NoopAuthManager");
  }

  @Test
  void noopExplicit() {
    try (AuthManager manager =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_NONE))) {
      assertThat(manager).isInstanceOf(NoopAuthManager.class);
    }
    assertThat(streamCaptor.toString())
        .contains(
            "Loading AuthManager implementation: org.apache.iceberg.rest.auth.NoopAuthManager");
  }

  @Test
  void basicExplicit() {
    try (AuthManager manager =
        AuthManagers.loadAuthManager(
            "test", Map.of(AuthProperties.AUTH_TYPE, AuthProperties.AUTH_TYPE_BASIC))) {
      assertThat(manager).isInstanceOf(BasicAuthManager.class);
    }
    assertThat(streamCaptor.toString())
        .contains(
            "Loading AuthManager implementation: org.apache.iceberg.rest.auth.BasicAuthManager");
  }

  @Test
  @SuppressWarnings("resource")
  void nonExistentAuthManager() {
    assertThatThrownBy(
            () -> AuthManagers.loadAuthManager("test", Map.of(AuthProperties.AUTH_TYPE, "unknown")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot initialize AuthManager implementation unknown");
  }
}
