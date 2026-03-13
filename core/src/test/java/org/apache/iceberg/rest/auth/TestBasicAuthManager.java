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

import java.util.Map;
import org.apache.iceberg.rest.HTTPHeaders;
import org.junit.jupiter.api.Test;

class TestBasicAuthManager {

  @Test
  void missingUsername() {
    try (AuthManager authManager = new BasicAuthManager("test")) {
      assertThatThrownBy(() -> authManager.catalogSession(null, Map.of()))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "Invalid username: missing required property %s", AuthProperties.BASIC_USERNAME);
    }
  }

  @Test
  void missingPassword() {
    try (AuthManager authManager = new BasicAuthManager("test")) {
      Map<String, String> properties = Map.of(AuthProperties.BASIC_USERNAME, "alice");
      assertThatThrownBy(() -> authManager.catalogSession(null, properties))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              "Invalid password: missing required property %s", AuthProperties.BASIC_PASSWORD);
    }
  }

  @Test
  void success() {
    Map<String, String> properties =
        Map.of(AuthProperties.BASIC_USERNAME, "alice", AuthProperties.BASIC_PASSWORD, "secret");
    try (AuthManager authManager = new BasicAuthManager("test");
        AuthSession session = authManager.catalogSession(null, properties)) {
      assertThat(session)
          .isEqualTo(
              DefaultAuthSession.of(HTTPHeaders.of(OAuth2Util.basicAuthHeaders("alice:secret"))));
    }
  }
}
