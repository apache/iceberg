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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestConfigUtil {

  @ParameterizedTest
  @MethodSource
  void requiresClientSecret(ClientAuthenticationMethod method, boolean expectedResult) {
    assertThat(ConfigUtil.requiresClientSecret(method)).isEqualTo(expectedResult);
  }

  static Stream<Arguments> requiresClientSecret() {
    return Stream.of(
        Arguments.of(null, false),
        Arguments.of(ClientAuthenticationMethod.CLIENT_SECRET_BASIC, true),
        Arguments.of(ClientAuthenticationMethod.CLIENT_SECRET_POST, true),
        Arguments.of(ClientAuthenticationMethod.NONE, false));
  }

  @Test
  void parseOptional() {
    assertThat(ConfigUtil.parseOptional(Map.of("a", "1"), "a")).hasValue("1");
    assertThat(ConfigUtil.parseOptional(Map.of("a", "1"), "b")).isEmpty();
  }

  @Test
  void parseOptionalWithParser() {
    assertThat(ConfigUtil.parseOptional(Map.of("a", "1"), "a", Integer::parseInt)).hasValue(1);
    assertThat(ConfigUtil.parseOptional(Map.of("a", "1"), "b", Integer::parseInt)).isEmpty();
  }

  @Test
  void parseOptionalWithParserFailure() {
    assertThatThrownBy(
            () -> ConfigUtil.parseOptional(Map.of("a", "invalid"), "a", Integer::parseInt))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse configuration value 'invalid'")
        .hasRootCauseInstanceOf(NumberFormatException.class);
  }

  @Test
  void parseOptionalInt() {
    assertThat(ConfigUtil.parseOptionalInt(Map.of("a", "1"), "a")).hasValue(1);
    assertThat(ConfigUtil.parseOptionalInt(Map.of("a", "1"), "b")).isEmpty();
  }

  @Test
  void parseOptionalIntFailure() {
    assertThatThrownBy(() -> ConfigUtil.parseOptionalInt(Map.of("a", "invalid"), "a"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse configuration value 'invalid'")
        .hasRootCauseInstanceOf(NumberFormatException.class);
  }

  @Test
  void parseList() {
    assertThat(ConfigUtil.parseList(Map.of("a", ""), "a", ",")).isEmpty();
    assertThat(ConfigUtil.parseList(Map.of("a", "1,2,3"), "a", ",")).containsExactly("1", "2", "3");
    assertThat(ConfigUtil.parseList(Map.of("a", "1 , 2 , 3"), "a", ","))
        .containsExactly("1", "2", "3");
    assertThat(ConfigUtil.parseList(Map.of("a", "1  2  3"), "a", " "))
        .containsExactly("1", "2", "3");
    assertThat(ConfigUtil.parseList(Map.of("a", "1,2,3"), "b", ",")).isEmpty();
  }

  @Test
  void parseListWithParser() {
    assertThat(ConfigUtil.parseList(Map.of("a", ""), "a", ",", Integer::parseInt)).isEmpty();
    assertThat(ConfigUtil.parseList(Map.of("a", "1,2,3"), "a", ",", Integer::parseInt))
        .containsExactly(1, 2, 3);
    assertThat(ConfigUtil.parseList(Map.of("a", "1 , 2 , 3"), "a", ",", Integer::parseInt))
        .containsExactly(1, 2, 3);
    assertThat(ConfigUtil.parseList(Map.of("a", "1  2  3"), "a", " ", Integer::parseInt))
        .containsExactly(1, 2, 3);
    assertThat(ConfigUtil.parseList(Map.of("a", "1,2,3"), "b", ",", Integer::parseInt)).isEmpty();
  }

  @Test
  void parseListWithParserFailure() {
    assertThatThrownBy(
            () -> ConfigUtil.parseList(Map.of("a", "1,invalid,3"), "a", ",", Integer::parseInt))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Failed to parse configuration value 'invalid'")
        .hasRootCauseInstanceOf(NumberFormatException.class);
  }
}
