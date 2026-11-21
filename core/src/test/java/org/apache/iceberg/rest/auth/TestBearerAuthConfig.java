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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestBearerAuthConfig {

  @TempDir private Path tempDir;

  @Test
  void staticToken() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token"));
    assertThat(config.token()).isEqualTo("static-token");
    assertThat(config.tokenFile()).isNull();
    assertThat(config.reloadInterval()).isNull();
  }

  @Test
  void tokenFileWithDefaultReloadInterval() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(
            Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, "/var/run/secrets/token"));
    assertThat(config.tokenFile()).isEqualTo(Paths.get("/var/run/secrets/token"));
    assertThat(config.reloadInterval()).isEqualTo(BearerAuthConfig.DEFAULT_RELOAD_INTERVAL);
    assertThat(config.token()).isNull();
  }

  @ParameterizedTest
  @ValueSource(strings = {"PT1S", "PT30S", "PT5M", "PT1H"})
  void tokenFileWithReloadInterval(String interval) {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(
            Map.of(
                BearerAuthConfig.BEARER_TOKEN_FILE,
                "/var/run/secrets/token",
                BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
                interval));
    assertThat(config.reloadInterval()).isEqualTo(Duration.parse(interval));
  }

  @ParameterizedTest
  @ValueSource(strings = {"token", "secrets/token", "./token", "../token", "C:\\secrets\\token"})
  void tokenFileValidPath(String path) {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, path));
    assertThat(config.tokenFile()).isEqualTo(Paths.get(path).toAbsolutePath().normalize());
  }

  @Test
  void tokenFileWithoutReloadInterval() {
    assertThatThrownBy(() -> new BearerAuthConfig(Paths.get("/var/run/secrets/token"), null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid bearer configuration: reload interval is required when using a token file");
  }

  @Test
  void staticTokenWithReloadInterval() {
    assertThatThrownBy(() -> new BearerAuthConfig(null, Duration.ofMinutes(1), "static-token"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid bearer configuration: reload interval is not allowed when using a static token");
  }

  @Test
  void missingConfiguration() {
    assertThatThrownBy(() -> BearerAuthConfig.fromProperties(Map.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer configuration: one of token file or static token is required");
  }

  @Test
  void mutuallyExclusiveConfiguration() {
    Map<String, String> properties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            "/var/run/secrets/token",
            BearerAuthConfig.BEARER_TOKEN,
            "static-token");
    assertThatThrownBy(() -> BearerAuthConfig.fromProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid bearer configuration: token file and static token are mutually exclusive");
  }

  @Test
  void legacyTokenIgnored() {
    assertThatThrownBy(
            () -> BearerAuthConfig.fromProperties(Map.of(OAuth2Properties.TOKEN, "legacy-token")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid bearer configuration: one of token file or static token is required");
  }

  @Test
  void legacyTokenIgnoredWithTokenFile() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(
            Map.of(
                OAuth2Properties.TOKEN,
                "legacy-token",
                BearerAuthConfig.BEARER_TOKEN_FILE,
                "/var/run/secrets/token"));
    assertThat(config.token()).isNull();
    assertThat(config.tokenFile()).isEqualTo(Paths.get("/var/run/secrets/token"));
  }

  @Test
  void tokenFileEmpty() {
    assertThatThrownBy(
            () -> BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN_FILE, "")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token file: empty");
  }

  @ParameterizedTest
  @ValueSource(strings = {"PT0.999S", "PT0S", "-PT1M"})
  void reloadIntervalTooShort(String interval) {
    Map<String, String> properties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            "/var/run/secrets/token",
            BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
            interval);
    assertThatThrownBy(() -> BearerAuthConfig.fromProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid reload interval: %s, must be >= %s",
            Duration.parse(interval), BearerAuthConfig.MIN_RELOAD_INTERVAL);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "60", "1m", "PT"})
  void reloadIntervalInvalid(String interval) {
    Map<String, String> properties =
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            "/var/run/secrets/token",
            BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
            interval);
    assertThatThrownBy(() -> BearerAuthConfig.fromProperties(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid reload interval: %s, expected an ISO-8601 duration such as PT1M", interval);
  }

  @Test
  void equalityForTokenFile() {
    BearerAuthConfig config = tokenFileConfig("/var/run/secrets/token", "PT1M");
    assertThat(config)
        .isEqualTo(tokenFileConfig("/var/run/secrets/./token", "PT1M"))
        .isEqualTo(tokenFileConfig("/var/run/other/../secrets/token", "PT1M"))
        .isNotEqualTo(tokenFileConfig("/var/run/secrets/token", "PT5M"))
        .isNotEqualTo(tokenFileConfig("/var/run/secrets/other", "PT1M"));
  }

  @Test
  void equalityForRelativeTokenFile() {
    assertThat(tokenFileConfig("token", "PT1M"))
        .isEqualTo(tokenFileConfig(Paths.get("token").toAbsolutePath().toString(), "PT1M"));
  }

  @Test
  void equalityForStaticToken() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "token1"));
    assertThat(config)
        .isEqualTo(BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "token1")))
        .isNotEqualTo(
            BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "token2")));
  }

  @Test
  void newSourceForStaticToken() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "static-token"));
    assertThat(config.newSource())
        .isInstanceOf(StaticTokenSource.class)
        .extracting(BearerTokenSource::token)
        .isEqualTo("static-token");
  }

  @Test
  void newSourceForTokenFile() throws IOException {
    Path tokenFile = tempDir.resolve("token");
    Files.writeString(tokenFile, "file-token");
    BearerAuthConfig config = tokenFileConfig(tokenFile.toString(), "PT1M");
    assertThat(config.newSource())
        .isInstanceOf(FileTokenSource.class)
        .extracting(BearerTokenSource::token)
        .isEqualTo("file-token");
  }

  @Test
  void toStringRedactsToken() {
    BearerAuthConfig config =
        BearerAuthConfig.fromProperties(Map.of(BearerAuthConfig.BEARER_TOKEN, "s3cr3t"));
    assertThat(config.toString()).doesNotContain("s3cr3t").contains("token=****");
  }

  @Test
  void toStringForTokenFile() {
    BearerAuthConfig config = tokenFileConfig("/var/run/secrets/token", "PT5M");
    assertThat(config.toString())
        .contains("tokenFile=/var/run/secrets/token")
        .contains("reloadInterval=PT5M")
        .doesNotContain("token=");
  }

  private static BearerAuthConfig tokenFileConfig(String path, String interval) {
    return BearerAuthConfig.fromProperties(
        Map.of(
            BearerAuthConfig.BEARER_TOKEN_FILE,
            path,
            BearerAuthConfig.BEARER_TOKEN_FILE_RELOAD_INTERVAL,
            interval));
  }
}
