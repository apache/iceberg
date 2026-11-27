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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** Configuration for the {@link BearerAuthManager}. */
record BearerAuthConfig(
    @Nullable Path tokenFile, @Nullable Duration reloadInterval, @Nullable String token) {

  static final String BEARER_TOKEN = "rest.auth.bearer.token";
  static final String BEARER_TOKEN_FILE = "rest.auth.bearer.token-file";
  static final String BEARER_TOKEN_FILE_RELOAD_INTERVAL =
      "rest.auth.bearer.token-file.reload-interval";

  static final Duration DEFAULT_RELOAD_INTERVAL = Duration.ofMinutes(1);
  static final Duration MIN_RELOAD_INTERVAL = Duration.ofSeconds(1);

  BearerAuthConfig {
    Preconditions.checkArgument(
        tokenFile == null || token == null,
        "Invalid bearer configuration: token file and static token are mutually exclusive");
    Preconditions.checkArgument(
        tokenFile != null || token != null,
        "Invalid bearer configuration: one of token file or static token is required");
    Preconditions.checkArgument(
        tokenFile == null || reloadInterval != null,
        "Invalid bearer configuration: reload interval is required when using a token file");
    Preconditions.checkArgument(
        tokenFile != null || reloadInterval == null,
        "Invalid bearer configuration: reload interval is not allowed when using a static token");
    Preconditions.checkArgument(
        reloadInterval == null || reloadInterval.compareTo(MIN_RELOAD_INTERVAL) >= 0,
        "Invalid reload interval: %s, must be >= %s",
        reloadInterval,
        MIN_RELOAD_INTERVAL);
    // normalize so that equal configurations designate the same file, e.g. when caching sessions
    tokenFile = tokenFile != null ? tokenFile.toAbsolutePath().normalize() : null;
  }

  static BearerAuthConfig fromProperties(Map<String, String> properties) {
    String tokenFile = properties.get(BEARER_TOKEN_FILE);
    String token = properties.get(BEARER_TOKEN);
    return tokenFile == null
        ? new BearerAuthConfig(null, null, token)
        : new BearerAuthConfig(asPath(tokenFile), parseReloadInterval(properties), token);
  }

  BearerTokenSource newSource() {
    return tokenFile != null
        ? new FileTokenSource(tokenFile, reloadInterval)
        : new StaticTokenSource(token);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .omitNullValues()
        .add("tokenFile", tokenFile)
        .add("reloadInterval", reloadInterval)
        .add("token", token != null ? "****" : null)
        .toString();
  }

  private static Path asPath(String tokenFile) {
    Preconditions.checkArgument(!tokenFile.isEmpty(), "Invalid token file: empty");
    return Paths.get(tokenFile);
  }

  private static Duration parseReloadInterval(Map<String, String> properties) {
    String value = properties.get(BEARER_TOKEN_FILE_RELOAD_INTERVAL);
    if (value == null) {
      return DEFAULT_RELOAD_INTERVAL;
    }

    try {
      return Duration.parse(value);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid reload interval: %s, expected an ISO-8601 duration such as PT1M", value),
          e);
    }
  }
}
