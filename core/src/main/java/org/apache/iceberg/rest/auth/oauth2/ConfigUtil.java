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

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

/**
 * Helper class for parsing configuration options. It also exposes useful constants for config
 * validation.
 */
final class ConfigUtil {

  static final List<GrantType> SUPPORTED_INITIAL_GRANT_TYPES =
      List.of(GrantType.CLIENT_CREDENTIALS, GrantType.TOKEN_EXCHANGE);

  static final List<ClientAuthenticationMethod> SUPPORTED_CLIENT_AUTH_METHODS =
      List.of(
          ClientAuthenticationMethod.NONE,
          ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
          ClientAuthenticationMethod.CLIENT_SECRET_POST);

  static boolean requiresClientSecret(@Nullable ClientAuthenticationMethod method) {
    return Objects.equals(method, ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
        || Objects.equals(method, ClientAuthenticationMethod.CLIENT_SECRET_POST);
  }

  static Optional<String> parseOptional(Map<String, String> properties, String option) {
    return parseOptional(properties, option, s -> s);
  }

  static <T> Optional<T> parseOptional(
      Map<String, String> properties, String option, ConfigParser<T> parser) {
    return Optional.ofNullable(properties.get(option)).map(parser::parseUnchecked);
  }

  static OptionalInt parseOptionalInt(Map<String, String> properties, String option) {
    ConfigParser<Integer> parser = Integer::parseInt;
    return Optional.ofNullable(properties.get(option))
        .map(parser::parseUnchecked)
        .map(OptionalInt::of)
        .orElseGet(OptionalInt::empty);
  }

  static List<String> parseList(Map<String, String> properties, String option, String delimiter) {
    return parseList(properties, option, delimiter, s -> s);
  }

  static <T> List<T> parseList(
      Map<String, String> properties, String option, String delimiter, ConfigParser<T> parser) {
    return Optional.ofNullable(properties.get(option))
        .map(s -> Splitter.on(delimiter).trimResults().omitEmptyStrings().splitToStream(s))
        .orElseGet(Stream::empty)
        .map(parser::parseUnchecked)
        .collect(Collectors.toList());
  }

  static void checkEndpoint(URI endpoint, String name, String key) {
    Preconditions.checkArgument(endpoint.isAbsolute(), "%s must not be relative (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getUserInfo() == null, "%s must not have a user info part (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getQuery() == null, "%s must not have a query part (%s)", name, key);
    Preconditions.checkArgument(
        endpoint.getFragment() == null, "%s must not have a fragment part (%s)", name, key);
  }

  @FunctionalInterface
  interface ConfigParser<T> {

    T parse(String value) throws Exception;

    default T parseUnchecked(String value) {
      try {
        return parse(value);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to parse configuration value '%s'".formatted(value), e);
      }
    }
  }

  private ConfigUtil() {}
}
