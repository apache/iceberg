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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.immutables.value.Value;

final class ConfigValidator {

  @Value.Immutable
  interface ConfigViolation {

    @Value.Parameter(order = 1)
    List<String> offendingKeys();

    @Value.Parameter(order = 2)
    String message();

    @Value.Lazy
    default String formattedMessage() {
      return message() + " (" + String.join(" / ", offendingKeys()) + ")";
    }
  }

  private final List<ConfigViolation> violations = Lists.newArrayList();

  @FormatMethod
  public void check(
      boolean condition, String offendingKey, @FormatString String msg, Object... args) {
    check(condition, List.of(offendingKey), msg, args);
  }

  @FormatMethod
  public void check(
      boolean condition, List<String> offendingKeys, @FormatString String msg, Object... args) {
    if (!condition) {
      violations.add(ImmutableConfigViolation.of(offendingKeys, String.format(msg, args)));
    }
  }

  public void checkEndpoint(URI endpoint, String offendingKey, String name) {
    check(endpoint.isAbsolute(), offendingKey, "%s %s", name, "must not be relative");
    check(
        endpoint.getUserInfo() == null,
        offendingKey,
        "%s %s",
        name,
        "must not have a user info part");
    check(endpoint.getQuery() == null, offendingKey, "%s %s", name, "must not have a query part");
    check(
        endpoint.getFragment() == null,
        offendingKey,
        "%s %s",
        name,
        "must not have a fragment part");
  }

  public void validate() {
    if (!violations.isEmpty()) {
      throw new IllegalArgumentException(
          buildDescription(violations.stream().map(ConfigViolation::formattedMessage)));
    }
  }

  private static final String DELIMITER = "\n  - ";

  @VisibleForTesting
  static String buildDescription(Stream<String> violations) {
    return "Invalid OAuth2 configuration:"
        + violations.collect(Collectors.joining(DELIMITER, DELIMITER, ""));
  }
}
