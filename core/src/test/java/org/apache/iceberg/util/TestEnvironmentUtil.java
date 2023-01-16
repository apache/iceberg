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
package org.apache.iceberg.util;

import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

class TestEnvironmentUtil {
  @Test
  public void testEnvironmentSubstitution() {
    Optional<Map.Entry<String, String>> envEntry = System.getenv().entrySet().stream().findFirst();
    Assumptions.assumeTrue(
        envEntry.isPresent(), "Expecting at least one env. variable to be present");
    Map<String, String> resolvedProps =
        EnvironmentUtil.resolveAll(ImmutableMap.of("env-test", "env:" + envEntry.get().getKey()));
    Assertions.assertEquals(
        ImmutableMap.of("env-test", envEntry.get().getValue()),
        resolvedProps,
        "Should get the user from the environment");
  }

  @Test
  public void testMultipleEnvironmentSubstitutions() {
    Map<String, String> result =
        EnvironmentUtil.resolveAll(
            ImmutableMap.of("USER", "u", "VAR", "value"),
            ImmutableMap.of("user-test", "env:USER", "other", "left-alone", "var", "env:VAR"));

    Assertions.assertEquals(
        ImmutableMap.of("user-test", "u", "other", "left-alone", "var", "value"),
        result,
        "Should resolve all values starting with env:");
  }

  @Test
  public void testEnvironmentSubstitutionWithMissingVar() {
    Map<String, String> result =
        EnvironmentUtil.resolveAll(ImmutableMap.of(), ImmutableMap.of("user-test", "env:USER"));

    Assertions.assertEquals(
        ImmutableMap.of(), result, "Should not contain values with missing environment variables");
  }
}
