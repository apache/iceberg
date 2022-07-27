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
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class EnvironmentUtil {
  private EnvironmentUtil() {}

  private static final String ENVIRONMENT_VARIABLE_PREFIX = "env:";

  public static Map<String, String> resolveAll(Map<String, String> properties) {
    return resolveAll(System.getenv(), properties);
  }

  @VisibleForTesting
  static Map<String, String> resolveAll(Map<String, String> env, Map<String, String> properties) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    properties.forEach(
        (name, value) -> {
          if (value.startsWith(ENVIRONMENT_VARIABLE_PREFIX)) {
            String resolved = env.get(value.substring(ENVIRONMENT_VARIABLE_PREFIX.length()));
            if (resolved != null) {
              builder.put(name, resolved);
            }
          } else {
            builder.put(name, value);
          }
        });

    return builder.build();
  }
}
