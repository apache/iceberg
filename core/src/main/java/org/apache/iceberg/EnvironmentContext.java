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
package org.apache.iceberg;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class EnvironmentContext {
  public static final String ENGINE_NAME = "engine-name";
  public static final String ENGINE_VERSION = "engine-version";

  private EnvironmentContext() {}

  private static final Map<String, String> PROPERTIES = Maps.newConcurrentMap();

  static {
    PROPERTIES.put("iceberg-version", IcebergBuild.fullVersion());
  }

  /**
   * Returns a {@link Map} of all properties.
   *
   * @return A {@link Map} of all properties.
   */
  public static Map<String, String> get() {
    return ImmutableMap.copyOf(PROPERTIES);
  }

  /**
   * Will add the given key/value pair in a global properties map.
   *
   * @param key The key to add
   * @param value The value to add
   */
  public static void put(String key, String value) {
    PROPERTIES.put(key, value);
  }
}
