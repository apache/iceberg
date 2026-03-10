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
package org.apache.iceberg.lance;

import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * General utility class for Lance format operations.
 *
 * <p>Provides helper methods for configuration, file path manipulation, and other common tasks
 * needed across Lance format implementations.
 */
public class LanceUtil {

  /** Configuration key prefix for Lance-specific table properties. */
  public static final String LANCE_CONFIG_PREFIX = "lance.";

  /** Configuration key for the Lance fragment size (number of rows per fragment). */
  public static final String LANCE_FRAGMENT_SIZE = LANCE_CONFIG_PREFIX + "fragment-size";

  /** Default fragment size (number of rows per fragment). */
  public static final int DEFAULT_FRAGMENT_SIZE = 1024 * 1024;

  /** Configuration key for Lance compression codec. */
  public static final String LANCE_COMPRESSION = LANCE_CONFIG_PREFIX + "compression";

  /** Default compression codec for Lance files. */
  public static final String DEFAULT_COMPRESSION = "zstd";

  private LanceUtil() {}

  /**
   * Extract Lance-specific properties from a properties map.
   *
   * @param properties the full properties map
   * @return a map containing only Lance-specific properties (with prefix stripped)
   */
  public static Map<String, String> lanceProperties(Map<String, String> properties) {
    Map<String, String> result = Maps.newHashMap();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(LANCE_CONFIG_PREFIX)) {
        String key = entry.getKey().substring(LANCE_CONFIG_PREFIX.length());
        result.put(key, entry.getValue());
      }
    }
    return result;
  }

  /**
   * Get the fragment size from properties, or use the default.
   *
   * @param properties the properties map
   * @return the fragment size
   */
  public static int fragmentSize(Map<String, String> properties) {
    String value = properties.get(LANCE_FRAGMENT_SIZE);
    if (value != null) {
      return Integer.parseInt(value);
    }
    return DEFAULT_FRAGMENT_SIZE;
  }

  /**
   * Get the compression codec from properties, or use the default.
   *
   * @param properties the properties map
   * @return the compression codec name
   */
  public static String compression(Map<String, String> properties) {
    return properties.getOrDefault(LANCE_COMPRESSION, DEFAULT_COMPRESSION);
  }

  /**
   * Add the Lance file extension to a filename if not already present.
   *
   * @param filename the filename
   * @return the filename with .lance extension
   */
  public static String addExtension(String filename) {
    if (filename.endsWith(".lance")) {
      return filename;
    }
    return filename + ".lance";
  }
}
