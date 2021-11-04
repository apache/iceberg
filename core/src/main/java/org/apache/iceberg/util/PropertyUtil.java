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

public class PropertyUtil {

  private PropertyUtil() {
  }

  public static boolean propertyAsBoolean(Map<String, String> properties,
                                          String property, boolean defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Boolean.parseBoolean(properties.get(property));
    }
    return defaultValue;
  }

  public static double propertyAsDouble(Map<String, String> properties,
      String property, double defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Double.parseDouble(properties.get(property));
    }
    return defaultValue;
  }

  public static int propertyAsInt(Map<String, String> properties,
                                  String property, int defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Integer.parseInt(properties.get(property));
    }
    return defaultValue;
  }

  public static long propertyAsLong(Map<String, String> properties,
                                    String property, long defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return Long.parseLong(properties.get(property));
    }
    return defaultValue;
  }

  public static String propertyAsString(Map<String, String> properties,
                                        String property, String defaultValue) {
    String value = properties.get(property);
    if (value != null) {
      return properties.get(property);
    }
    return defaultValue;
  }

  @FunctionalInterface
  private interface PropertyResolver<A, B, C, R> {
    R apply(A argA, B argB, C argC);
  }

  private static <T> T resolveProperty(PropertyResolver<Map<String, String>, String, T, T> resolver,
                                    Map<String, String> options, String optionKey,
                                    Map<String, String> properties, String propertyKey, T defaultVal) {
    return resolver.apply(options, optionKey, resolver.apply(properties, propertyKey, defaultVal));
  }

  private static <T> T resolveProperty(PropertyResolver<Map<String, String>, String, T, T> resolver,
                                      Map<String, String> options, String optionKey,
                                      Map<String, String> properties1, String propertyKey1,
                                      Map<String, String> properties2, String propertyKey2, T defaultVal) {
    return resolver.apply(options, optionKey, resolver.apply(properties1, propertyKey1,
            resolver.apply(properties2, propertyKey2, defaultVal)));
  }

  public static long resolveLongProperty(Map<String, String> options, String optionKey,
                                         Map<String, String> properties, String propertyKey, long defaultVal) {
    return resolveProperty(PropertyUtil::propertyAsLong, options, optionKey, properties, propertyKey, defaultVal);
  }

  public static long resolveLongProperty(Map<String, String> options, String optionKey,
                                         Map<String, String> properties1, String propertyKey1,
                                         Map<String, String> properties2, String propertyKey2, long defaultVal) {
    return resolveProperty(PropertyUtil::propertyAsLong, options, optionKey, properties1, propertyKey1,
            properties2, propertyKey2, defaultVal);
  }

  public static int resolveIntProperty(Map<String, String> options, String optionKey,
                                       Map<String, String> properties, String propertyKey, int defaultVal) {
    return resolveProperty(PropertyUtil::propertyAsInt, options, optionKey, properties, propertyKey, defaultVal);
  }

  public static boolean resolveBooleanProperty(Map<String, String> options, String optionKey,
                                               Map<String, String> properties, String propertyKey, boolean defaultVal) {
    return resolveProperty(PropertyUtil::propertyAsBoolean, options, optionKey, properties, propertyKey, defaultVal);
  }
}
