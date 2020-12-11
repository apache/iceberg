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

package org.apache.iceberg.util.conf;

import java.util.function.Function;

/**
 * Wrapper for different {@code Map<String, String>} like config (eg hashmap and hadoop configuration)
 *
 * Users of iceberg can add company specific namespaces to ensure they won't collide with config.
 */
public abstract class Conf {
  public static final String DEFAULT_NAMESPACE = "";

  private final String namespace;

  protected abstract String get(String key);

  protected abstract void set(String key, String value);

  protected Conf() {
    namespace = DEFAULT_NAMESPACE;
  }

  protected Conf(String namespace) {
    this.namespace = namespace;
  }

  public String withNamespace(String property) {
    return namespace + property;
  }

  private <V> V propertyAs(String property, V defaultValue, Function<String, V> parser) {
    return propertyAs(property, defaultValue, parser, false);
  }
  /**
   * When conf passes through spark, some values are lowercased. But the json encoding for the Dek &
   * kekId is cased. Conf is used for reading the kekId from spark options and persisting it to the
   * KeyMetadata byte buffer. This will try reading both types, but only write cased.
   */
  private <V> V propertyAs(
      String property, V defaultValue, Function<String, V> parser, Boolean checkLowercase) {
    String namespacedProperty = withNamespace(property);
    String value = get(namespacedProperty);
    if (value != null) {
      return parser.apply(value);
    } else if (checkLowercase) {
      return propertyAs(property.toLowerCase(), defaultValue, parser, false);
    }
    return defaultValue;
  }

  private <V> V propertyAs(String property, Function<String, V> parser) {
    return propertyAs(property, parser, false);
  }

  private <V> V propertyAs(String property, Function<String, V> parser, Boolean checkLowercase) {
    String namespacedProperty = withNamespace(property);
    String value = get(namespacedProperty);
    if (value != null) {
      return parser.apply(value);
    } else if (checkLowercase) {
      return propertyAs(property.toLowerCase(), parser, false);
    } else {
      throw new IllegalArgumentException(
          String.format("Property %s required ", namespacedProperty));
    }
  }

  private <V> void set(String property, V value, Function<V, String> asString) {
    if (value == null) {
      throw new IllegalArgumentException(
          String.format("Can't set null property property %s ", property));
    }
    String namespacedProperty = withNamespace(property);
    String valueAsString = asString.apply(value);
    set(namespacedProperty, valueAsString);
  }

  public boolean containsKey(String property) {
    return propertyAsString(property, null) != null;
  }

  public void setBoolean(String property, boolean value) {
    set(property, value, String::valueOf);
  }

  public void setDouble(String property, double value) {
    set(property, value, String::valueOf);
  }

  public void setInt(String property, int value) {
    set(property, value, String::valueOf);
  }

  public void setLong(String property, long value) {
    set(property, value, String::valueOf);
  }

  public void setString(String property, String value) {
    set(property, value, Function.identity());
  }

  public boolean propertyAsBoolean(String property) {
    return propertyAs(property, Boolean::parseBoolean);
  }

  public boolean propertyAsBoolean(String property, boolean defaultValue) {
    return propertyAs(property, defaultValue, Boolean::parseBoolean);
  }

  public double propertyAsDouble(String property) {
    return propertyAs(property, Double::parseDouble);
  }

  public double propertyAsDouble(String property, double defaultValue) {
    return propertyAs(property, defaultValue, Double::parseDouble);
  }

  public int propertyAsInt(String property) {
    return propertyAs(property, Integer::parseInt);
  }

  public int propertyAsInt(String property, int defaultValue) {
    return propertyAs(property, defaultValue, Integer::parseInt);
  }

  public long propertyAsLong(String property) {
    return propertyAs(property, Long::parseLong);
  }

  public long propertyAsLong(String property, long defaultValue) {
    return propertyAs(property, defaultValue, Long::parseLong);
  }

  public String propertyAsString(String property) {
    return propertyAs(property, Function.identity(), true);
  }

  public String propertyAsString(String property, String defaultValue) {
    return propertyAs(property, defaultValue, Function.identity(), true);
  }

  public String namespace() {
    return namespace;
  }
}
