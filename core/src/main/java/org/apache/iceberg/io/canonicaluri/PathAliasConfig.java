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
package org.apache.iceberg.io.canonicaluri;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class PathAliasConfig implements Serializable {
  private static final Pattern RULE_PROPERTY =
      Pattern.compile("^pathalias\\.rule\\.(\\d+)\\.(canonical|physical|id)$");

  private final boolean enabled;
  private final String delegateImpl;
  private final boolean metricsEnabled;
  private final boolean debugLogPhysicalPaths;
  private final PathMapper mapper;

  private PathAliasConfig(
      boolean enabled,
      String delegateImpl,
      boolean metricsEnabled,
      boolean debugLogPhysicalPaths,
      PathMapper mapper) {
    this.enabled = enabled;
    this.delegateImpl = delegateImpl;
    this.metricsEnabled = metricsEnabled;
    this.debugLogPhysicalPaths = debugLogPhysicalPaths;
    this.mapper = mapper;
  }

  static PathAliasConfig from(Map<String, String> properties) {
    boolean enabled =
        booleanProperty(
            properties, PathAliasProperties.ENABLED, PathAliasProperties.ENABLED_DEFAULT);
    boolean metricsEnabled =
        booleanProperty(
            properties,
            PathAliasProperties.METRICS_ENABLED,
            PathAliasProperties.METRICS_ENABLED_DEFAULT);
    boolean debugLogPhysicalPaths =
        booleanProperty(
            properties,
            PathAliasProperties.DEBUG_LOG_PHYSICAL_PATHS,
            PathAliasProperties.DEBUG_LOG_PHYSICAL_PATHS_DEFAULT);
    boolean readFailover =
        booleanProperty(
            properties,
            PathAliasProperties.READ_FAILOVER,
            PathAliasProperties.READ_FAILOVER_DEFAULT);
    if (readFailover) {
      throw new IllegalArgumentException(
          "Invalid pathalias.read-failover: read failover is not implemented");
    }

    String writeMode =
        properties.getOrDefault(
            PathAliasProperties.WRITE_MODE, PathAliasProperties.WRITE_MODE_DEFAULT);
    if (!"strict".equalsIgnoreCase(writeMode)) {
      throw new IllegalArgumentException(
          "Invalid pathalias.write-mode: only strict mode is supported");
    }

    PathMapper.UnmatchedPolicy unmatchedPolicy = unmatchedPolicy(properties);
    List<MappingRule> rules = rules(properties);
    if (enabled && rules.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid path alias configuration: at least one mapping rule is required");
    }

    String delegateImpl =
        properties.getOrDefault(
            PathAliasProperties.DELEGATE_IMPL, PathAliasProperties.DELEGATE_IMPL_DEFAULT);
    if (delegateImpl.isEmpty()) {
      throw new IllegalArgumentException("Invalid pathalias.delegate.impl: value is empty");
    }

    return new PathAliasConfig(
        enabled,
        delegateImpl,
        metricsEnabled,
        debugLogPhysicalPaths,
        new PathMapper(enabled, unmatchedPolicy, rules));
  }

  boolean enabled() {
    return enabled;
  }

  String delegateImpl() {
    return delegateImpl;
  }

  boolean metricsEnabled() {
    return metricsEnabled;
  }

  boolean debugLogPhysicalPaths() {
    return debugLogPhysicalPaths;
  }

  PathMapper mapper() {
    return mapper;
  }

  private static PathMapper.UnmatchedPolicy unmatchedPolicy(Map<String, String> properties) {
    String value =
        properties
            .getOrDefault(PathAliasProperties.UNMATCHED, PathAliasProperties.UNMATCHED_DEFAULT)
            .toLowerCase(Locale.ROOT);
    if ("passthrough".equals(value)) {
      return PathMapper.UnmatchedPolicy.PASSTHROUGH;
    } else if ("fail".equals(value)) {
      return PathMapper.UnmatchedPolicy.FAIL;
    }

    throw new IllegalArgumentException(
        "Invalid pathalias.unmatched: expected passthrough or fail, found " + value);
  }

  private static List<MappingRule> rules(Map<String, String> properties) {
    Map<Integer, RuleProperties> rulesByIndex = new TreeMap<>();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      Matcher matcher = RULE_PROPERTY.matcher(entry.getKey());
      if (matcher.matches()) {
        int index = Integer.parseInt(matcher.group(1));
        RuleProperties rule = rulesByIndex.computeIfAbsent(index, ignored -> new RuleProperties());
        rule.set(matcher.group(2), entry.getValue());
      }
    }

    List<MappingRule> rules = new ArrayList<>();
    for (Map.Entry<Integer, RuleProperties> entry : rulesByIndex.entrySet()) {
      int index = entry.getKey();
      RuleProperties values = entry.getValue();
      if (values.canonical == null || values.physical == null) {
        throw new IllegalArgumentException(
            "Invalid path alias rule " + index + ": canonical and physical are required");
      }

      rules.add(
          new MappingRule(
              values.id == null ? Integer.toString(index) : values.id,
              values.canonical,
              values.physical));
    }

    return rules;
  }

  private static boolean booleanProperty(
      Map<String, String> properties, String property, boolean defaultValue) {
    String value = properties.get(property);
    if (value == null) {
      return defaultValue;
    }

    if ("true".equalsIgnoreCase(value)) {
      return true;
    } else if ("false".equalsIgnoreCase(value)) {
      return false;
    }

    throw new IllegalArgumentException(
        "Invalid " + property + ": expected true or false, found " + value);
  }

  private static class RuleProperties implements Serializable {
    private String canonical;
    private String physical;
    private String id;

    private void set(String property, String value) {
      if ("canonical".equals(property)) {
        this.canonical = value;
      } else if ("physical".equals(property)) {
        this.physical = value;
      } else {
        this.id = value;
      }
    }
  }
}
