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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class PathMapper implements Serializable {
  enum UnmatchedPolicy {
    PASSTHROUGH,
    FAIL
  }

  private final boolean enabled;
  private final UnmatchedPolicy unmatchedPolicy;
  private final List<MappingRule> canonicalRules;
  private final List<MappingRule> physicalRules;

  PathMapper(boolean enabled, UnmatchedPolicy unmatchedPolicy, List<MappingRule> rules) {
    this.enabled = enabled;
    this.unmatchedPolicy = unmatchedPolicy;
    validateRules(rules);
    this.canonicalRules = new ArrayList<>(rules);
    this.canonicalRules.sort(Comparator.comparingInt(MappingRule::canonicalSpecificity).reversed());
    this.physicalRules = new ArrayList<>(rules);
    this.physicalRules.sort(Comparator.comparingInt(MappingRule::physicalSpecificity).reversed());
  }

  MappingResult toPhysical(String location, boolean rejectPhysicalAlias) {
    if (!enabled) {
      return MappingResult.unmatched(location);
    }

    ParsedLocation parsed = ParsedLocation.parse(location, "canonical location");
    for (MappingRule rule : canonicalRules) {
      String mapped = rule.mapToPhysical(parsed);
      if (mapped != null) {
        return MappingResult.mapped(mapped, rule);
      }
    }

    if (rejectPhysicalAlias) {
      for (MappingRule rule : physicalRules) {
        if (rule.matchesPhysical(parsed)) {
          throw new IllegalArgumentException(
              "Invalid canonical location: physical alias is not allowed in strict mode");
        }
      }
    }

    if (unmatchedPolicy == UnmatchedPolicy.FAIL) {
      throw new IllegalArgumentException(
          "No path alias rule matches canonical location: " + location);
    }

    return MappingResult.unmatched(location);
  }

  MappingResult toCanonical(String location) {
    if (!enabled) {
      return MappingResult.unmatched(location);
    }

    ParsedLocation parsed = ParsedLocation.parse(location, "physical location");
    for (MappingRule rule : physicalRules) {
      String mapped = rule.mapToCanonical(parsed);
      if (mapped != null) {
        return MappingResult.mapped(mapped, rule);
      }
    }

    if (unmatchedPolicy == UnmatchedPolicy.FAIL) {
      throw new IllegalArgumentException(
          "No path alias rule matches physical location: " + location);
    }

    return MappingResult.unmatched(location);
  }

  int ruleCount() {
    return canonicalRules.size();
  }

  private static void validateRules(List<MappingRule> rules) {
    Set<String> canonicalKeys = new HashSet<>();
    Set<String> physicalKeys = new HashSet<>();
    for (MappingRule rule : rules) {
      if (!canonicalKeys.add(rule.canonicalKey())) {
        throw new IllegalArgumentException(
            "Invalid path alias rules: duplicate canonical prefix for rule " + rule.id());
      }

      if (!physicalKeys.add(rule.physicalKey())) {
        throw new IllegalArgumentException(
            "Invalid path alias rules: duplicate physical prefix for rule " + rule.id());
      }
    }

    for (int left = 0; left < rules.size(); left += 1) {
      for (int right = left + 1; right < rules.size(); right += 1) {
        if (rules.get(left).physicalOverlaps(rules.get(right))) {
          throw new IllegalArgumentException(
              "Invalid path alias rules: physical prefixes overlap for rules "
                  + rules.get(left).id()
                  + " and "
                  + rules.get(right).id());
        }
      }
    }

    for (MappingRule canonicalRule : rules) {
      for (MappingRule physicalRule : rules) {
        if (canonicalRule.canonicalOverlapsPhysical(physicalRule)) {
          throw new IllegalArgumentException(
              "Invalid path alias rules: canonical prefix for rule "
                  + canonicalRule.id()
                  + " overlaps physical prefix for rule "
                  + physicalRule.id());
        }
      }
    }
  }
}
