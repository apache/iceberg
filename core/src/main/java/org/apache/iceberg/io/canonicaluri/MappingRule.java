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

final class MappingRule implements Serializable {
  private final String id;
  private final ParsedLocation canonical;
  private final ParsedLocation physical;
  private final String canonicalBasePath;
  private final String physicalBasePath;

  MappingRule(String id, String canonicalPrefix, String physicalPrefix) {
    if (id == null || id.isEmpty()) {
      throw new IllegalArgumentException("Invalid mapping rule: ID is empty");
    }

    this.id = id;
    this.canonical = ParsedLocation.parse(canonicalPrefix, "canonical prefix for rule " + id);
    this.physical = ParsedLocation.parse(physicalPrefix, "physical prefix for rule " + id);
    this.canonicalBasePath = basePath(canonical.rawPath());
    this.physicalBasePath = basePath(physical.rawPath());
  }

  String id() {
    return id;
  }

  int canonicalSpecificity() {
    return canonicalBasePath.length();
  }

  int physicalSpecificity() {
    return physicalBasePath.length();
  }

  String canonicalKey() {
    return key(canonical, canonicalBasePath);
  }

  String physicalKey() {
    return key(physical, physicalBasePath);
  }

  boolean physicalOverlaps(MappingRule other) {
    return physical.hasSameEndpoint(other.physical)
        && pathsOverlap(physicalBasePath, other.physicalBasePath);
  }

  boolean canonicalOverlapsPhysical(MappingRule other) {
    return canonical.hasSameEndpoint(other.physical)
        && pathsOverlap(canonicalBasePath, other.physicalBasePath);
  }

  String mapToPhysical(ParsedLocation location) {
    String suffix = suffix(canonical, canonicalBasePath, location);
    return suffix == null ? null : physical.withRawPath(join(physicalBasePath, suffix, location));
  }

  String mapToCanonical(ParsedLocation location) {
    String suffix = suffix(physical, physicalBasePath, location);
    return suffix == null ? null : canonical.withRawPath(join(canonicalBasePath, suffix, location));
  }

  boolean matchesPhysical(ParsedLocation location) {
    return suffix(physical, physicalBasePath, location) != null;
  }

  private static String suffix(
      ParsedLocation prefix, String prefixBasePath, ParsedLocation location) {
    if (!prefix.hasSameEndpoint(location)) {
      return null;
    }

    String locationPath = location.rawPath();
    if (prefixBasePath.isEmpty()) {
      return locationPath.startsWith("/") ? locationPath.substring(1) : locationPath;
    }

    if (locationPath.equals(prefixBasePath)) {
      return "";
    }

    String prefixWithSeparator = prefixBasePath + "/";
    return locationPath.startsWith(prefixWithSeparator)
        ? locationPath.substring(prefixWithSeparator.length())
        : null;
  }

  private static String join(String targetBasePath, String suffix, ParsedLocation sourceLocation) {
    if (suffix.isEmpty()) {
      if (sourceLocation.rawPath().endsWith("/") && !targetBasePath.endsWith("/")) {
        return targetBasePath + "/";
      }

      return targetBasePath;
    }

    if (targetBasePath.isEmpty() || "/".equals(targetBasePath)) {
      return "/" + suffix;
    }

    return targetBasePath + "/" + suffix;
  }

  private static String basePath(String path) {
    if (path.isEmpty() || "/".equals(path)) {
      return "";
    }

    int end = path.length();
    while (end > 1 && path.charAt(end - 1) == '/') {
      end -= 1;
    }

    return path.substring(0, end);
  }

  private static boolean pathsOverlap(String left, String right) {
    if (left.isEmpty() || right.isEmpty()) {
      return true;
    }

    return left.equals(right) || left.startsWith(right + "/") || right.startsWith(left + "/");
  }

  private static String key(ParsedLocation location, String path) {
    return location.scheme() + "|" + location.authorityKey() + "|" + path;
  }
}
