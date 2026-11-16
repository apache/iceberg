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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.Objects;

final class ParsedLocation implements Serializable {
  private final String original;
  private final String scheme;
  private final String authority;
  private final String rawPath;

  private ParsedLocation(String original, String scheme, String authority, String rawPath) {
    this.original = original;
    this.scheme = scheme;
    this.authority = authority;
    this.rawPath = rawPath;
  }

  static ParsedLocation parse(String location, String description) {
    if (location == null || location.isEmpty()) {
      throw new IllegalArgumentException("Invalid " + description + ": location is empty");
    }

    URI uri = toUri(location, description);
    validateHierarchicalUri(uri, description);

    String normalizedScheme = uri.getScheme().toLowerCase(Locale.ROOT);
    String rawAuthority = uri.getRawAuthority();
    String path = uri.getRawPath() == null ? "" : uri.getRawPath();
    String decodedPath = uri.getPath() == null ? "" : uri.getPath();
    validatePath(decodedPath, description);

    return new ParsedLocation(location, normalizedScheme, rawAuthority, path);
  }

  private static URI toUri(String location, String description) {
    try {
      return new URI(location);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          "Invalid " + description + " URI: " + sanitize(location), e);
    }
  }

  private static void validateHierarchicalUri(URI uri, String description) {
    if (!uri.isAbsolute() || uri.isOpaque()) {
      throw new IllegalArgumentException(
          "Invalid " + description + " URI: must be an absolute hierarchical URI");
    }

    if (uri.getRawQuery() != null) {
      throw new IllegalArgumentException("Invalid " + description + " URI: query is not allowed");
    }

    if (uri.getRawFragment() != null) {
      throw new IllegalArgumentException(
          "Invalid " + description + " URI: fragment is not allowed");
    }

    String normalizedScheme = uri.getScheme().toLowerCase(Locale.ROOT);
    String rawAuthority = uri.getRawAuthority();
    if (rawAuthority == null && !"file".equals(normalizedScheme)) {
      throw new IllegalArgumentException("Invalid " + description + " URI: authority is required");
    }

    if (uri.getRawUserInfo() != null || (rawAuthority != null && rawAuthority.contains("@"))) {
      throw new IllegalArgumentException(
          "Invalid " + description + " URI: user info is not allowed");
    }
  }

  private static void validatePath(String path, String description) {
    if (path.indexOf('\\') >= 0) {
      throw new IllegalArgumentException(
          "Invalid " + description + " URI: backslash path separators are not allowed");
    }

    String[] segments = path.split("/", -1);
    for (String segment : segments) {
      if (".".equals(segment) || "..".equals(segment)) {
        throw new IllegalArgumentException(
            "Invalid " + description + " URI: path traversal is not allowed");
      }
    }
  }

  String original() {
    return original;
  }

  String scheme() {
    return scheme;
  }

  String authority() {
    return authority;
  }

  String rawPath() {
    return rawPath;
  }

  String authorityKey() {
    return authority == null ? "" : authority.toLowerCase(Locale.ROOT);
  }

  String withRawPath(String newRawPath) {
    if (authority == null) {
      return scheme + ":" + newRawPath;
    }

    return scheme + "://" + authority + newRawPath;
  }

  boolean hasSameEndpoint(ParsedLocation other) {
    return scheme.equals(other.scheme) && authorityKey().equals(other.authorityKey());
  }

  private static String sanitize(String location) {
    int query = location.indexOf('?');
    return query >= 0 ? location.substring(0, query) + "?<redacted>" : location;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (!(other instanceof ParsedLocation)) {
      return false;
    }

    ParsedLocation that = (ParsedLocation) other;
    return scheme.equals(that.scheme)
        && authorityKey().equals(that.authorityKey())
        && rawPath.equals(that.rawPath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scheme, authorityKey(), rawPath);
  }
}
