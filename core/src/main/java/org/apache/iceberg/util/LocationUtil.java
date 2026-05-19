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

import java.util.UUID;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

public class LocationUtil {
  private LocationUtil() {}

  public static String stripTrailingSlash(String path) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "path must not be null or empty");

    String result = path;
    while (!result.endsWith("://") && result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  /**
   * Returns a path component derived from the {@code tableIdentifier}, used as part of the table
   * location URI.
   *
   * <p>If {@code useUniqueLocation} is {@code true}, the returned component will include a random
   * UUID suffix. Otherwise, the plain table name is returned.
   *
   * @param tableIdentifier Iceberg table identifier
   * @param useUniqueLocation whether to ensure uniqueness
   * @return a string representing the table name component for a location URI
   */
  public static String tableLocation(TableIdentifier tableIdentifier, boolean useUniqueLocation) {
    Preconditions.checkArgument(null != tableIdentifier, "Invalid identifier: null");

    if (useUniqueLocation) {
      String uniqueSuffix = UUID.randomUUID().toString().replace("-", "");
      return String.format("%s-%s", tableIdentifier.name(), uniqueSuffix);
    } else {
      return tableIdentifier.name();
    }
  }

  /**
   * Returns true if the location contains a URI scheme (e.g. {@code s3:}, {@code hdfs:}, {@code
   * file:}), per <a href="https://datatracker.ietf.org/doc/html/rfc3986#section-3.1">RFC 3986
   * section 3.1</a>.
   */
  private static boolean hasScheme(String location) {
    if (location.isEmpty()) {
      return false;
    }

    // Early termination for relative locations since most commonly start with /
    if (location.charAt(0) == '/') {
      return false;
    }

    for (int i = 0; i < location.length(); i += 1) {
      char ch = location.charAt(i);
      if (ch == ':') {
        return i > 0;
      }

      if (!Character.isLetterOrDigit(ch) && ch != '+' && ch != '-' && ch != '.') {
        return false;
      }
    }

    return false;
  }

  /**
   * Resolves a location against a table location. If the location has a URI scheme, it is returned
   * as-is. Otherwise, the table location and the relative location are joined by the URI separator
   * character {@code /}.
   */
  public static String resolveLocation(String tableLocation, String location) {
    if (hasScheme(location)) {
      return location;
    }

    return tableLocation + "/" + location;
  }

  /**
   * Relativizes a location against a table location. If the location starts with the table location
   * immediately followed by the URI separator character {@code /}, the prefix and separator are
   * removed and the remaining relative portion is returned. Otherwise, the location is returned
   * as-is.
   */
  public static String relativizeLocation(String tableLocation, String location) {
    int prefixLength = tableLocation.length();
    if (location.length() > prefixLength
        && location.charAt(prefixLength) == '/'
        && location.startsWith(tableLocation)) {
      return location.substring(prefixLength + 1);
    }

    return location;
  }
}
