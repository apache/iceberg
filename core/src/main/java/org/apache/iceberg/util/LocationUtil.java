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

  /** Returns true if the path is an absolute URI (contains a scheme like {@code s3://}). */
  public static boolean isAbsolute(String path) {
    return path != null && path.contains("://");
  }

  /**
   * Resolves a path against a table location. Relative paths (produced by {@link #relativize}) are
   * resolved by direct concatenation with the table location. Absolute paths are returned as-is.
   *
   * <p>Resolution only applies when the table location has a URI scheme. Paths are never resolved
   * against bare local paths.
   */
  public static String resolve(String path, String tableLocation) {
    if (isAbsolute(path) || tableLocation == null || !isAbsolute(tableLocation)) {
      return path;
    }

    return tableLocation + path;
  }

  /**
   * Relativizes a path against a table location. If the path starts with the table location, the
   * table location prefix is stripped, leaving a relative path that starts with {@code /}. If the
   * path is not under the table location, it is returned as-is.
   *
   * <p>Relativization only applies when both the path and table location have URI schemes.
   */
  public static String relativize(String path, String tableLocation) {
    if (path != null
        && tableLocation != null
        && isAbsolute(tableLocation)
        && path.startsWith(tableLocation + "/")) {
      return path.substring(tableLocation.length());
    }

    return path;
  }
}
