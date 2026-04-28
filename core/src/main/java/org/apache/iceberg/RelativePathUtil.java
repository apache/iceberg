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

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.LocationUtil;

class RelativePathUtil {
  private RelativePathUtil() {}

  /**
   * Returns true if the path has a URI scheme (e.g. {@code s3://}, {@code file:/}, {@code
   * hdfs://}). Per RFC 3986, a scheme ends at the first {@code :} and cannot contain {@code /}, so
   * a colon that appears after a slash is not a scheme delimiter.
   */
  static boolean isAbsolute(String path) {
    if (path == null) {
      return false;
    }

    int colonIndex = path.indexOf(':');
    if (colonIndex <= 0) {
      return false;
    }

    int slashIndex = path.indexOf('/');
    return slashIndex == -1 || slashIndex > colonIndex;
  }

  /**
   * Resolves a relative path against a table location. Relative paths (produced by {@link
   * #relativize}) start with {@code /} and are resolved by direct concatenation with the table
   * location. Absolute paths are returned as-is.
   *
   * <p>Resolution only applies when the table location has a URI scheme. Paths are never resolved
   * against bare local paths.
   */
  static String resolve(String path, String tableLocation) {
    if (path == null || isAbsolute(path) || !isAbsolute(tableLocation)) {
      return path;
    }

    Preconditions.checkArgument(path.startsWith("/"), "Relative path must start with /: %s", path);

    return LocationUtil.stripTrailingSlash(tableLocation) + path;
  }

  /**
   * Relativizes a path against a table location. If the path starts with the table location, the
   * table location prefix is stripped, leaving a relative path that starts with {@code /}. If the
   * path is not under the table location, it is returned as-is.
   *
   * <p>Relativization only applies when both the path and table location have URI schemes.
   */
  static String relativize(String path, String tableLocation) {
    if (path == null || !isAbsolute(tableLocation)) {
      return path;
    }

    String normalized = LocationUtil.stripTrailingSlash(tableLocation);
    if (path.startsWith(normalized + "/")) {
      return path.substring(normalized.length());
    }

    return path;
  }
}
