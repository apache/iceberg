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

class RelativePathUtil {
  private static final int MAX_SCHEME_LENGTH = 10;

  private RelativePathUtil() {}

  /**
   * Returns true if the path contains a URI scheme (e.g. {@code s3:}, {@code hdfs:}, {@code
   * file:}). Checks at most the first 10 characters for a {@code :} preceded by alphanumeric
   * characters, per <a href="https://datatracker.ietf.org/doc/html/rfc3986#section-3.1">RFC 3986
   * section 3.1</a>.
   */
  static boolean hasScheme(String path) {
    if (path == null) {
      return false;
    }

    int limit = Math.min(path.length(), MAX_SCHEME_LENGTH);
    for (int i = 0; i < limit; i += 1) {
      char ch = path.charAt(i);
      if (ch == ':') {
        return i > 0;
      }

      if (!Character.isLetterOrDigit(ch)) {
        return false;
      }
    }

    return false;
  }

  /**
   * Resolves a relative path against a table location. If the path has a URI scheme, it is returned
   * as-is. Otherwise, the path is appended to the table location without any additional separator.
   */
  static String resolve(String path, String tableLocation) {
    Preconditions.checkArgument(tableLocation != null, "Table location must not be null");
    if (path == null || hasScheme(path)) {
      return path;
    }

    return tableLocation + path;
  }

  /**
   * Relativizes a path against a table location. If the path starts with the table location, the
   * prefix is removed and the remaining relative portion is returned. Otherwise, the path is
   * returned as-is.
   */
  static String relativize(String path, String tableLocation) {
    Preconditions.checkArgument(tableLocation != null, "Table location must not be null");
    if (path == null) {
      return null;
    }

    if (path.startsWith(tableLocation)) {
      return path.substring(tableLocation.length());
    }

    return path;
  }
}
