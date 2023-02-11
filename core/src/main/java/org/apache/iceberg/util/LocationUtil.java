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

import java.nio.file.Paths;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class LocationUtil {
  private static final String SCHEME_DELIM = "://";
  private static final String PATH_DELIM = "/";

  private LocationUtil() {}

  public static String stripTrailingSlash(String path) {
    Preconditions.checkArgument(
        path != null && path.length() > 0, "path must not be null or empty");

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  public static String posixNormalize(String path) {
    String[] schemeSplit = path.split(SCHEME_DELIM, -1);
    ValidationException.check(
        schemeSplit.length <= 2, // length is at least 1 in a split
        "Invalid path, must only contain at most 1 scheme: %s",
        path);
    if (schemeSplit.length == 1) {
      return Paths.get(path).normalize().toString();
    }

    return schemeSplit[0] + SCHEME_DELIM + Paths.get(schemeSplit[1]).normalize().toString();
  }
}
