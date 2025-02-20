/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */
package org.apache.iceberg.expressions;

import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;

class PathUtil {
  private PathUtil() {}

  private static final String RFC9535_NAME_FIRST =
      "[A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]";
  private static final String RFC9535_NAME_CHARS =
      "[0-9A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]*";
  private static final Predicate<String> RFC9535_MEMBER_NAME_SHORTHAND =
      Pattern.compile(RFC9535_NAME_FIRST + RFC9535_NAME_CHARS).asMatchPredicate();

  private static final Splitter DOT = Splitter.on(".");
  private static final String ROOT = "$";

  static List<String> parse(String path) {
    Preconditions.checkArgument(path != null, "Invalid path: null");
    Preconditions.checkArgument(
        !path.contains("[") && !path.contains("]"), "Unsupported path, contains bracket: %s", path);
    Preconditions.checkArgument(
        !path.contains("*"), "Unsupported path, contains wildcard: %s", path);
    Preconditions.checkArgument(
        !path.contains(".."), "Unsupported path, contains recursive descent: %s", path);

    List<String> parts = DOT.splitToList(path);
    Preconditions.checkArgument(
        ROOT.equals(parts.get(0)), "Invalid path, does not start with %s: %s", ROOT, path);

    List<String> names = parts.subList(1, parts.size());
    for (String name : names) {
      Preconditions.checkArgument(
          RFC9535_MEMBER_NAME_SHORTHAND.test(name),
          "Invalid path: %s (%s has invalid characters)",
          path,
          name);
    }

    return names;
  }
}
