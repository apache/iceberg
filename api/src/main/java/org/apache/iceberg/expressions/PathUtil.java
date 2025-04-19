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
package org.apache.iceberg.expressions;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

public class PathUtil {
  private PathUtil() {}

  private static final String RFC9535_NAME_FIRST =
      "[A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]";
  private static final String RFC9535_NAME_CHARS =
      "[0-9A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]*";
  private static final Predicate<String> RFC9535_MEMBER_NAME_SHORTHAND =
      Pattern.compile(RFC9535_NAME_FIRST + RFC9535_NAME_CHARS).asMatchPredicate();

  private static final Pattern RFC9535_REQUIRES_ESCAPE =
      Pattern.compile(
          "[^\\x{0020}-\\x{0026}\\x{0028}-\\x{005B}\\x{005D}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]");

  private static final Map<Character, String> RFC9535_ESCAPE_REPLACEMENTS = buildReplacementMap();

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

  public static String toNormalizedPath(Iterable<String> fields) {
    return ROOT
        + Streams.stream(fields)
            .map(PathUtil::rfc9535escape)
            .map(name -> "['" + name + "']")
            .collect(Collectors.joining(""));
  }

  @VisibleForTesting
  static String rfc9535escape(String name) {
    StringBuilder builder = new StringBuilder();
    Matcher matcher = RFC9535_REQUIRES_ESCAPE.matcher(name);
    while (matcher.find()) {
      matcher.appendReplacement(builder, replacement(matcher.group()));
    }

    matcher.appendTail(builder);

    return builder.toString();
  }

  private static String replacement(String esc) {
    String replacement = RFC9535_ESCAPE_REPLACEMENTS.get(esc.charAt(0));
    if (replacement != null) {
      return replacement;
    }

    throw new IllegalArgumentException("Cannot escape for normalized path: " + esc);
  }

  @SuppressWarnings("DefaultLocale")
  private static Map<Character, String> buildReplacementMap() {
    ImmutableMap.Builder<Character, String> builder = ImmutableMap.builder();

    // replacements are double-escaped to pass correctly through appendReplacement
    builder.put('\b', "\\\\b");
    builder.put('\t', "\\\\t");
    builder.put('\f', "\\\\f");
    builder.put('\n', "\\\\n");
    builder.put('\r', "\\\\r");
    builder.put('\'', "\\\\'");
    builder.put('\\', "\\\\\\\\");

    // RFC9535 normal-hexchar: add control chars that are escaped as hex
    Set<Character> specialEscapeChars = Set.of('\b', '\t', '\f', '\n', '\r');
    for (char ch = 0; ch <= 0x1F; ch = (char) (ch + 1)) {
      // add all escaped chars to the map
      if (!specialEscapeChars.contains(ch)) {
        builder.put(ch, String.format("\\\\u%04x", (int) ch));
      }
    }

    return builder.build();
  }
}
