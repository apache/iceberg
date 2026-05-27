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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

public class PathUtil {
  private PathUtil() {}

  /**
   * One step in a variant JSONPath: an object member name or a zero-based array index (RFC 9535
   * {@code [n]} selector).
   *
   * <p>This is a copy of the canonical implementation from <a
   * href="https://github.com/apache/iceberg/pull/15384">PR #15384</a>. Once that PR merges, this
   * class will be the single definition and this note can be removed.
   */
  public sealed interface PathSegment permits PathSegment.Name, PathSegment.Index {
    record Name(String name) implements PathSegment {}

    record Index(int index) implements PathSegment {}
  }

  private static final String RFC9535_NAME_FIRST =
      "[A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]";
  private static final String RFC9535_NAME_CHARS =
      "[0-9A-Za-z_\\x{0080}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]*";
  private static final Predicate<String> RFC9535_MEMBER_NAME_SHORTHAND =
      Pattern.compile(RFC9535_NAME_FIRST + RFC9535_NAME_CHARS).asMatchPredicate();

  /** Letters that follow {@code \} for control-character escapes in RFC 9535 quoted segments. */
  private static final String RFC9535_SIMPLE_ESCAPE_LETTERS = "btnfr";

  private static final String RFC9535_SIMPLE_ESCAPE_CHARS = "\b\t\n\f\r";

  private static final Pattern RFC9535_REQUIRES_ESCAPE =
      Pattern.compile(
          "[^\\x{0020}-\\x{0026}\\x{0028}-\\x{005B}\\x{005D}-\\x{D7FF}\\x{E000}-\\x{10FFFF}]");

  /**
   * Matches one bracket segment {@code ['...']} where inner text may contain RFC 9535 escapes
   * (quote, backslash, control characters, and four-digit hex escapes).
   */
  private static final Pattern BRACKET_SEGMENT = Pattern.compile("\\['((?:[^'\\\\]|\\\\.)*)'\\]");

  private static final Map<Character, String> RFC9535_ESCAPE_REPLACEMENTS = buildReplacementMap();

  private static final String ROOT = "$";

  /**
   * Parses a path into segments. After the root {@code $}, each segment is either dot shorthand
   * ({@code .name} per RFC 9535), a single-quoted bracket name ({@code ['...']}) with RFC 9535
   * escapes, or a numeric array index ({@code [n]}). Forms may be mixed (e.g. {@code $.a['b.c']},
   * {@code $.items[0].tags}, {@code $.matrix[0][1]}). Wildcards and recursive descent are not
   * supported.
   *
   * <p>The root path {@code $} yields an empty segment list.
   */
  public static List<PathSegment> parse(String path) {
    Preconditions.checkArgument(path != null, "Invalid path: null");
    Preconditions.checkArgument(!path.isEmpty(), "Invalid path: empty");
    Preconditions.checkArgument(
        path.startsWith(ROOT), "Invalid path, does not start with %s: %s", ROOT, path);

    if (path.equals(ROOT)) {
      return Lists.newArrayList();
    }

    return parseAfterRoot(path);
  }

  /**
   * Parses a JSON path into navigation steps for shredded variant extraction. Object field steps
   * are plain strings; array index steps use {@code "[n]"} encoding (see {@link
   * #isArrayIndexPart}).
   */
  public static List<String> parseObjectPath(String path) {
    List<String> parts = Lists.newArrayList();
    for (PathSegment segment : parse(path)) {
      if (segment instanceof PathSegment.Name) {
        parts.add(((PathSegment.Name) segment).name());
      } else if (segment instanceof PathSegment.Index) {
        parts.add("[" + ((PathSegment.Index) segment).index() + "]");
      } else {
        throw new IllegalStateException("Unknown segment: " + segment);
      }
    }
    return parts;
  }

  /**
   * Returns true when {@code part} is an array index step encoded as {@code "[n]"} by {@link
   * #parseObjectPath}.
   */
  public static boolean isArrayIndexPart(String part) {
    if (part.length() < 3 || part.charAt(0) != '[' || part.charAt(part.length() - 1) != ']') {
      return false;
    }
    for (int i = 1; i < part.length() - 1; i++) {
      if (!Character.isDigit(part.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /** Returns the array index from a {@code "[n]"} part produced by {@link #parseObjectPath}. */
  public static int parseArrayIndexPart(String part) {
    Preconditions.checkArgument(isArrayIndexPart(part), "Invalid array index part: %s", part);
    return Integer.parseInt(part.substring(1, part.length() - 1));
  }

  /** Normalizes object field names only (no array indices). */
  public static String toNormalizedPath(Iterable<String> fields) {
    return toNormalizedPath(
        Streams.stream(fields).map(PathSegment.Name::new).collect(Collectors.toList()));
  }

  static String toNormalizedPath(List<PathSegment> segments) {
    StringBuilder builder = new StringBuilder(ROOT);
    for (PathSegment segment : segments) {
      if (segment instanceof PathSegment.Name) {
        String name = ((PathSegment.Name) segment).name();
        builder.append("['").append(rfc9535escape(name)).append("']");
      } else if (segment instanceof PathSegment.Index) {
        int index = ((PathSegment.Index) segment).index();
        Preconditions.checkArgument(index >= 0, "Invalid path, negative array index: %s", index);
        builder.append('[').append(index).append(']');
      } else {
        throw new IllegalStateException("Unknown segment: " + segment);
      }
    }
    return builder.toString();
  }

  private static List<PathSegment> parseAfterRoot(String path) {
    List<PathSegment> segments = Lists.newArrayList();
    Matcher bracketMatcher = BRACKET_SEGMENT.matcher(path);
    int len = path.length();
    int pos = ROOT.length();

    while (pos < len) {
      char ch = path.charAt(pos);
      pos =
          switch (ch) {
            case '.' -> appendDotSegment(segments, path, pos);
            case '[' -> appendBracketOrIndexSegment(segments, path, pos, bracketMatcher);
            default ->
                throw new IllegalArgumentException(
                    String.format(
                        "Invalid path, expected '.' or '[' at position %s: %s", pos, path));
          };
    }

    return segments;
  }

  /**
   * Appends a dot-style segment to {@code segments} by reading from {@code path[dotPos]}: a single
   * leading {@code .} then an RFC 9535 shorthand name until the next {@code .} or {@code [}.
   *
   * @param segments output; segments parsed so far, updated in place
   * @param path full path
   * @param dotPos index of the {@code .} starting the segment
   */
  private static int appendDotSegment(List<PathSegment> segments, String path, int dotPos) {
    int pos = dotPos + 1;
    int pathLen = path.length();
    Preconditions.checkArgument(pos < pathLen, "Invalid path, trailing dot: %s", path);
    int start = pos;
    while (pos < pathLen) {
      char ch = path.charAt(pos);
      if (ch == '.' || ch == '[') {
        break;
      }
      pos++;
    }

    Preconditions.checkArgument(pos > start, "Invalid path, empty segment after '.': %s", path);
    String name = path.substring(start, pos);
    Preconditions.checkArgument(
        RFC9535_MEMBER_NAME_SHORTHAND.test(name),
        "Invalid path: %s (%s has invalid characters)",
        path,
        name);
    segments.add(new PathSegment.Name(name));
    return pos;
  }

  /**
   * Appends a bracket segment to {@code segments} starting at {@code path[bracketPos]}. If the next
   * character is a digit, consumes a numeric array index {@code [n]}; otherwise consumes a quoted
   * name {@code ['...']}. A lone {@code [} with no following quoted form (e.g. the path ends at
   * {@code $[}) is rejected in {@link #appendQuotedBracketSegment} when the pattern does not match.
   *
   * @param segments output; segments parsed so far, updated in place
   * @param path full path
   * @param bracketPos index of the opening {@code [}
   */
  private static int appendBracketOrIndexSegment(
      List<PathSegment> segments, String path, int bracketPos, Matcher bracketMatcher) {
    Preconditions.checkArgument(
        bracketPos < path.length() && path.charAt(bracketPos) == '[', "Invalid path: %s", path);
    if (bracketPos + 1 < path.length() && isAsciiDigit(path.charAt(bracketPos + 1))) {
      return appendArrayIndexSegment(segments, path, bracketPos);
    }
    return appendQuotedBracketSegment(segments, path, bracketPos, bracketMatcher);
  }

  private static boolean isAsciiDigit(char ch) {
    return ch >= '0' && ch <= '9';
  }

  /**
   * Appends a non-negative array index from {@code [n]} to {@code segments}, starting with {@code
   * [} at {@code path[bracketPos]}.
   *
   * @param segments output; segments parsed so far, updated in place
   * @param path full path
   * @param bracketPos index of the opening {@code [} before the digits
   */
  private static int appendArrayIndexSegment(
      List<PathSegment> segments, String path, int bracketPos) {
    int pos = bracketPos + 1;
    int len = path.length();
    int start = pos;
    while (pos < len && isAsciiDigit(path.charAt(pos))) {
      pos++;
    }
    Preconditions.checkArgument(pos > start, "Invalid path, empty array index in: %s", path);
    Preconditions.checkArgument(
        pos < len && path.charAt(pos) == ']', "Invalid path, unclosed array index in: %s", path);
    int index;
    String digits = path.substring(start, pos);
    try {
      index = Integer.parseInt(digits);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Invalid path, array index out of int range: %s", path), e);
    }
    Preconditions.checkArgument(index >= 0, "Invalid path, negative array index in: %s", path);
    segments.add(new PathSegment.Index(index));
    return pos + 1;
  }

  /**
   * Appends a name from a {@code ['...']} segment to {@code segments} using the bracket matcher
   * (inner text may use RFC 9535 escapes). Expects a full quoted bracket token at {@code
   * path[bracketPos]}; otherwise the matcher or alignment checks throw.
   *
   * @param segments output; segments parsed so far, updated in place
   * @param path full path
   * @param bracketPos index of the opening {@code [} that must begin {@code ['}
   */
  private static int appendQuotedBracketSegment(
      List<PathSegment> segments, String path, int bracketPos, Matcher bracketMatcher) {
    Preconditions.checkArgument(
        bracketMatcher.find(bracketPos), "Invalid path, malformed bracket segment: %s", path);
    Preconditions.checkArgument(
        bracketMatcher.start() == bracketPos,
        "Invalid path, unexpected characters at position %s: %s",
        bracketPos,
        path);
    segments.add(new PathSegment.Name(rfc9535unescape(bracketMatcher.group(1))));
    return bracketMatcher.end();
  }

  /** Unescapes the inner text of a {@code ['...']} segment (inverse of {@link #rfc9535escape}). */
  @VisibleForTesting
  @SuppressWarnings("StatementSwitchToExpressionSwitch")
  static String rfc9535unescape(String escaped) {
    if (!escaped.contains("\\")) {
      return escaped;
    }

    StringBuilder builder = new StringBuilder(escaped.length());
    int cursor = 0;
    while (cursor < escaped.length()) {
      char ch = escaped.charAt(cursor);
      if (ch != '\\') {
        builder.append(ch);
        cursor += 1;
      } else {
        Preconditions.checkArgument(
            cursor + 1 < escaped.length(), "Invalid escape sequence at end of: %s", escaped);
        char next = escaped.charAt(cursor + 1);
        switch (next) {
          case 'u':
            Preconditions.checkArgument(
                cursor + 5 < escaped.length(),
                "Invalid \\uXXXX escape at position %s in: %s",
                cursor,
                escaped);
            builder.append((char) Integer.parseInt(escaped.substring(cursor + 2, cursor + 6), 16));
            cursor += 6;
            break;
          case 'b':
          case 't':
          case 'f':
          case 'n':
          case 'r':
          case '\'':
          case '\\':
            builder.append(rfc9535SimpleEscapedChar(next));
            cursor += 2;
            break;
          default:
            throw new IllegalArgumentException(
                "Invalid escape sequence \\" + next + " in: " + escaped);
        }
      }
    }

    return builder.toString();
  }

  private static char rfc9535SimpleEscapedChar(char next) {
    int idx = RFC9535_SIMPLE_ESCAPE_LETTERS.indexOf(next);
    if (idx >= 0) {
      return RFC9535_SIMPLE_ESCAPE_CHARS.charAt(idx);
    }
    if (next == '\'') {
      return '\'';
    }
    if (next == '\\') {
      return '\\';
    }
    throw new IllegalArgumentException("Invalid simple escape: \\" + next);
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
