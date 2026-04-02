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

import static org.apache.iceberg.expressions.PathUtil.PathSegment.Index;
import static org.apache.iceberg.expressions.PathUtil.PathSegment.Name;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

@SuppressWarnings({"AvoidEscapedUnicodeCharacters", "IllegalTokenText"})
public class TestPathUtil {

  private static List<PathUtil.PathSegment> names(String... names) {
    return java.util.Arrays.stream(names).map(Name::new).collect(Collectors.toList());
  }

  @Test
  public void testSimplePath() {
    assertThat(PathUtil.parse("$.event.id")).isEqualTo(names("event", "id"));
  }

  private static final String[] VALID_PATHS =
      new String[] {
        "$", // root path
        "$.event_id",
        "$.event.id",
        "$['event_id']", // bracket form
        "$.event['x.y']", // mixed: dot then bracket
        "$['event']['id']", // bracket then bracket
        "$['a'].b", // bracket then dot
        "$.\u2603", // snowman
        "$.\uD834\uDD1E", // surrogate pair, U+1D11E
        "$.matrix[0][1]",
        "$.basket[0][2].a",
        "$.items[0].tags[1]",
        "$['matrix'][0][1]",
        "$['items'][0]['tags'][1]",
        "$['basket'][0][2]['a']",
      };

  @ParameterizedTest
  @FieldSource("VALID_PATHS")
  public void testExtractExpressionBindingPaths(String path) {
    assertThatCode(() -> PathUtil.parse(path)).doesNotThrowAnyException();
  }

  private static final String[] INVALID_PATHS =
      new String[] {
        null,
        "",
        "event_id", // missing root
        "$..event_id", // uses recursive descent
        "$.events.*", // uses wildcard
        "$.0invalid", // starts with a digit
        "$._\uD834", // dangling high surrogate
        "$._\uDC34", // low surrogate without high surrogate
      };

  @ParameterizedTest
  @FieldSource("INVALID_PATHS")
  public void testExtractBindingWithInvalidPath(String path) {
    assertThatThrownBy(() -> PathUtil.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("(Unsupported|Invalid) path.*");
  }

  private static final String[][] NORMALIZED_PATHS =
      new String[][] {
        new String[] {"$", "$"},
        new String[] {"$.a", "$['a']"}, // RFC 9535 example
        new String[] {"$.a.b.c", "$['a']['b']['c']"},
        new String[] {"$.\u2603", "$['☃']"},
        new String[] {"$.a\uD834\uDD1Eb.x", "$['a\uD834\uDD1Eb']['x']"},
        // Dot shorthand vs bracket form for names; array steps use unquoted [n] in both.
        new String[] {"$.matrix[0][1]", "$['matrix'][0][1]"},
        new String[] {"$.items[0].tags[1]", "$['items'][0]['tags'][1]"},
        new String[] {"$.basket[0][2].a", "$['basket'][0][2]['a']"},
      };

  @ParameterizedTest
  @FieldSource("NORMALIZED_PATHS")
  public void testNormalizedPath(String shortPath, String normalizedPath) {
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse(shortPath))).isEqualTo(normalizedPath);
  }

  private static final Object[][] NORMALIZED_FIELD_LISTS =
      new Object[][] {
        new Object[] {List.of(), "$"},
        new Object[] {List.of("a.b", "c"), "$['a.b']['c']"},
        new Object[] {List.of("a", "b", "c"), "$['a']['b']['c']"},
        new Object[] {List.of("a", "\u2603", "c"), "$['a']['\u2603']['c']"},
        new Object[] {List.of("a\uD834\uDD1Eb", "c"), "$['a\uD834\uDD1Eb']['c']"},
        new Object[] {List.of("a'b\n", "\u000Cc"), "$['a\\'b\\n']['\\fc']"},
        new Object[] {List.of("a'b\u000B\n", "\u000Cc"), "$['a\\'b\\u000b\\n']['\\fc']"},
      };

  @ParameterizedTest
  @FieldSource("NORMALIZED_FIELD_LISTS")
  public void testNormalizedFieldLists(List<String> fields, String normalizedPath) {
    assertThat(PathUtil.toNormalizedPath(fields)).isEqualTo(normalizedPath);
  }

  private static final String[][] ESCAPE_CASES =
      new String[][] {
        new String[] {"\u000B", "\\u000b"}, // RFC 9535 example
        new String[] {"\b", "\\b"},
        new String[] {"\t", "\\t"},
        new String[] {"\f", "\\f"},
        new String[] {"\n", "\\n"},
        new String[] {"\r", "\\r"},
        new String[] {"'", "\\'"},
        new String[] {"\\", "\\\\"},
        new String[] {"a\\b", "a\\\\b"},
        new String[] {"a\\b'", "a\\\\b\\'"},
      };

  @ParameterizedTest
  @FieldSource("ESCAPE_CASES")
  public void testPathEscaping(String name, String escaped) {
    assertThat(PathUtil.rfc9535escape(name)).isEqualTo(escaped);
  }

  @ParameterizedTest
  @FieldSource("ESCAPE_CASES")
  public void testPathUnescapeRoundTrip(String name, String escaped) {
    assertThat(PathUtil.rfc9535unescape(escaped)).isEqualTo(name);
  }

  @ParameterizedTest
  @FieldSource("NORMALIZED_PATHS")
  public void testParseDotAndBracketAgree(String dotPath, String normalizedPath) {
    assertThat(PathUtil.parse(dotPath)).isEqualTo(PathUtil.parse(normalizedPath));
  }

  @Test
  public void testParseSingleSegmentWithDot() {
    assertThat(PathUtil.parse("$['a.b']")).isEqualTo(names("a.b"));
    assertThat(PathUtil.parse("$['user.name']")).isEqualTo(names("user.name"));
  }

  @Test
  public void testParseDistinctFromDotSplit() {
    assertThat(PathUtil.parse("$.a.b")).isEqualTo(names("a", "b"));
    assertThat(PathUtil.parse("$['a.b']")).isEqualTo(names("a.b"));
  }

  @Test
  public void testParseNormalizedRoundTrip() {
    for (Object[] row : NORMALIZED_FIELD_LISTS) {
      @SuppressWarnings("unchecked")
      List<String> fields = (List<String>) row[0];
      String normalized = (String) row[1];
      assertThat(PathUtil.toNormalizedPath(PathUtil.parse(normalized))).isEqualTo(normalized);
      assertThat(PathUtil.parse(normalized))
          .isEqualTo(fields.stream().map(Name::new).collect(Collectors.toList()));
    }
  }

  @Test
  public void testParseRoot() {
    assertThat(PathUtil.parse("$")).isEqualTo(List.of());
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$"))).isEqualTo("$");
  }

  /**
   * Bracket paths can represent keys that dot notation cannot (empty name, {@code []}, {@code ..}).
   */
  @Test
  public void testParseSpecialFieldNames() {
    assertThat(PathUtil.parse("$['']")).isEqualTo(names(""));
    assertThat(PathUtil.parse("$['[]']")).isEqualTo(names("[]"));
    assertThat(PathUtil.parse("$['..']")).isEqualTo(names(".."));
    assertThat(PathUtil.parse("$['*']")).isEqualTo(names("*"));
    assertThat(PathUtil.parse("$['[']")).isEqualTo(names("["));
    assertThat(PathUtil.parse("$[']']")).isEqualTo(names("]"));

    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$['']"))).isEqualTo("$['']");
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$['[]']"))).isEqualTo("$['[]']");
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$['..']"))).isEqualTo("$['..']");
  }

  @Test
  public void testParseNestedWithSpecialMiddleSegment() {
    assertThat(PathUtil.parse("$['a']['..']['b']")).isEqualTo(names("a", "..", "b"));
    assertThat(PathUtil.toNormalizedPath(List.of("a", "..", "b"))).isEqualTo("$['a']['..']['b']");
  }

  @Test
  public void testParseMixedDotAndBracket() {
    assertThat(PathUtil.parse("$.a['b.c']")).isEqualTo(names("a", "b.c"));
    assertThat(PathUtil.parse("$.events['user.name'].id"))
        .isEqualTo(names("events", "user.name", "id"));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$.a['b.c']"))).isEqualTo("$['a']['b.c']");
  }

  @Test
  public void testParseMixedBracketThenDot() {
    assertThat(PathUtil.parse("$['x.y'].z")).isEqualTo(names("x.y", "z"));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$['x.y'].z"))).isEqualTo("$['x.y']['z']");
  }

  @Test
  public void testParseArrayIndexSegments() {
    assertThat(PathUtil.parse("$.matrix[0][1]"))
        .isEqualTo(List.of(new Name("matrix"), new Index(0), new Index(1)));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$.matrix[0][1]")))
        .isEqualTo("$['matrix'][0][1]");

    assertThat(PathUtil.parse("$.basket[0][2].a"))
        .isEqualTo(List.of(new Name("basket"), new Index(0), new Index(2), new Name("a")));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$.basket[0][2].a")))
        .isEqualTo("$['basket'][0][2]['a']");

    assertThat(PathUtil.parse("$.items[0].tags[1]"))
        .isEqualTo(List.of(new Name("items"), new Index(0), new Name("tags"), new Index(1)));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$.items[0].tags[1]")))
        .isEqualTo("$['items'][0]['tags'][1]");

    assertThat(PathUtil.parse("$.events[0].event_id"))
        .isEqualTo(List.of(new Name("events"), new Index(0), new Name("event_id")));
    assertThat(PathUtil.toNormalizedPath(PathUtil.parse("$.events[0].event_id")))
        .isEqualTo("$['events'][0]['event_id']");

    assertThat(PathUtil.parse("$['items'][0]['tags'][1]"))
        .isEqualTo(PathUtil.parse("$.items[0].tags[1]"));
    assertThat(PathUtil.parse("$['matrix'][0][1]")).isEqualTo(PathUtil.parse("$.matrix[0][1]"));
  }

  @Test
  public void testParseArrayIndexRoundTrip() {
    // parse(toNormalizedPath(parse(x))) == parse(x) for paths with [n] segments
    for (String[] pair : NORMALIZED_PATHS) {
      String normalized = pair[1];
      if (normalized.contains("[") && !normalized.contains("['")) {
        // normalized path has array indices; round-trip must be stable
        assertThat(PathUtil.parse(PathUtil.toNormalizedPath(PathUtil.parse(normalized))))
            .isEqualTo(PathUtil.parse(normalized));
      }
    }
    // explicit cases to be clear about what is covered
    for (String path :
        new String[] {
          "$['matrix'][0][1]",
          "$['items'][0]['tags'][1]",
          "$['basket'][0][2]['a']",
          "$['events'][0]['event_id']",
        }) {
      assertThat(PathUtil.parse(PathUtil.toNormalizedPath(PathUtil.parse(path))))
          .isEqualTo(PathUtil.parse(path));
    }
  }

  private static final String[] INVALID_PARSE_PATHS =
      new String[] {
        null,
        "",
        "event_id",
        "$.a..b",
        "$.events.*",
        "$['a'", // unclosed
        "$['a']x", // trailing junk
        "$.['a']", // empty dot segment before bracket
        "$a.b", // missing separator after $
        "$.a[]", // empty array index
        "$.a[-1]", // negative index (not valid JSONPath index syntax here)
      };

  @ParameterizedTest
  @FieldSource("INVALID_PARSE_PATHS")
  public void testParseInvalid(String path) {
    assertThatThrownBy(() -> PathUtil.parse(path))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching("(Unsupported|Invalid) path.*");
  }
}
