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

  @Test
  void testParseArrayPath() {
    assertThat(PathUtil.parse("$.commits[0].author.name"))
        .isEqualTo(
            List.of(new Name("commits"), new Index(0), new Name("author"), new Name("name")));
  }

  @Test
  void testParseArrayIndexOnly() {
    assertThat(PathUtil.parse("$.a[1][2].b"))
        .isEqualTo(List.of(new Name("a"), new Index(1), new Index(2), new Name("b")));
  }

  @Test
  void testParseBracketMixed() {
    assertThat(PathUtil.parse("$['issue']['labels'][0]['name']"))
        .isEqualTo(List.of(new Name("issue"), new Name("labels"), new Index(0), new Name("name")));
  }

  @Test
  void testParseObjectPathSupportsDotAndBracketKeys() {
    assertThat(PathUtil.parseObjectPath("$.size")).containsExactly("size");
    assertThat(PathUtil.parseObjectPath("$.pull_request.user.login"))
        .containsExactly("pull_request", "user", "login");
    assertThat(PathUtil.parseObjectPath("$['city']")).containsExactly("city");
    assertThat(PathUtil.parseObjectPath("$['pull_request']['user']['login']"))
        .containsExactly("pull_request", "user", "login");
  }

  @Test
  void testParseObjectPathSupportsArrayIndexes() {
    assertThat(PathUtil.parseObjectPath("$.commits[0].author.name"))
        .containsExactly("commits", "[0]", "author", "name");
    assertThat(PathUtil.parseObjectPath("$.a[1][2].b")).containsExactly("a", "[1]", "[2]", "b");
    assertThat(PathUtil.parseObjectPath("$['issue']['labels'][0]['name']"))
        .containsExactly("issue", "labels", "[0]", "name");
  }

  @Test
  void testIsArrayIndexPartDetectsNumericBrackets() {
    assertThat(PathUtil.isArrayIndexPart("[0]")).isTrue();
    assertThat(PathUtil.isArrayIndexPart("[12]")).isTrue();
    assertThat(PathUtil.isArrayIndexPart("commits")).isFalse();
    assertThat(PathUtil.isArrayIndexPart("['field']")).isFalse();
    assertThat(PathUtil.isArrayIndexPart("[x]")).isFalse();
  }

  @Test
  void testParseArrayIndexPart() {
    assertThat(PathUtil.parseArrayIndexPart("[0]")).isEqualTo(0);
    assertThat(PathUtil.parseArrayIndexPart("[12]")).isEqualTo(12);
    assertThatThrownBy(() -> PathUtil.parseArrayIndexPart("commits"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid array index part");
  }
}
