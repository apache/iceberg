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

package org.apache.iceberg.rest;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestRESTUtil {

  @Test
  public void testFilterAndRemovePrefix() {
    Map<String, String> input = ImmutableMap.of(
        "warehouse", "/tmp/warehouse",
        "rest.prefix", "/ws/ralphs_catalog",
        "rest.token", "YnVybiBhZnRlciByZWFkaW5nIC0gYWxzbyBoYW5rIGFuZCByYXVsIDQgZXZlcgo=",
        "rest.rest.uri", "https://localhost:1080/",
        ".rest", "",
        "", "");

    Map<String, String> expected = ImmutableMap.of(
        "prefix", "/ws/ralphs_catalog",
        "token", "YnVybiBhZnRlciByZWFkaW5nIC0gYWxzbyBoYW5rIGFuZCByYXVsIDQgZXZlcgo=",
        "rest.uri", "https://localhost:1080/"
    );

    Map<String, String> actual = RESTUtil.filterByPrefix(input, "rest.");

    Assertions.assertThat(actual).isEqualTo(expected);
  }

  private static Stream<String[]> stripTrailingSlashTestCases() {
    return Stream.of(
        new String[] {"https://foo", "https://foo"},
        new String[] {"https://foo/", "https://foo"},
        new String[] {"https://foo////", "https://foo"},
        new String[] {null, null}
    );
  }

  @DisplayName("Should remove all slash characters from the end of the input string")
  @ParameterizedTest(name = "{index} => input={0}, expected={1}")
  @MethodSource("stripTrailingSlashTestCases")
  public void testStripTrailingSlash(String input, String expected) {
    Assertions.assertThat(RESTUtil.stripTrailingSlash(input)).isEqualTo(expected);
  }

  private static Stream<Object[]> roundTripUrlEncodeDecodeNamespaceTestCases() {
    return Stream.of(
        new Object[] {new String[] {"dogs"}, "dogs"},
        new Object[] {new String[] {"dogs.named.hank"}, "dogs.named.hank"},
        new Object[] {new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"},
        new Object[] {new String[] {"dogs", "named", "hank"}, "dogs\u0000named\u0000hank"},
        new Object[] {
            new String[] {"dogs.and.cats", "named", "hank.or.james-westfall"},
            "dogs.and.cats\u0000named\u0000hank.or.james-westfall"
        }
    );
  }

  @DisplayName("Url encoding and decoding a namespace should return an equal namespace")
  @ParameterizedTest(name = "{index} => levels={0}, encodedNamespace={1}")
  @MethodSource("roundTripUrlEncodeDecodeNamespaceTestCases")
  public void testRoundTripUrlEncodeDecodeNamespace(String[] levels, String encodedNs) {
    Namespace namespace = Namespace.of(levels);
    // To be placed into a URL path as query parameter or path parameter
    Assertions.assertThat(RESTUtil.urlEncode(namespace))
        .isEqualTo(encodedNs);

    // Decoded (after pulling as String) from URL
    Namespace asNamespace = RESTUtil.urlDecode(encodedNs);
    Assertions.assertThat(asNamespace).isEqualTo(namespace);
  }

  @Test
  public void testNamespaceUrlEncodeDecodeDoesNotAllowNull() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.urlEncode(null))
        .withMessage("Invalid namespace: null");

    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.urlDecode(null))
        .withMessage("Invalid namespace: null");
  }
}
