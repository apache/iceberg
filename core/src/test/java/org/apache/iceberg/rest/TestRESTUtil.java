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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRESTUtil {

  @Test
  public void testExtractPrefixMap() {
    Map<String, String> input =
        ImmutableMap.of(
            "warehouse", "/tmp/warehouse",
            "rest.prefix", "/ws/ralphs_catalog",
            "rest.token", "YnVybiBhZnRlciByZWFkaW5nIC0gYWxzbyBoYW5rIGFuZCByYXVsIDQgZXZlcgo=",
            "rest.rest.uri", "https://localhost:1080/",
            "doesnt_start_with_prefix.rest", "",
            "", "");

    Map<String, String> expected =
        ImmutableMap.of(
            "prefix", "/ws/ralphs_catalog",
            "token", "YnVybiBhZnRlciByZWFkaW5nIC0gYWxzbyBoYW5rIGFuZCByYXVsIDQgZXZlcgo=",
            "rest.uri", "https://localhost:1080/");

    Map<String, String> actual = RESTUtil.extractPrefixMap(input, "rest.");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testStripTrailingSlash() {
    String[][] testCases =
        new String[][] {
          new String[] {"https://foo", "https://foo"},
          new String[] {"https://foo/", "https://foo"},
          new String[] {"https://foo////", "https://foo"},
          new String[] {null, null}
        };

    for (String[] testCase : testCases) {
      String input = testCase[0];
      String expected = testCase[1];
      assertThat(RESTUtil.stripTrailingSlash(input)).isEqualTo(expected);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"%1F", "%2D", "%2E", "#", "_"})
  public void testRoundTripUrlEncodeDecodeNamespace(String namespaceSeparator) {
    // Namespace levels and their expected url encoded form
    Object[][] testCases =
        new Object[][] {
          new Object[] {new String[] {"dogs"}, "dogs"},
          new Object[] {new String[] {"dogs.named.hank"}, "dogs.named.hank"},
          new Object[] {new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"},
          new Object[] {
            new String[] {"dogs", "named", "hank"},
            String.format("dogs%snamed%shank", namespaceSeparator, namespaceSeparator)
          },
          new Object[] {
            new String[] {"dogs.and.cats", "named", "hank.or.james-westfall"},
            String.format(
                "dogs.and.cats%snamed%shank.or.james-westfall",
                namespaceSeparator, namespaceSeparator),
          }
        };

    for (Object[] namespaceWithEncoding : testCases) {
      String[] levels = (String[]) namespaceWithEncoding[0];
      String encodedNs = (String) namespaceWithEncoding[1];

      Namespace namespace = Namespace.of(levels);

      // To be placed into a URL path as query parameter or path parameter
      assertThat(RESTUtil.encodeNamespace(namespace, namespaceSeparator)).isEqualTo(encodedNs);

      // Decoded (after pulling as String) from URL
      assertThat(RESTUtil.decodeNamespace(encodedNs, namespaceSeparator)).isEqualTo(namespace);
    }
  }

  @Test
  public void encodeAsOldClientAndDecodeAsNewServer() {
    Namespace namespace = Namespace.of("first", "second", "third");
    // old client would call encodeNamespace without specifying a separator
    String encodedNamespace = RESTUtil.encodeNamespace(namespace);
    assertThat(encodedNamespace).contains(RESTUtil.NAMESPACE_ESCAPED_SEPARATOR);

    // newer server would try and decode the namespace with the separator it communicates to clients
    Namespace decodeNamespace = RESTUtil.decodeNamespace(encodedNamespace, "%2E");
    assertThat(decodeNamespace).isEqualTo(namespace);
  }

  @Test
  public void testNamespaceUrlEncodeDecodeDoesNotAllowNull() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.encodeNamespace(null))
        .withMessage("Invalid namespace: null");

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.decodeNamespace(null))
        .withMessage("Invalid namespace: null");
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2URLEncoding() {
    // from OAuth2, RFC 6749 Appendix B.
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String expected = "+%25%26%2B%C2%A3%E2%82%AC";

    assertThat(RESTUtil.encodeString(utf8)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataEncoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> formData = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String expected = "client_id=12345&client_secret=" + asString;

    assertThat(RESTUtil.encodeFormData(formData)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataDecoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> expected = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String formString = "client_id=12345&client_secret=" + asString;

    assertThat(RESTUtil.decodeFormData(formString)).isEqualTo(expected);
  }

  @Test
  public void nullOrEmptyNamespaceSeparator() {
    String errorMsg = "Invalid separator: null or empty";
    assertThatThrownBy(() -> RESTUtil.encodeNamespace(Namespace.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(errorMsg);

    assertThatThrownBy(() -> RESTUtil.encodeNamespace(Namespace.empty(), ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(errorMsg);

    assertThatThrownBy(() -> RESTUtil.decodeNamespace("namespace", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(errorMsg);

    assertThatThrownBy(() -> RESTUtil.decodeNamespace("namespace", ""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(errorMsg);
  }
}
