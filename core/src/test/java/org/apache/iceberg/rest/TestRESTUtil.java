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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

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

    Assertions.assertThat(actual).isEqualTo(expected);
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
      Assertions.assertThat(RESTUtil.stripTrailingSlash(input)).isEqualTo(expected);
    }
  }

  @Test
  public void testRoundTripUrlEncodeDecodeNamespace() {
    // Namespace levels and their expected url encoded form
    Object[][] testCases =
        new Object[][] {
          new Object[] {new String[] {"dogs"}, "dogs"},
          new Object[] {new String[] {"dogs.named.hank"}, "dogs.named.hank"},
          new Object[] {new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"},
          new Object[] {new String[] {"dogs", "named", "hank"}, "dogs%1Fnamed%1Fhank"},
          new Object[] {
            new String[] {"dogs.and.cats", "named", "hank.or.james-westfall"},
            "dogs.and.cats%1Fnamed%1Fhank.or.james-westfall"
          }
        };

    for (Object[] namespaceWithEncoding : testCases) {
      String[] levels = (String[]) namespaceWithEncoding[0];
      String encodedNs = (String) namespaceWithEncoding[1];

      Namespace namespace = Namespace.of(levels);

      // To be placed into a URL path as query parameter or path parameter
      Assertions.assertThat(RESTUtil.encodeNamespace(namespace)).isEqualTo(encodedNs);

      // Decoded (after pulling as String) from URL
      Namespace asNamespace = RESTUtil.decodeNamespace(encodedNs);
      Assertions.assertThat(asNamespace).isEqualTo(namespace);
    }
  }

  @Test
  public void testNamespaceUrlEncodeDecodeDoesNotAllowNull() {
    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.encodeNamespace(null))
        .withMessage("Invalid namespace: null");

    Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> RESTUtil.decodeNamespace(null))
        .withMessage("Invalid namespace: null");
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2URLEncoding() {
    // from OAuth2, RFC 6749 Appendix B.
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String expected = "+%25%26%2B%C2%A3%E2%82%AC";

    Assertions.assertThat(RESTUtil.encodeString(utf8)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataEncoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> formData = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String expected = "client_id=12345&client_secret=" + asString;

    Assertions.assertThat(RESTUtil.encodeFormData(formData)).isEqualTo(expected);
  }

  @Test
  @SuppressWarnings("checkstyle:AvoidEscapedUnicodeCharacters")
  public void testOAuth2FormDataDecoding() {
    String utf8 = "\u0020\u0025\u0026\u002B\u00A3\u20AC";
    String asString = "+%25%26%2B%C2%A3%E2%82%AC";
    Map<String, String> expected = ImmutableMap.of("client_id", "12345", "client_secret", utf8);
    String formString = "client_id=12345&client_secret=" + asString;

    Assertions.assertThat(RESTUtil.decodeFormData(formString)).isEqualTo(expected);
  }
}
