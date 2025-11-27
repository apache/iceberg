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
import java.util.UUID;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

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
      assertThat(RESTUtil.encodeNamespace(namespace)).isEqualTo(encodedNs);

      // Decoded (after pulling as String) from URL
      Namespace asNamespace = RESTUtil.decodeNamespace(encodedNs);
      assertThat(asNamespace).isEqualTo(namespace);
    }
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
  public void testNullEndpointPath() {
    assertThat(RESTUtil.resolveEndpoint("http://catalog-uri", null)).isNull();
  }

  @Test
  public void testAbsoluteEndpointPath() {
    assertThat(
            RESTUtil.resolveEndpoint("http://catalog-uri", "http://catalog-uri/refresh-endpoint"))
        .isEqualTo("http://catalog-uri/refresh-endpoint");
    assertThat(
            RESTUtil.resolveEndpoint("http://catalog-uri/", "http://catalog-uri/refresh-endpoint"))
        .isEqualTo("http://catalog-uri/refresh-endpoint");
  }

  @Test
  public void testRelativeEndpointPath() {
    assertThat(RESTUtil.resolveEndpoint(null, "/refresh-endpoint")).isEqualTo("/refresh-endpoint");
    assertThat(RESTUtil.resolveEndpoint("http://catalog-uri", "/refresh-endpoint"))
        .isEqualTo("http://catalog-uri/refresh-endpoint");
    assertThat(RESTUtil.resolveEndpoint("http://catalog-uri/", "/refresh-endpoint"))
        .isEqualTo("http://catalog-uri/refresh-endpoint");
    assertThat(
            RESTUtil.resolveEndpoint("http://catalog-uri/", "relative-endpoint/refresh-endpoint"))
        .isEqualTo("http://catalog-uri/relative-endpoint/refresh-endpoint");
  }

  @Test
  public void namespaceToQueryParam() {
    assertThatThrownBy(() -> RESTUtil.namespaceToQueryParam(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    assertThat(RESTUtil.namespaceToQueryParam(Namespace.empty())).isEqualTo("");
    assertThat(RESTUtil.namespaceToQueryParam(Namespace.of(""))).isEqualTo("");
    assertThat(RESTUtil.namespaceToQueryParam(Namespace.of("ns"))).isEqualTo("ns");

    // verify that the unicode character (\001f) and not its UTF-8 escaped version (%1F) is used
    assertThat(RESTUtil.namespaceToQueryParam(Namespace.of("one", "ns")))
        .isEqualTo("one\u001fns")
        .isNotEqualTo("one%1Fns");
    assertThat(RESTUtil.namespaceToQueryParam(Namespace.of("one", "two", "ns")))
        .isEqualTo("one\u001ftwo\u001fns")
        .isNotEqualTo("one%1Ftwo%1Fns");
    assertThat(RESTUtil.namespaceToQueryParam(Namespace.of("one.two", "three.four", "ns")))
        .isEqualTo("one.two\u001fthree.four\u001fns")
        .isNotEqualTo("one.two%1Fthree.four%1Fns");
  }

  @Test
  public void namespaceFromQueryParam() {
    assertThatThrownBy(() -> RESTUtil.namespaceFromQueryParam(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid namespace: null");

    assertThat(RESTUtil.namespaceFromQueryParam("")).isEqualTo(Namespace.of(""));
    assertThat(RESTUtil.namespaceFromQueryParam("ns")).isEqualTo(Namespace.of("ns"));
    assertThat(RESTUtil.namespaceFromQueryParam("one\u001fns"))
        .isEqualTo(Namespace.of("one", "ns"));
    assertThat(RESTUtil.namespaceFromQueryParam("one\u001ftwo\u001fns"))
        .isEqualTo(Namespace.of("one", "two", "ns"));
    assertThat(RESTUtil.namespaceFromQueryParam("one.two\u001fthree.four\u001fns"))
        .isEqualTo(Namespace.of("one.two", "three.four", "ns"));

    // using the UTF-8 escaped version will produce a wrong namespace instance
    assertThat(RESTUtil.namespaceFromQueryParam("one%1Fns")).isEqualTo(Namespace.of("one%1Fns"));
    assertThat(RESTUtil.namespaceFromQueryParam("one%1Ftwo%1Fns"))
        .isEqualTo(Namespace.of("one%1Ftwo%1Fns"));
    assertThat(RESTUtil.namespaceFromQueryParam("one%1Ftwo\u001fns"))
        .isEqualTo(Namespace.of("one%1Ftwo", "ns"));
  }

  @Test
  public void uuidV7HasVersionAndVariant() {
    String id = RESTUtil.generateUuidV7();
    UUID uuid = UUID.fromString(id);
    assertThat(uuid.version()).isEqualTo(7);
    assertThat(uuid.variant()).isEqualTo(2);
  }
}
