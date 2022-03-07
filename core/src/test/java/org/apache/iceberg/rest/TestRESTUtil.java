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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public class TestRESTUtil {

  static class LevelsAndURLEncodedString {
    public String[] levels;
    public String parameterEncodedForUrlUsage;

    LevelsAndURLEncodedString(String[] levels, String parameterEncodedForUrlUsage) {
      this.levels = levels;
      this.parameterEncodedForUrlUsage = parameterEncodedForUrlUsage;
    }
  }

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

  @Test
  public void testStripTrailingSlash() {
    Map<String, String> testCaseAndExpectedAnswer = ImmutableMap.of(
        "", "",
        "https://foo", "https://foo",
        "https://foo/", "https://foo",
        "https://foo////", "https://foo///"
    );

    testCaseAndExpectedAnswer.forEach((input, expected) ->
        Assertions.assertThat(RESTUtil.stripTrailingSlash(input)).isEqualTo(expected)
    );

    Assertions.assertThat(RESTUtil.stripTrailingSlash(null)).isNull();
  }

  @Test
  public void testAsURLVariable() {
    List<LevelsAndURLEncodedString> testCases = ImmutableList.of(
        new LevelsAndURLEncodedString(new String[] {"dogs"}, "dogs"),
        new LevelsAndURLEncodedString(new String[] {"dogs.named.hank"}, "dogs.named.hank"),
        new LevelsAndURLEncodedString(new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"),
        new LevelsAndURLEncodedString(
            new String[] {"dogs", "named", "hank"},
            "dogs\u0000named\u0000hank"),
        new LevelsAndURLEncodedString(
            new String[] {"dogs.and.cats", "named", "hank.or.james-westfall"},
            "dogs.and.cats\u0000named\u0000hank.or.james-westfall"));

    for (LevelsAndURLEncodedString testCase : testCases) {
      String[] levels = testCase.levels;
      String nsEncodedForURLVariable = testCase.parameterEncodedForUrlUsage;
      Namespace namespace = Namespace.of(levels);

      // To be placed into a URL path as query parameter or path variable
      Assertions.assertThat(RESTUtil.urlEncode(namespace))
          .isEqualTo(nsEncodedForURLVariable);

      // Decoded (after pulling as String) from URL
      Namespace asNamespace = RESTUtil.urlDecode(nsEncodedForURLVariable);
      Assertions.assertThat(asNamespace).isEqualTo(namespace);
    }
  }
}
