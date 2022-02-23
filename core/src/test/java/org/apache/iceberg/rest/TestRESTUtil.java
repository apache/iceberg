/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import java.util.List;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
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
  public void testAsURLVariable() {
    List<LevelsAndURLEncodedString> testCases = ImmutableList.of(
        new LevelsAndURLEncodedString(new String[] {"dogs"}, "dogs"),
        new LevelsAndURLEncodedString(new String[] {"dogs.named.hank"}, "dogs.named.hank"),
        new LevelsAndURLEncodedString(new String[] {"dogs/named/hank"}, "dogs%2Fnamed%2Fhank"),
        new LevelsAndURLEncodedString(
            new String[] {"dogs", "named", "hank"},
            "dogs\u0000named\u0000hank"),
        new LevelsAndURLEncodedString(
            new String[] {"dogs.and.cats", "named", "hank."},
            "dogs.and.cats\u0000named\u0000hank."));

    for (LevelsAndURLEncodedString testCase : testCases) {
      String[] levels = testCase.levels;
      String nsEncodedForURLVariable = testCase.parameterEncodedForUrlUsage;
      Namespace namespace = Namespace.of(levels);

      // To be placed into a URL path as query parameter or path variable
      Assertions.assertThat(Namespace.asURLVariable(namespace))
          .isEqualTo(nsEncodedForURLVariable);

      // Decoded (after pulling as String) from URL
      Namespace asNamespace = Namespace.fromURLVariable(nsEncodedForURLVariable);
      Assertions.assertThat(asNamespace).isEqualTo(namespace);
    }
  }
}
