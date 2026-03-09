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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestPropertyUtil {

  @Test
  void propertiesWithPrefixStripsLiterally() {
    // The prefix contains dots which are regex wildcards. Using replaceFirst(prefix, "")
    // would treat dots as regex "any char", which is incorrect. substring(prefix.length())
    // strips the prefix literally.
    Map<String, String> properties =
        Map.of(
            "write.parquet.column.int_field", "false",
            "write.parquet.column.string_field", "true",
            "unrelated.key", "value");

    Map<String, String> result =
        PropertyUtil.propertiesWithPrefix(properties, "write.parquet.column.");

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of("int_field", "false", "string_field", "true"));
  }

  @Test
  void propertiesWithPrefixHandlesRegexSpecialChars() {
    // Prefix with regex-special characters that would cause PatternSyntaxException
    // with replaceFirst but work correctly with substring
    Map<String, String> properties = Map.of("prefix[0].key", "value");

    Map<String, String> result =
        PropertyUtil.propertiesWithPrefix(properties, "prefix[0].");

    assertThat(result).containsExactlyInAnyOrderEntriesOf(Map.of("key", "value"));
  }

  @Test
  void mergeProperties() {
    Map<String, String> properties = Map.of("k1", "v1", "k2", "v2");
    Map<String, String> overrides = Map.of("k1", "v11", "k3", "v3");

    assertThat(PropertyUtil.mergeProperties(null, null)).isNull();
    assertThat(PropertyUtil.mergeProperties(properties, null)).isEqualTo(properties);
    assertThat(PropertyUtil.mergeProperties(properties, Map.of())).isEqualTo(properties);
    assertThat(PropertyUtil.mergeProperties(null, overrides)).isEqualTo(overrides);
    assertThat(PropertyUtil.mergeProperties(Map.of(), overrides)).isEqualTo(overrides);
    assertThat(PropertyUtil.mergeProperties(properties, overrides))
        .containsExactlyInAnyOrderEntriesOf(Map.of("k1", "v11", "k2", "v2", "k3", "v3"));
  }
}
