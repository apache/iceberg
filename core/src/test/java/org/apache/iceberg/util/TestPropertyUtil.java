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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestPropertyUtil {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  void propertiesWithPrefixHandlesRegexSpecialChars() {
    // prefix[0]. is a valid regex where [0] matches char '0' and . matches any char,
    // so replaceFirst (used by the older propertiesWithPrefix) would silently fail to strip the
    // literal prefix "prefix[0]."
    Map<String, String> properties = Map.of("prefix[0].key", "value");

    Map<String, String> result = PropertyUtil.propertiesWithPrefix(properties, "prefix[0].");

    assertThat(result).containsExactly(Map.entry("key", "value"));
  }

  @Test
  void propertiesWithPrefixHandlesUnclosedRegexChars() {
    // prefix[0. contains an unclosed character class which would cause
    // PatternSyntaxException with replaceFirst (used by the older propertiesWithPrefix) but works
    // correctly with substring
    Map<String, String> properties = Map.of("prefix[0.key", "value");

    Map<String, String> result = PropertyUtil.propertiesWithPrefix(properties, "prefix[0.");

    assertThat(result).containsExactly(Map.entry("key", "value"));
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

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TableProperties#COLUMN_PROPERTY_PREFIXES")
  void applySchemaChangesRenamesColumnProperty(String prefix) {
    Schema renamed =
        new Schema(required(1, "ID", Types.IntegerType.get()), SCHEMA.findField("data"));

    Map<String, String> result =
        PropertyUtil.applySchemaChanges(Map.of(prefix + "id", "v"), SCHEMA, renamed);

    assertThat(result).isEqualTo(Map.of(prefix + "ID", "v"));
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TableProperties#COLUMN_PROPERTY_PREFIXES")
  void applySchemaChangesRemovesDroppedColumnProperty(String prefix) {
    Schema dropped = new Schema(SCHEMA.findField("data"));

    Map<String, String> result =
        PropertyUtil.applySchemaChanges(Map.of(prefix + "id", "v"), SCHEMA, dropped);

    assertThat(result).isEmpty();
  }

  @Test
  void applySchemaChangesRenamedPropertyReplacesUnresolvedProperty() {
    String prefix = TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
    Schema renamed =
        new Schema(required(1, "ID", Types.IntegerType.get()), SCHEMA.findField("data"));
    Map<String, String> properties = Maps.newLinkedHashMap();
    properties.put(prefix + "id", "true");
    properties.put(prefix + "ID", "false");

    Map<String, String> result = PropertyUtil.applySchemaChanges(properties, SCHEMA, renamed);

    assertThat(result).isEqualTo(Map.of(prefix + "ID", "true"));
  }
}
