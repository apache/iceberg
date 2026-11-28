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
package org.apache.iceberg.index;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.Test;

class TestZorderTransform {

  @Test
  void nameAndRangeSupport() {
    ZorderTransform transform = new ZorderTransform();
    assertThat(transform.name()).isEqualTo("ZORDER");
    assertThat(transform.supportsRangePredicates()).isFalse();
  }

  @Test
  void registryResolvesZorder() {
    assertThat(TransformFunctions.forName("zorder", ImmutableMap.of()))
        .isInstanceOf(ZorderTransform.class);
  }

  @Test
  void encodeIsDeterministicAndNonNegative() {
    ZorderTransform transform = new ZorderTransform();
    List<Object> key = ImmutableList.of(123456789L, "device-abc");
    assertThat(transform.transform(key))
        .isEqualTo(transform.transform(ImmutableList.of(123456789L, "device-abc")))
        .isGreaterThanOrEqualTo(0L);
  }

  @Test
  void distinctForWellDistributedVarcharKeys() {
    ZorderTransform transform = new ZorderTransform();
    List<String> alphabet = Lists.newArrayList();
    for (char c = 'a'; c <= 't'; c++) {
      alphabet.add(String.valueOf(c));
    }
    Set<Long> values = Sets.newHashSet();
    for (String first : alphabet) {
      for (String second : alphabet) {
        values.add(transform.transform(ImmutableList.of(first, second)));
      }
    }
    // 20 x 20 keys that differ in their leading byte must not collide.
    assertThat(values).hasSize(alphabet.size() * alphabet.size());
  }

  @Test
  void rejectsTooManyColumns() {
    ZorderTransform transform = new ZorderTransform();
    List<Object> nineColumns = Lists.newArrayList();
    for (int i = 0; i < 9; i++) {
      nineColumns.add((long) i);
    }
    assertThatThrownBy(() -> transform.transform(nineColumns))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at most");
  }

  @Test
  void rejectsUnsupportedType() {
    assertThatThrownBy(() -> new ZorderTransform().transform(ImmutableList.of(1.5d)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported type");
  }
}
