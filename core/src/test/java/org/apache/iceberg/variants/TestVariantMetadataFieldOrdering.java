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
package org.apache.iceberg.variants;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

public class TestVariantMetadataFieldOrdering {

  // U+FFFF encodes to 3 UTF-8 bytes (EF BF BF), U+10000 to 4 (F0 90 80 80)
  private static final String NAME_3_BYTE = new String(Character.toChars(0xFFFF));
  private static final String NAME_4_BYTE = new String(Character.toChars(0x10000));

  @Test
  public void utf8OrderedDictionaryIsSortedAndOrdered() {
    SerializedMetadata metadata =
        (SerializedMetadata) Variants.metadata(ImmutableList.of(NAME_3_BYTE, NAME_4_BYTE));

    assertThat(metadata.isSorted()).isTrue();
    assertThat(metadata.get(0)).isEqualTo(NAME_3_BYTE);
    assertThat(metadata.get(1)).isEqualTo(NAME_4_BYTE);
    assertThat(metadata.id(NAME_3_BYTE)).isEqualTo(0);
    assertThat(metadata.id(NAME_4_BYTE)).isEqualTo(1);
  }

  @Test
  public void utf16OrderedDictionaryIsNotFlaggedSorted() {
    // UTF-16 order differs from UTF-8: high surrogate D800 sorts before FFFF
    List<String> utf16Ordered =
        ImmutableList.of(NAME_3_BYTE, NAME_4_BYTE).stream().sorted().collect(Collectors.toList());
    assertThat(utf16Ordered).containsExactly(NAME_4_BYTE, NAME_3_BYTE);

    SerializedMetadata metadata = (SerializedMetadata) Variants.metadata(utf16Ordered);

    assertThat(metadata.isSorted()).isFalse();
    // lookups still succeed via the unsorted (linear) path, at their input positions
    assertThat(metadata.id(NAME_4_BYTE)).isEqualTo(0);
    assertThat(metadata.id(NAME_3_BYTE)).isEqualTo(1);
  }
}
