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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers.CustomRow;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionSet {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "category", Types.StringType.get()));
  private static final PartitionSpec UNPARTITIONED_SPEC = PartitionSpec.unpartitioned();
  private static final PartitionSpec BY_DATA_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(1).build();
  private static final PartitionSpec BY_DATA_CATEGORY_BUCKET_SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("data").bucket("category", 8).withSpecId(3).build();
  private static final Map<Integer, PartitionSpec> SPECS =
      ImmutableMap.of(
          UNPARTITIONED_SPEC.specId(),
          UNPARTITIONED_SPEC,
          BY_DATA_SPEC.specId(),
          BY_DATA_SPEC,
          BY_DATA_CATEGORY_BUCKET_SPEC.specId(),
          BY_DATA_CATEGORY_BUCKET_SPEC);

  @Test
  public void testGet() {
    PartitionSet set = PartitionSet.create(SPECS);
    set.add(BY_DATA_SPEC.specId(), Row.of("a"));
    set.add(UNPARTITIONED_SPEC.specId(), null);
    set.add(UNPARTITIONED_SPEC.specId(), Row.of());
    set.add(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), CustomRow.of("a", 1));

    assertThat(set).hasSize(4);
    assertThat(set.contains(BY_DATA_SPEC.specId(), CustomRow.of("a"))).isTrue();
    assertThat(set.contains(UNPARTITIONED_SPEC.specId(), null)).isTrue();
    assertThat(set.contains(UNPARTITIONED_SPEC.specId(), CustomRow.of())).isTrue();
    assertThat(set.contains(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("a", 1))).isTrue();
  }

  @Test
  public void testRemove() {
    PartitionSet set = PartitionSet.create(SPECS);
    set.add(BY_DATA_SPEC.specId(), Row.of("a"));
    set.add(UNPARTITIONED_SPEC.specId(), null);
    set.add(UNPARTITIONED_SPEC.specId(), Row.of());
    set.add(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), CustomRow.of("a", 1));

    assertThat(set).hasSize(4);
    assertThat(set.remove(BY_DATA_SPEC.specId(), CustomRow.of("a"))).isTrue();
    assertThat(set.remove(UNPARTITIONED_SPEC.specId(), null)).isTrue();
    assertThat(set.remove(UNPARTITIONED_SPEC.specId(), CustomRow.of())).isTrue();
    assertThat(set.remove(BY_DATA_CATEGORY_BUCKET_SPEC.specId(), Row.of("a", 1))).isTrue();
    assertThat(set).isEmpty();
  }
}
