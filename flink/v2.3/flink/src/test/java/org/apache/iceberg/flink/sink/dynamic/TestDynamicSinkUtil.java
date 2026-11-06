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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestDynamicSinkUtil {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Test
  void testValidateUpsertSkippedWhenNotUpsert() {
    assertThatCode(
            () ->
                DynamicSinkUtil.validateUpsert(
                    false, PartitionSpec.unpartitioned(), Collections.emptySet()))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateUpsertFailsWithoutEqualityFields() {
    assertThatThrownBy(
            () ->
                DynamicSinkUtil.validateUpsert(
                    true, PartitionSpec.unpartitioned(), Collections.emptySet()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Equality field columns shouldn't be empty when configuring to use UPSERT data.");
  }

  @Test
  void testValidateUpsertUnpartitionedWithEqualityFields() {
    assertThatCode(
            () ->
                DynamicSinkUtil.validateUpsert(
                    true, PartitionSpec.unpartitioned(), Sets.newHashSet(1)))
        .doesNotThrowAnyException();
  }

  @Test
  void testValidateUpsertFailsWhenPartitionSourceNotEqualityField() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    assertThatThrownBy(() -> DynamicSinkUtil.validateUpsert(true, spec, Sets.newHashSet(1)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("should be included in equality fields");
  }

  @Test
  void testValidateUpsertPartitionedWithPartitionSourceInEqualityFields() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    assertThatCode(() -> DynamicSinkUtil.validateUpsert(true, spec, Sets.newHashSet(1, 2)))
        .doesNotThrowAnyException();
  }
}
