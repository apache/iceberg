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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestSortField {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  @Test
  void sourceIdForSingleArgField() {
    SortField field = SortOrder.builderFor(SCHEMA).asc("id").build().fields().get(0);

    assertThat(field.sourceIds()).containsExactly(1);
    assertThat(field.sourceId()).isEqualTo(1);
  }

  @Test
  void sourceIdFailsForMultiArgField() {
    SortField field =
        UnboundSortOrder.builder()
            .withOrderId(1)
            .addSortField(
                "bucket[16]", ImmutableList.of(1, 2), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .build()
            .bind(SCHEMA)
            .fields()
            .get(0);

    assertThat(field.sourceIds()).containsExactly(1, 2);
    assertThatThrownBy(field::sourceId)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Use sourceIds() for multi-argument transforms, found 2 source ids");
  }
}
