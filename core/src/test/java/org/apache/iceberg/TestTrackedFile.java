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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestTrackedFile {

  private static final Types.StructType CONTENT_STATS_TYPE =
      Types.StructType.of(
          optional(200, "col_1_value_count", Types.LongType.get()),
          optional(201, "col_1_null_count", Types.LongType.get()),
          optional(202, "col_2_value_count", Types.LongType.get()));

  @Test
  public void schemaWithContentStatsFieldOrder() {
    Types.StructType type = TrackedFile.schemaWithContentStats(CONTENT_STATS_TYPE);
    List<Types.NestedField> fields = type.fields();

    assertThat(fields)
        .extracting(Types.NestedField::name)
        .containsExactly(
            "tracking",
            "content_type",
            "location",
            "file_format",
            "record_count",
            "file_size_in_bytes",
            "spec_id",
            "content_stats",
            "sort_order_id",
            "deletion_vector",
            "manifest_info",
            "key_metadata",
            "split_offsets",
            "equality_ids");
  }

  @Test
  public void schemaWithContentStatsFieldIds() {
    Types.StructType type = TrackedFile.schemaWithContentStats(CONTENT_STATS_TYPE);
    List<Types.NestedField> fields = type.fields();

    assertThat(fields)
        .extracting(Types.NestedField::fieldId)
        .containsExactly(147, 134, 100, 101, 103, 104, 141, 146, 140, 148, 150, 131, 132, 135);
  }

  @Test
  public void schemaWithContentStatsUsesProvidedType() {
    Types.StructType type = TrackedFile.schemaWithContentStats(CONTENT_STATS_TYPE);
    Types.NestedField contentStatsField = type.field(TrackedFile.CONTENT_STATS_ID);

    assertThat(contentStatsField.type().asStructType()).isEqualTo(CONTENT_STATS_TYPE);
  }

  @Test
  public void schemaWithContentStatsReflectsInput() {
    Types.StructType smallStats =
        Types.StructType.of(optional(200, "col_1_value_count", Types.LongType.get()));
    Types.StructType largeStats =
        Types.StructType.of(
            optional(200, "col_1_value_count", Types.LongType.get()),
            optional(201, "col_1_null_count", Types.LongType.get()),
            optional(202, "col_2_value_count", Types.LongType.get()),
            optional(203, "col_2_null_count", Types.LongType.get()));

    Types.StructType smallType = TrackedFile.schemaWithContentStats(smallStats);
    Types.StructType largeType = TrackedFile.schemaWithContentStats(largeStats);

    Types.StructType smallResult =
        smallType.field(TrackedFile.CONTENT_STATS_ID).type().asStructType();
    Types.StructType largeResult =
        largeType.field(TrackedFile.CONTENT_STATS_ID).type().asStructType();

    assertThat(smallResult.fields()).hasSize(1);
    assertThat(largeResult.fields()).hasSize(4);
  }
}
