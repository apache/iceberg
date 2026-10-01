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
package org.apache.iceberg.flink.sink.shuffle;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSortKeyUtil {
  @Test
  public void testResultSchema() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.StringType.get()),
            Types.NestedField.required(2, "ratio", Types.DoubleType.get()),
            Types.NestedField.optional(
                3,
                "user",
                Types.StructType.of(
                    Types.NestedField.required(11, "name", Types.StringType.get()),
                    Types.NestedField.required(12, "ts", Types.TimestampType.withoutZone()),
                    Types.NestedField.optional(13, "device_id", Types.UUIDType.get()),
                    Types.NestedField.optional(
                        14,
                        "location",
                        Types.StructType.of(
                            Types.NestedField.required(101, "lat", Types.FloatType.get()),
                            Types.NestedField.required(102, "long", Types.FloatType.get()),
                            Types.NestedField.required(103, "blob", Types.BinaryType.get()))))));

    SortOrder sortOrder =
        SortOrder.builderFor(schema)
            .asc("ratio")
            .sortBy(Expressions.hour("user.ts"), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .sortBy(
                Expressions.bucket("user.device_id", 16), SortDirection.ASC, NullOrder.NULLS_FIRST)
            .sortBy(
                Expressions.truncate("user.location.blob", 16),
                SortDirection.ASC,
                NullOrder.NULLS_FIRST)
            .build();

    assertThat(SortKeyUtil.sortKeySchema(schema, sortOrder).asStruct())
        .isEqualTo(
            Types.StructType.of(
                Types.NestedField.required(0, "ratio_0", Types.DoubleType.get()),
                Types.NestedField.required(1, "ts_1", Types.IntegerType.get()),
                Types.NestedField.optional(2, "device_id_2", Types.IntegerType.get()),
                Types.NestedField.required(3, "blob_3", Types.BinaryType.get())));
  }
}
