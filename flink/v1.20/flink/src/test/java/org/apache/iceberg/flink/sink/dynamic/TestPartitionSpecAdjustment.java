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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestPartitionSpecAdjustment {

  @Test
  void testPartitionSpecSourceIdRemappingBasedOnFieldNames() {
    Schema specSchema =
        new Schema(
            // Use zero-based field ids
            Types.NestedField.required(0, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                1,
                "data",
                Types.StructType.of(Types.NestedField.required(2, "str", Types.StringType.get()))));

    PartitionSpec spec = PartitionSpec.builderFor(specSchema).bucket("id", 10).build();

    Schema tableSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(
                2,
                "data",
                Types.StructType.of(Types.NestedField.required(3, "str", Types.StringType.get()))));

    PartitionSpec adjustedSpec =
        PartitionSpecAdjustment.adjustPartitionSpecToTableSchema(tableSchema, spec);

    assertThat(adjustedSpec)
        .isEqualTo(PartitionSpec.builderFor(tableSchema).bucket("id", 10).build());
  }
}
