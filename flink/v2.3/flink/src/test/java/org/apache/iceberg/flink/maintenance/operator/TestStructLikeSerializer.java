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
package org.apache.iceberg.flink.maintenance.operator;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestStructLikeSerializer {

  private final StructLikeSerializer serializer = new StructLikeSerializer();

  @Test
  void encodeDecodeRoundTripsAcrossTypes() {
    Types.StructType partitionType =
        Types.StructType.of(
            Types.NestedField.required(100, "p_int", Types.IntegerType.get()),
            Types.NestedField.required(101, "p_str", Types.StringType.get()),
            Types.NestedField.required(102, "p_dec", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(103, "p_date", Types.DateType.get()),
            Types.NestedField.optional(104, "p_null", Types.StringType.get()));

    PartitionData partition = new PartitionData(partitionType);
    partition.set(0, 7);
    partition.set(1, "abc");
    partition.set(2, new BigDecimal("12.34"));
    partition.set(3, 19000);
    partition.set(4, null);

    StructLike decoded =
        StructLikeSerializer.decodePartition(
            serializer.encodePartition(partition, partitionType), partitionType);

    assertThat(decoded.get(0, Integer.class)).isEqualTo(7);
    assertThat(decoded.get(1, String.class)).isEqualTo("abc");
    assertThat(decoded.get(2, BigDecimal.class)).isEqualTo(new BigDecimal("12.34"));
    assertThat(decoded.get(3, Integer.class)).isEqualTo(19000);
    assertThat(decoded.get(4, String.class)).isNull();
  }

  @Test
  void unpartitionedEncodesToEmpty() {
    Types.StructType empty = PartitionSpec.unpartitioned().partitionType();
    PartitionData partition = new PartitionData(empty);

    byte[] encoded = serializer.encodePartition(partition, empty);
    assertThat(encoded).isEmpty();

    StructLike decoded = StructLikeSerializer.decodePartition(encoded, empty);
    assertThat(decoded.size()).isZero();
  }
}
