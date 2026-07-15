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
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestStructLikeSerializer {

  private static final Types.StructType KEY_TYPE =
      Types.StructType.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  private final StructLikeSerializer serializer = new StructLikeSerializer();

  @Test
  void serializeKeyIsDeterministic() {
    GenericRecord key = GenericRecord.create(KEY_TYPE);
    key.set(0, 42);
    key.set(1, "hello");

    SerializedEqualityValues key1 = serializer.serializeKey(key, KEY_TYPE);
    SerializedEqualityValues key2 = serializer.serializeKey(key, KEY_TYPE);
    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void serializeKeyDiffersForDifferentKeys() {
    GenericRecord record1 = GenericRecord.create(KEY_TYPE);
    record1.set(0, 1);
    record1.set(1, "a");

    GenericRecord record2 = GenericRecord.create(KEY_TYPE);
    record2.set(0, 2);
    record2.set(1, "b");

    SerializedEqualityValues key1 = serializer.serializeKey(record1, KEY_TYPE);
    SerializedEqualityValues key2 = serializer.serializeKey(record2, KEY_TYPE);
    assertThat(key1).isNotEqualTo(key2);
  }

  @Test
  void sameValuesDifferentFieldSetsAreNotEqual() {
    Types.StructType typeWithField1 =
        Types.StructType.of(Types.NestedField.required(1, "id", Types.IntegerType.get()));
    Types.StructType typeWithField3 =
        Types.StructType.of(Types.NestedField.required(3, "other", Types.IntegerType.get()));

    GenericRecord record1 = GenericRecord.create(typeWithField1);
    record1.set(0, 42);

    GenericRecord record3 = GenericRecord.create(typeWithField3);
    record3.set(0, 42);

    SerializedEqualityValues key1 = serializer.serializeKey(record1, typeWithField1);
    SerializedEqualityValues key2 = serializer.serializeKey(record3, typeWithField3);
    assertThat(key1).isNotEqualTo(key2);
  }

  @Test
  void serializeKeyWithDecimal() {
    Types.StructType decimalKeyType =
        Types.StructType.of(
            Types.NestedField.required(1, "id", Types.DecimalType.of(10, 2)),
            Types.NestedField.required(2, "name", Types.StringType.get()));

    GenericRecord record = GenericRecord.create(decimalKeyType);
    record.set(0, new BigDecimal("123.45"));
    record.set(1, "test");

    SerializedEqualityValues key1 = serializer.serializeKey(record, decimalKeyType);
    SerializedEqualityValues key2 = serializer.serializeKey(record, decimalKeyType);
    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void serializeKeyHandlesNulls() {
    GenericRecord record = GenericRecord.create(KEY_TYPE);
    record.set(0, 1);
    record.set(1, null);

    SerializedEqualityValues key1 = serializer.serializeKey(record, KEY_TYPE);
    SerializedEqualityValues key2 = serializer.serializeKey(record, KEY_TYPE);
    assertThat(key1).isEqualTo(key2);
  }

  @Test
  void nullDiffersFromNonNull() {
    GenericRecord withNull = GenericRecord.create(KEY_TYPE);
    withNull.set(0, 1);
    withNull.set(1, null);

    GenericRecord withValue = GenericRecord.create(KEY_TYPE);
    withValue.set(0, 1);
    withValue.set(1, "");

    assertThat(serializer.serializeKey(withNull, KEY_TYPE))
        .isNotEqualTo(serializer.serializeKey(withValue, KEY_TYPE));
  }

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
