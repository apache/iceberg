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

import java.util.function.UnaryOperator;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestPartitionUtil {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          required(3, "ts", Types.TimestampType.withoutZone()),
          required(4, "category", Types.StringType.get()));

  @Test
  void testConvertPartitionFuncIdentity() {
    // Test when source and output specs are the same
    PartitionSpec spec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(1).build();

    UnaryOperator<StructLike> converter = PartitionUtil.convertPartitionFunc(spec, spec);
    StructLike partition = Row.of("test");

    StructLike result = converter.apply(partition);
    assertThat(result).isSameAs(partition);
  }

  @Test
  void testConvertPartitionFuncIdentityToIdentity() {
    // Test converting between specs with same identity transform
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of("test_value");

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, String.class)).isEqualTo("test_value");
  }

  @Test
  void testConvertPartitionFuncHoursToDays() {
    // Test converting hours to days
    PartitionSpec sourceSpec = PartitionSpec.builderFor(SCHEMA).hour("ts").withSpecId(1).build();
    PartitionSpec outputSpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    // 489100 hours since epoch = 2025-10-18-04
    StructLike sourcePartition = Row.of(489100);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, Integer.class)).isEqualTo(20379);
  }

  @Test
  void testConvertPartitionFuncDaysToMonths() {
    // Test converting days to months
    PartitionSpec sourceSpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(1).build();
    PartitionSpec outputSpec = PartitionSpec.builderFor(SCHEMA).month("ts").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    // 20379 days since epoch = 2025-10-18
    StructLike sourcePartition = Row.of(20379);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, Integer.class)).isEqualTo(669);
  }

  @Test
  void testConvertPartitionFuncDaysToYears() {
    // Test converting days to years
    PartitionSpec sourceSpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(1).build();
    PartitionSpec outputSpec = PartitionSpec.builderFor(SCHEMA).year("ts").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    // 20379 days since epoch = 2025-10-18
    StructLike sourcePartition = Row.of(20379);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, Integer.class)).isEqualTo(55);
  }

  @Test
  void testConvertPartitionFuncTruncateString() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("category", 10).withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("category", 5).withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of("1234567890");

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, String.class)).isEqualTo("12345");
  }

  @Test
  void testConvertPartitionFuncTruncateInteger() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("id", 10).withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("id", 100).withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of(123450);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, Integer.class)).isEqualTo(123400);
  }

  @Test
  void testConvertPartitionFuncWithNullValue() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of((Object) null);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, Object.class)).isNull();
  }

  @Test
  void testConvertPartitionFuncMultipleFields() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA)
            .identity("data")
            .bucket("category", 16)
            .withSpecId(1)
            .build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA)
            .identity("data")
            .bucket("category", 16)
            .withSpecId(2)
            .build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of("test", 5);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, String.class)).isEqualTo("test");
    assertThat(result.get(1, Integer.class)).isEqualTo(5);
  }

  @Test
  void testConvertPartitionFuncSubsetOfFields() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").hour("ts").withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(2).build();

    UnaryOperator<StructLike> converter =
        PartitionUtil.convertPartitionFunc(sourceSpec, outputSpec);
    StructLike sourcePartition = Row.of("test", 5);

    StructLike result = converter.apply(sourcePartition);
    assertThat(result.get(0, String.class)).isEqualTo("test");
  }

  @Test
  void testNoRepartitionNeededIdentity() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(2).build();

    assertThat(PartitionUtil.needRepartition(sourceSpec, outputSpec)).isFalse();
  }

  @Test
  void testNoRepartitionNeededTimeTransforms() {
    PartitionSpec sourceSpec = PartitionSpec.builderFor(SCHEMA).hour("ts").withSpecId(1).build();
    PartitionSpec outputSpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(2).build();

    assertThat(PartitionUtil.needRepartition(sourceSpec, outputSpec)).isFalse();
  }

  @Test
  void testNoRepartitionNeededTruncateString() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("category", 10).withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).truncate("category", 5).withSpecId(2).build();

    assertThat(PartitionUtil.needRepartition(sourceSpec, outputSpec)).isFalse();
  }

  @Test
  void testNeedRepartitionMissingSourceField() {
    PartitionSpec sourceSpec =
        PartitionSpec.builderFor(SCHEMA).identity("data").withSpecId(1).build();
    PartitionSpec outputSpec =
        PartitionSpec.builderFor(SCHEMA).identity("category").withSpecId(2).build();

    assertThat(PartitionUtil.needRepartition(sourceSpec, outputSpec)).isTrue();
  }

  @Test
  void testNeedRepartitionIncompatibleTransforms() {
    PartitionSpec sourceSpec = PartitionSpec.builderFor(SCHEMA).day("ts").withSpecId(1).build();
    PartitionSpec outputSpec = PartitionSpec.builderFor(SCHEMA).hour("ts").withSpecId(2).build();

    // Can't convert from coarser (day) to finer (hour) granularity
    assertThat(PartitionUtil.needRepartition(sourceSpec, outputSpec)).isTrue();
  }
}
