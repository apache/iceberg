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
package org.apache.iceberg.spark.broadcastvar.expressions.transforms;

import static org.apache.iceberg.spark.broadcastvar.expressions.RangeInTestUtils.createPredicate;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.math.BigDecimal;
import java.util.UUID;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.transforms.BaseBucketingProjection;
import org.apache.iceberg.types.Types;
import org.junit.Test;

public class TestBucketingProjectionWithRangeIn extends BaseBucketingProjection {
  @Test
  public void testBucketIntegerStrict() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + 1}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testBucketIntegerInclusive() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}),
        Expression.Operation.RANGE_IN,
        "[6, 7, " + "8]");
  }

  @Test
  public void testBucketLongStrict() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + 1}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testBucketLongInclusive() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}),
        Expression.Operation.RANGE_IN,
        "[6, 7, 8]");
  }

  @Test
  public void testBucketDecimalStrict() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    BigDecimal delta = new BigDecimal(1);
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value.add(delta)}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testBucketDecimalInclusive() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value.add(delta), value, value.subtract(delta)}),
        Expression.Operation.RANGE_IN,
        "[2, 2, 6]");
  }

  @Test
  public void testBucketStringStrict() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + "abc"}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testBucketStringInclusive() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value, value + "abc"}),
        Expression.Operation.RANGE_IN,
        "[4, 9]");
  }

  @Test
  public void testBucketUUIDStrict() {
    UUID value = new UUID(123L, 456L);
    Schema schema = new Schema(optional(1, "value", Types.UUIDType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    UUID anotherValue = new UUID(456L, 123L);
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, anotherValue}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testBucketUUIDInclusive() {
    UUID value = new UUID(123L, 456L);
    Schema schema = new Schema(optional(1, "value", Types.UUIDType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();
    UUID anotherValue = new UUID(456L, 123L);
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value, anotherValue}),
        Expression.Operation.RANGE_IN,
        "[4, 6]");
  }
}
