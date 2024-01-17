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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.transforms.BaseTruncatesProjection;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

public class TestTruncatesProjectionWithRangeIn extends BaseTruncatesProjection {
  @Test
  public void testIntegerStrictLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + 1}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerStrictUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value - 1}),
        Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerInclusiveLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}),
        Expression.Operation.RANGE_IN,
        "[90, 100, 100]");
  }

  @Test
  public void testIntegerInclusiveUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}),
        Expression.Operation.RANGE_IN,
        "[90, 90, 100]");
  }

  @Test
  public void testLongStrictLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + 1}, DataTypes.LongType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testLongStrictUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value - 1}, DataTypes.LongType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testLongInclusiveLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}, DataTypes.LongType),
        Expression.Operation.RANGE_IN,
        "[90, 100, 100]");
  }

  @Test
  public void testLongInclusiveUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();
    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value - 1, value, value + 1}, DataTypes.LongType),
        Expression.Operation.RANGE_IN,
        "[90, 90, 100]");
  }

  @Test
  public void testDecimalStrictLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    BigDecimal delta = new BigDecimal(1);
    assertProjectionStrictValue(
        spec,
        createPredicate(
            "value", new Object[] {value, value.add(delta)}, DataTypes.createDecimalType(9, 2)),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalStrictUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    BigDecimal delta = new BigDecimal(1);

    assertProjectionStrictValue(
        spec,
        createPredicate(
            "value",
            new Object[] {value, value.subtract(delta)},
            DataTypes.createDecimalType(9, 2)),
        Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalInclusiveLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        createPredicate(
            "value",
            new Object[] {value.add(delta), value, value.subtract(delta)},
            DataTypes.createDecimalType(9, 2)),
        Expression.Operation.RANGE_IN,
        "[99.00, 100.00, 101.00]");
  }

  @Test
  public void testDecimalInclusiveUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        createPredicate(
            "value",
            new Object[] {value.add(delta), value, value.subtract(delta)},
            DataTypes.createDecimalType(9, 2)),
        Expression.Operation.RANGE_IN,
        "[98.90, 99.90, 100.90]");
  }

  @Test
  public void testStringStrict() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionStrictValue(
        spec,
        createPredicate("value", new Object[] {value, value + "abc"}, DataTypes.StringType),
        Expression.Operation.FALSE);
  }

  @Test
  public void testStringInclusive() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionInclusive(
        spec,
        createPredicate("value", new Object[] {value, value + "abc"}, DataTypes.StringType),
        Expression.Operation.RANGE_IN,
        "[abcde, abcde]");
  }
}
