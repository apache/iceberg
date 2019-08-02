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

package org.apache.iceberg.transforms;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestTruncatesProjection {

  public void assertProjectionStrict(PartitionSpec spec, UnboundPredicate<?> filter,
                                     Expression.Operation expectedOp, String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Literal literal = predicate.literal();
    Truncate transform = (Truncate) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString(literal.value());
    Assert.assertEquals(expectedLiteral, output);
  }

  public void assertProjectionStrictValue(PartitionSpec spec, UnboundPredicate<?> filter,
                                          Expression.Operation expectedOp) {

    Expression projection = Projections.strict(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusiveValue(PartitionSpec spec, UnboundPredicate<?> filter,
                                             Expression.Operation expectedOp) {

    Expression projection = Projections.inclusive(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusive(PartitionSpec spec, UnboundPredicate<?> filter,
                                        Expression.Operation expectedOp, String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Literal literal = predicate.literal();
    Truncate transform = (Truncate) spec.getFieldsBySourceId(1).get(0).transform();
    String output = transform.toHumanString(literal.value());
    Assert.assertEquals(expectedLiteral, output);
  }

  @Test
  public void testIntegerStrictLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "100");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "100");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerStrictUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "90");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "90");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testIntegerInclusiveLowerBound() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "100");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testIntegerInclusiveUpperBound() {
    Integer value = 99;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "90");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testLongStrictLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "100");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "100");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testLongStrictUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "90");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "90");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testLongInclusiveLowerBound() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "100");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testLongInclusiveUpperBound() {
    Long value = 99L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "90");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "90");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testDecimalStrictLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "100.00");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "100.00");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalStrictUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "99.90");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "100.00");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "99.90");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "99.90");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testDecimalInclusiveLowerBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "100.00");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "100.00");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testDecimalInclusiveUpperBound() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("99.99").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 10).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "99.90");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "100.00");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "99.90");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "99.90");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testStringStrict() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, "abcde");
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, "abcde");
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, "abcde");
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, "abcde");
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "abcde");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testStringInclusive() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, "abcde");
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, "abcde");
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, "abcde");
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, "abcde");
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "abcde");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }

  @Test
  public void testBinaryStrict() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes("UTF-8"));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();
    String expectedValue = TransformUtil.base64encode(ByteBuffer.wrap("abcde".getBytes("UTF-8")));

    assertProjectionStrict(spec, lessThan("value", value), Expression.Operation.LT, expectedValue);
    assertProjectionStrict(spec, lessThanOrEqual("value", value), Expression.Operation.LT, expectedValue);
    assertProjectionStrict(spec, greaterThan("value", value), Expression.Operation.GT, expectedValue);
    assertProjectionStrict(spec, greaterThanOrEqual("value", value), Expression.Operation.GT, expectedValue);
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, expectedValue);
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
  }

  @Test
  public void testBinaryInclusive() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes("UTF-8"));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("value", 5).build();
    String expectedValue = TransformUtil.base64encode(ByteBuffer.wrap("abcde".getBytes("UTF-8")));

    assertProjectionInclusive(spec, lessThan("value", value), Expression.Operation.LT_EQ, expectedValue);
    assertProjectionInclusive(spec, lessThanOrEqual("value", value), Expression.Operation.LT_EQ, expectedValue);
    assertProjectionInclusive(spec, greaterThan("value", value), Expression.Operation.GT_EQ, expectedValue);
    assertProjectionInclusive(spec, greaterThanOrEqual("value", value), Expression.Operation.GT_EQ, expectedValue);
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, expectedValue);
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
  }
}
