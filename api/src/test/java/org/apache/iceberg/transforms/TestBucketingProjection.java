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

import static org.apache.iceberg.TestHelpers.assertAndUnwrapUnbound;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.expressions.Projections;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestBucketingProjection {

  public void assertProjectionStrict(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {

    Expression projection = Projections.strict(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(expectedOp, predicate.op());

    Assert.assertNotEquals(
        "Strict projection never runs for IN", Expression.Operation.IN, predicate.op());

    if (predicate.op() == Expression.Operation.NOT_IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(String::valueOf)
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<?> literal = predicate.literal();
      String output = String.valueOf(literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }

  public void assertProjectionStrictValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {
    Expression projection = Projections.strict(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusiveValue(
      PartitionSpec spec, UnboundPredicate<?> filter, Expression.Operation expectedOp) {
    Expression projection = Projections.inclusive(spec).project(filter);
    Assert.assertEquals(projection.op(), expectedOp);
  }

  public void assertProjectionInclusive(
      PartitionSpec spec,
      UnboundPredicate<?> filter,
      Expression.Operation expectedOp,
      String expectedLiteral) {
    Expression projection = Projections.inclusive(spec).project(filter);
    UnboundPredicate<?> predicate = assertAndUnwrapUnbound(projection);

    Assert.assertEquals(predicate.op(), expectedOp);

    Assert.assertNotEquals(
        "Inclusive projection never runs for NOT_IN", Expression.Operation.NOT_IN, predicate.op());

    if (predicate.op() == Expression.Operation.IN) {
      Iterable<?> values = Iterables.transform(predicate.literals(), Literal::value);
      String actual =
          Lists.newArrayList(values).stream()
              .sorted()
              .map(String::valueOf)
              .collect(Collectors.toList())
              .toString();
      Assert.assertEquals(expectedLiteral, actual);
    } else {
      Literal<?> literal = predicate.literal();
      String output = String.valueOf(literal.value());
      Assert.assertEquals(expectedLiteral, output);
    }
  }

  @Test
  public void testBucketIntegerStrict() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100) is 6
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "6");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[6, 7, 8]");
    assertProjectionStrictValue(spec, in("value", value, value + 1), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketIntegerInclusive() {
    Integer value = 100;
    Schema schema = new Schema(optional(1, "value", Types.IntegerType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100) is 6
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "6");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[6, 7, 8]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + 1), Expression.Operation.TRUE);
  }

  // all types

  @Test
  public void testBucketLongStrict() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100) is 6
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "6");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec,
        notIn("value", value - 1, value, value + 1),
        Expression.Operation.NOT_IN,
        "[6, 7, 8]");
    assertProjectionStrictValue(spec, in("value", value, value + 1), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketLongInclusive() {
    Long value = 100L;
    Schema schema = new Schema(optional(1, "value", Types.LongType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100) is 6
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "6");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value - 1, value, value + 1), Expression.Operation.IN, "[6, 7, 8]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + 1), Expression.Operation.TRUE);
  }

  @Test
  public void testBucketDecimalStrict() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100.00) is 2
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "2");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionStrict(
        spec,
        notIn("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.NOT_IN,
        "[2, 2, 6]");
    assertProjectionStrictValue(
        spec, in("value", value, value.add(delta)), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketDecimalInclusive() {
    Types.DecimalType type = Types.DecimalType.of(9, 2);
    BigDecimal value = (BigDecimal) Literal.of("100.00").to(type).value();
    Schema schema = new Schema(optional(1, "value", type));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. 100.00) is 2
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "2");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    BigDecimal delta = new BigDecimal(1);
    assertProjectionInclusive(
        spec,
        in("value", value.add(delta), value, value.subtract(delta)),
        Expression.Operation.IN,
        "[2, 2, 6]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value.add(delta)), Expression.Operation.TRUE);
  }

  @Test
  public void testBucketStringStrict() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. "abcdefg") is 4
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "4");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    assertProjectionStrict(
        spec, notIn("value", value, value + "abc"), Expression.Operation.NOT_IN, "[4, 9]");
    assertProjectionStrictValue(
        spec, in("value", value, value + "abc"), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketStringInclusive() {
    String value = "abcdefg";
    Schema schema = new Schema(optional(1, "value", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. "abcdefg") is 4
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "4");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    assertProjectionInclusive(
        spec, in("value", value, value + "abc"), Expression.Operation.IN, "[4, 9]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, value + "abc"), Expression.Operation.TRUE);
  }

  @Test
  public void testBucketByteBufferStrict() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes("UTF-8"));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. "abcdefg") is 4
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "4");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    ByteBuffer anotherValue = ByteBuffer.wrap("abcdehij".getBytes("UTF-8"));
    assertProjectionStrict(
        spec, notIn("value", value, anotherValue), Expression.Operation.NOT_IN, "[4, 6]");
    assertProjectionStrictValue(spec, in("value", value, anotherValue), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketByteBufferInclusive() throws Exception {
    ByteBuffer value = ByteBuffer.wrap("abcdefg".getBytes("UTF-8"));
    Schema schema = new Schema(optional(1, "value", Types.BinaryType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. "abcdefg") is 4
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "4");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    ByteBuffer anotherValue = ByteBuffer.wrap("abcdehij".getBytes("UTF-8"));
    assertProjectionInclusive(
        spec, in("value", value, anotherValue), Expression.Operation.IN, "[4, 6]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, anotherValue), Expression.Operation.TRUE);
  }

  @Test
  public void testBucketUUIDStrict() {
    UUID value = new UUID(123L, 456L);
    Schema schema = new Schema(optional(1, "value", Types.UUIDType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. UUID(123L, 456L)) is 4
    assertProjectionStrict(spec, notEqual("value", value), Expression.Operation.NOT_EQ, "4");
    assertProjectionStrictValue(spec, equal("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, lessThanOrEqual("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(spec, greaterThan("value", value), Expression.Operation.FALSE);
    assertProjectionStrictValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.FALSE);

    UUID anotherValue = new UUID(456L, 123L);
    assertProjectionStrict(
        spec, notIn("value", value, anotherValue), Expression.Operation.NOT_IN, "[4, 6]");
    assertProjectionStrictValue(spec, in("value", value, anotherValue), Expression.Operation.FALSE);
  }

  @Test
  public void testBucketUUIDInclusive() {
    UUID value = new UUID(123L, 456L);
    Schema schema = new Schema(optional(1, "value", Types.UUIDType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).bucket("value", 10).build();

    // the bucket number of the value (i.e. UUID(123L, 456L)) is 4
    assertProjectionInclusive(spec, equal("value", value), Expression.Operation.EQ, "4");
    assertProjectionInclusiveValue(spec, notEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, lessThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, lessThanOrEqual("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(spec, greaterThan("value", value), Expression.Operation.TRUE);
    assertProjectionInclusiveValue(
        spec, greaterThanOrEqual("value", value), Expression.Operation.TRUE);

    UUID anotherValue = new UUID(456L, 123L);
    assertProjectionInclusive(
        spec, in("value", value, anotherValue), Expression.Operation.IN, "[4, 6]");
    assertProjectionInclusiveValue(
        spec, notIn("value", value, anotherValue), Expression.Operation.TRUE);
  }
}
