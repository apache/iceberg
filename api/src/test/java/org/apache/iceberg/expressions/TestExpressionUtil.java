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
package org.apache.iceberg.expressions;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionUtil {
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "val", Types.IntegerType.get()),
          Types.NestedField.required(3, "val2", Types.IntegerType.get()),
          Types.NestedField.required(4, "ts", Types.TimestampType.withoutZone()),
          Types.NestedField.required(5, "date", Types.DateType.get()),
          Types.NestedField.required(6, "time", Types.DateType.get()),
          Types.NestedField.optional(7, "data", Types.StringType.get()),
          Types.NestedField.optional(8, "measurement", Types.DoubleType.get()));

  private static final Types.StructType STRUCT = SCHEMA.asStruct();

  @Test
  public void testUnchangedUnaryPredicates() {
    for (Expression unary :
        Lists.newArrayList(
            Expressions.isNull("test"),
            Expressions.notNull("test"),
            Expressions.isNaN("test"),
            Expressions.notNaN("test"))) {
      assertEquals(unary, ExpressionUtil.sanitize(unary));
    }
  }

  @Test
  public void testSanitizeIn() {
    assertEquals(
        Expressions.in("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.in("test", 34, 345)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.in("test", 34, 345)));
  }

  @Test
  public void testSanitizeNotIn() {
    assertEquals(
        Expressions.notIn("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notIn("test", 34, 345)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test NOT IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.notIn("test", 34, 345)));
  }

  @Test
  public void testSanitizeLessThan() {
    assertEquals(
        Expressions.lessThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThan("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test < (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThan("test", 34)));
  }

  @Test
  public void testSanitizeLessThanOrEqual() {
    assertEquals(
        Expressions.lessThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThanOrEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test <= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThan() {
    assertEquals(
        Expressions.greaterThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThan("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test > (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThan("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThanOrEqual() {
    assertEquals(
        Expressions.greaterThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThanOrEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test >= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeEqual() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34)));
  }

  @Test
  public void testSanitizeNotEqual() {
    assertEquals(
        Expressions.notEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notEqual("test", 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test != (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.notEqual("test", 34)));
  }

  @Test
  public void testSanitizeStartsWith() {
    assertEquals(
        Expressions.startsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.startsWith("test", "aaa")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.startsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeNotStartsWith() {
    assertEquals(
        Expressions.notStartsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.notStartsWith("test", "aaa")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test NOT STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.notStartsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeTransformedTerm() {
    assertEquals(
        Expressions.equal(Expressions.truncate("test", 2), "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal(Expressions.truncate("test", 2), 34)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "truncate[2](test) = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal(Expressions.truncate("test", 2), 34)));
  }

  @Test
  public void testSanitizeLong() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34L)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34L)));
  }

  @Test
  public void testSanitizeFloat() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12F)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12F)));
  }

  @Test
  public void testSanitizeDouble() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12D)));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12D)));
  }

  @Test
  public void testSanitizeDate() {
    assertEquals(
        Expressions.equal("test", "(date)"),
        ExpressionUtil.sanitize(Expressions.equal("test", "2022-04-29")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (date)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", "2022-04-29")));
  }

  @Test
  public void testSanitizeTime() {
    assertEquals(
        Expressions.equal("test", "(time)"),
        ExpressionUtil.sanitize(Expressions.equal("test", "23:49:51")));

    Assert.assertEquals(
        "Sanitized string should be identical except for descriptive literal",
        "test = (time)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", "23:49:51")));
  }

  @Test
  public void testSanitizeTimestamp() {
    for (String timestamp :
        Lists.newArrayList(
            "2022-04-29T23:49:51",
            "2022-04-29T23:49:51.123456",
            "2022-04-29T23:49:51-07:00",
            "2022-04-29T23:49:51.123456+01:00")) {
      assertEquals(
          Expressions.equal("test", "(timestamp)"),
          ExpressionUtil.sanitize(Expressions.equal("test", timestamp)));

      Assert.assertEquals(
          "Sanitized string should be identical except for descriptive literal",
          "test = (timestamp)",
          ExpressionUtil.toSanitizedString(Expressions.equal("test", timestamp)));
    }
  }

  @Test
  public void testIdenticalExpressionIsEquivalent() {
    Expression[] exprs =
        new Expression[] {
          Expressions.isNull("data"),
          Expressions.notNull("data"),
          Expressions.isNaN("measurement"),
          Expressions.notNaN("measurement"),
          Expressions.lessThan("id", 5),
          Expressions.lessThanOrEqual("id", 5),
          Expressions.greaterThan("id", 5),
          Expressions.greaterThanOrEqual("id", 5),
          Expressions.equal("id", 5),
          Expressions.notEqual("id", 5),
          Expressions.in("id", 5, 6),
          Expressions.notIn("id", 5, 6),
          Expressions.startsWith("data", "aaa"),
          Expressions.notStartsWith("data", "aaa"),
          Expressions.alwaysTrue(),
          Expressions.alwaysFalse(),
          Expressions.and(Expressions.lessThan("id", 5), Expressions.notNull("data")),
          Expressions.or(Expressions.lessThan("id", 5), Expressions.notNull("data")),
        };

    for (Expression expr : exprs) {
      Assert.assertTrue(
          "Should accept identical expression: " + expr,
          ExpressionUtil.equivalent(expr, expr, STRUCT, true));

      for (Expression other : exprs) {
        if (expr != other) {
          Assert.assertFalse(ExpressionUtil.equivalent(expr, other, STRUCT, true));
        }
      }
    }
  }

  @Test
  public void testIdenticalTermIsEquivalent() {
    UnboundTerm<?>[] terms =
        new UnboundTerm<?>[] {
          Expressions.ref("id"),
          Expressions.truncate("id", 2),
          Expressions.bucket("id", 16),
          Expressions.year("ts"),
          Expressions.month("ts"),
          Expressions.day("ts"),
          Expressions.hour("ts"),
        };

    for (UnboundTerm<?> term : terms) {
      BoundTerm<?> bound = term.bind(STRUCT, true);
      Assert.assertTrue("Should accept identical expression: " + term, bound.isEquivalentTo(bound));

      for (UnboundTerm<?> other : terms) {
        if (term != other) {
          Assert.assertFalse(bound.isEquivalentTo(other.bind(STRUCT, true)));
        }
      }
    }
  }

  @Test
  public void testRefEquivalence() {
    Assert.assertFalse(
        "Should not find different refs equivalent",
        Expressions.ref("val")
            .bind(STRUCT, true)
            .isEquivalentTo(Expressions.ref("val2").bind(STRUCT, true)));
  }

  @Test
  public void testInEquivalence() {
    Assert.assertTrue(
        "Should ignore duplicate longs (in)",
        ExpressionUtil.equivalent(
            Expressions.in("id", 1, 2, 1), Expressions.in("id", 2, 1, 2), STRUCT, true));
    Assert.assertTrue(
        "Should ignore duplicate longs (notIn)",
        ExpressionUtil.equivalent(
            Expressions.notIn("id", 1, 2, 1), Expressions.notIn("id", 2, 1, 2), STRUCT, true));

    Assert.assertTrue(
        "Should ignore duplicate strings (in)",
        ExpressionUtil.equivalent(
            Expressions.in("data", "a", "b", "a"), Expressions.in("data", "b", "a"), STRUCT, true));
    Assert.assertTrue(
        "Should ignore duplicate strings (notIn)",
        ExpressionUtil.equivalent(
            Expressions.notIn("data", "b", "b"), Expressions.notIn("data", "b"), STRUCT, true));

    Assert.assertTrue(
        "Should detect equivalence with equal (in, string)",
        ExpressionUtil.equivalent(
            Expressions.in("data", "a"), Expressions.equal("data", "a"), STRUCT, true));
    Assert.assertTrue(
        "Should detect equivalence with notEqual (notIn, long)",
        ExpressionUtil.equivalent(
            Expressions.notIn("id", 1), Expressions.notEqual("id", 1), STRUCT, true));

    Assert.assertFalse(
        "Should detect different sets (in, long)",
        ExpressionUtil.equivalent(
            Expressions.in("id", 1, 2, 3), Expressions.in("id", 1, 2), STRUCT, true));
    Assert.assertFalse(
        "Should detect different sets (notIn, string)",
        ExpressionUtil.equivalent(
            Expressions.notIn("data", "a", "b"), Expressions.notIn("data", "a"), STRUCT, true));
  }

  @Test
  public void testInequalityEquivalence() {
    String[] cols = new String[] {"id", "val", "ts", "date", "time"};

    for (String col : cols) {
      Assert.assertTrue(
          "Should detect < to <= equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.lessThan(col, 34L), Expressions.lessThanOrEqual(col, 33L), STRUCT, true));
      Assert.assertTrue(
          "Should detect <= to < equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.lessThanOrEqual(col, 34L), Expressions.lessThan(col, 35L), STRUCT, true));
      Assert.assertTrue(
          "Should detect > to >= equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.greaterThan(col, 34L),
              Expressions.greaterThanOrEqual(col, 35L),
              STRUCT,
              true));
      Assert.assertTrue(
          "Should detect >= to > equivalence: " + col,
          ExpressionUtil.equivalent(
              Expressions.greaterThanOrEqual(col, 34L),
              Expressions.greaterThan(col, 33L),
              STRUCT,
              true));
    }

    Assert.assertFalse(
        "Should not detect equivalence for different columns",
        ExpressionUtil.equivalent(
            Expressions.lessThan("val", 34L),
            Expressions.lessThanOrEqual("val2", 33L),
            STRUCT,
            true));
    Assert.assertFalse(
        "Should not detect equivalence for different types",
        ExpressionUtil.equivalent(
            Expressions.lessThan("val", 34L),
            Expressions.lessThanOrEqual("id", 33L),
            STRUCT,
            true));
  }

  @Test
  public void testAndEquivalence() {
    Assert.assertTrue(
        "Should detect and equivalence in any order",
        ExpressionUtil.equivalent(
            Expressions.and(
                Expressions.lessThan("id", 34), Expressions.greaterThanOrEqual("id", 20)),
            Expressions.and(
                Expressions.greaterThan("id", 19L), Expressions.lessThanOrEqual("id", 33L)),
            STRUCT,
            true));
  }

  @Test
  public void testOrEquivalence() {
    Assert.assertTrue(
        "Should detect or equivalence in any order",
        ExpressionUtil.equivalent(
            Expressions.or(
                Expressions.lessThan("id", 20), Expressions.greaterThanOrEqual("id", 34)),
            Expressions.or(
                Expressions.greaterThan("id", 33L), Expressions.lessThanOrEqual("id", 19L)),
            STRUCT,
            true));
  }

  @Test
  public void testNotEquivalence() {
    Assert.assertTrue(
        "Should detect not equivalence by rewriting",
        ExpressionUtil.equivalent(
            Expressions.not(
                Expressions.or(
                    Expressions.in("data", "a"), Expressions.greaterThanOrEqual("id", 34))),
            Expressions.and(Expressions.lessThan("id", 34L), Expressions.notEqual("data", "a")),
            STRUCT,
            true));
  }

  @Test
  public void testSelectsPartitions() {
    Assert.assertTrue(
        "Should select partitions, on boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThan("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
            true));

    Assert.assertFalse(
        "Should not select partitions, 1 ms off boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThanOrEqual("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).hour("ts").build(),
            true));

    Assert.assertFalse(
        "Should not select partitions, on hour not day boundary",
        ExpressionUtil.selectsPartitions(
            Expressions.lessThan("ts", "2021-03-09T10:00:00.000000"),
            PartitionSpec.builderFor(SCHEMA).day("ts").build(),
            true));
  }

  private void assertEquals(Expression expected, Expression actual) {
    if (expected instanceof UnboundPredicate) {
      Assert.assertTrue("Should be an UnboundPredicate", actual instanceof UnboundPredicate);
      assertEquals((UnboundPredicate<?>) expected, (UnboundPredicate<?>) actual);
    } else {
      Assert.fail("Unknown expected expression: " + expected);
    }
  }

  private void assertEquals(UnboundPredicate<?> expected, UnboundPredicate<?> actual) {
    Assert.assertEquals("Operation should match", expected.op(), actual.op());
    assertEquals(expected.term(), actual.term());
    Assert.assertEquals("Literals should match", expected.literals(), actual.literals());
  }

  private void assertEquals(UnboundTerm<?> expected, UnboundTerm<?> actual) {
    if (expected instanceof NamedReference) {
      Assert.assertTrue("Should be a NamedReference", actual instanceof NamedReference);
      assertEquals((NamedReference<?>) expected, (NamedReference<?>) actual);
    } else if (expected instanceof UnboundTransform) {
      Assert.assertTrue("Should be an UnboundTransform", actual instanceof UnboundTransform);
      assertEquals((UnboundTransform<?, ?>) expected, (UnboundTransform<?, ?>) actual);
    } else {
      Assert.fail("Unknown expected term: " + expected);
    }
  }

  private void assertEquals(NamedReference<?> expected, NamedReference<?> actual) {
    Assert.assertEquals("Should reference the same field name", expected.name(), actual.name());
  }

  private void assertEquals(UnboundTransform<?, ?> expected, UnboundTransform<?, ?> actual) {
    Assert.assertEquals(
        "Should apply the same transform",
        expected.transform().toString(),
        actual.transform().toString());
    assertEquals(expected.ref(), actual.ref());
  }
}
