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

import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestExpressionUtil {
  @Test
  public void testUnchangedUnaryPredicates() {
    for (Expression unary : Lists.newArrayList(
        Expressions.isNull("test"), Expressions.notNull("test"), Expressions.isNaN("test"), Expressions.notNaN("test"))
    ) {
      assertEquals(unary, ExpressionUtil.sanitize(unary));
    }
  }

  @Test
  public void testSanitizeIn() {
    assertEquals(
        Expressions.in("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.in("test", 34, 345)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.in("test", 34, 345)));
  }

  @Test
  public void testSanitizeNotIn() {
    assertEquals(
        Expressions.notIn("test", "(2-digit-int)", "(3-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notIn("test", 34, 345)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test NOT IN ((2-digit-int), (3-digit-int))",
        ExpressionUtil.toSanitizedString(Expressions.notIn("test", 34, 345)));
  }

  @Test
  public void testSanitizeLessThan() {
    assertEquals(
        Expressions.lessThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThan("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test < (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThan("test", 34)));
  }

  @Test
  public void testSanitizeLessThanOrEqual() {
    assertEquals(
        Expressions.lessThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.lessThanOrEqual("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test <= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.lessThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThan() {
    assertEquals(
        Expressions.greaterThan("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThan("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test > (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThan("test", 34)));
  }

  @Test
  public void testSanitizeGreaterThanOrEqual() {
    assertEquals(
        Expressions.greaterThanOrEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.greaterThanOrEqual("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test >= (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.greaterThanOrEqual("test", 34)));
  }

  @Test
  public void testSanitizeEqual() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34)));
  }

  @Test
  public void testSanitizeNotEqual() {
    assertEquals(
        Expressions.notEqual("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.notEqual("test", 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test != (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.notEqual("test", 34)));
  }

  @Test
  public void testSanitizeStartsWith() {
    assertEquals(
        Expressions.startsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.startsWith("test", "aaa")));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.startsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeNotStartsWith() {
    assertEquals(
        Expressions.notStartsWith("test", "(hash-34d05fb7)"),
        ExpressionUtil.sanitize(Expressions.notStartsWith("test", "aaa")));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test NOT STARTS WITH (hash-34d05fb7)",
        ExpressionUtil.toSanitizedString(Expressions.notStartsWith("test", "aaa")));
  }

  @Test
  public void testSanitizeTransformedTerm() {
    assertEquals(
        Expressions.equal(Expressions.truncate("test", 2), "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal(Expressions.truncate("test", 2), 34)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "truncate[2](test) = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal(Expressions.truncate("test", 2), 34)));
  }

  @Test
  public void testSanitizeLong() {
    assertEquals(
        Expressions.equal("test", "(2-digit-int)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34L)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-int)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34L)));
  }

  @Test
  public void testSanitizeFloat() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12F)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12F)));
  }

  @Test
  public void testSanitizeDouble() {
    assertEquals(
        Expressions.equal("test", "(2-digit-float)"),
        ExpressionUtil.sanitize(Expressions.equal("test", 34.12D)));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (2-digit-float)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", 34.12D)));
  }

  @Test
  public void testSanitizeDate() {
    assertEquals(
        Expressions.equal("test", "(date)"),
        ExpressionUtil.sanitize(Expressions.equal("test", "2022-04-29")));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (date)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", "2022-04-29")));
  }

  @Test
  public void testSanitizeTime() {
    assertEquals(
        Expressions.equal("test", "(time)"),
        ExpressionUtil.sanitize(Expressions.equal("test", "23:49:51")));

    Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
        "test = (time)",
        ExpressionUtil.toSanitizedString(Expressions.equal("test", "23:49:51")));
  }

  @Test
  public void testSanitizeTimestamp() {
    for (String timestamp : Lists.newArrayList("2022-04-29T23:49:51", "2022-04-29T23:49:51.123456",
        "2022-04-29T23:49:51-07:00", "2022-04-29T23:49:51.123456+01:00")) {
      assertEquals(
          Expressions.equal("test", "(timestamp)"),
          ExpressionUtil.sanitize(Expressions.equal("test", timestamp)));

      Assert.assertEquals("Sanitized string should be identical except for descriptive literal",
          "test = (timestamp)",
          ExpressionUtil.toSanitizedString(Expressions.equal("test", timestamp)));
    }
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
    Assert.assertEquals("Should apply the same transform",
        expected.transform().toString(), actual.transform().toString());
    assertEquals(expected.ref(), actual.ref());
  }
}
