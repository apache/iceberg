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

package org.apache.iceberg;

import org.apache.iceberg.InformationSchemaNamespacesTable.PartialNamespaceEvaluator;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestPartialNamespaceEvaluator {
  @Parameterized.Parameters(name = "column = {0}")
  public static Object[] parameters() {
    return new Object[] {"namespace_name", "parent_namespace_name"};
  }

  private final String column;

  public TestPartialNamespaceEvaluator(String column) {
    this.column = column;
  }

  @Test
  public void testNullChecksName() {
    Assert.assertFalse("Should never be null", eval(Expressions.isNull(column), Namespace.of("a")));
    Assert.assertTrue("Should never be null", eval(Expressions.notNull(column), Namespace.of("a")));
  }

  @Test
  public void testAnd() {
    Expression expr =
        Expressions.and(
            Expressions.startsWith("namespace_name", "a.b"),
            Expressions.startsWith("parent_namespace_name", "a"));

    PartialNamespaceEvaluator evaluator = new PartialNamespaceEvaluator(expr, true);

    Namespace ab = Namespace.of("a", "b");
    Assert.assertTrue("Should match exact namespace", evaluator.eval(ab.toString(), ab.parent().toString()));

    Namespace aab = Namespace.of("aa", "b");
    Assert.assertFalse("Should not match namespace name", evaluator.eval(aab.toString(), aab.parent().toString()));
  }

  @Test
  public void testOr() {
    Expression expr =
        Expressions.or(
            Expressions.startsWith("namespace_name", "b.c"),
            Expressions.startsWith("parent_namespace_name", "a"));

    PartialNamespaceEvaluator evaluator = new PartialNamespaceEvaluator(expr, true);

    Namespace aab = Namespace.of("aa", "b");
    Assert.assertTrue("Should match by parent name", evaluator.eval(aab.toString(), aab.parent().toString()));

    Namespace bcd = Namespace.of("b.c", "d");
    Assert.assertTrue("Should match by parent name", evaluator.eval(bcd.toString(), bcd.parent().toString()));

    Namespace c = Namespace.of("c");
    Assert.assertFalse("Should not match either part", evaluator.eval(c.toString(), c.parent().toString()));
  }

  @Test
  public void testLessThanOrEqualToName() {
    Expression ltEq = Expressions.lessThanOrEqual(column, "b.b");

    Assert.assertTrue("Should match containing parent", eval(ltEq, Namespace.of("b")));
    Assert.assertTrue("Should match exact namespace", eval(ltEq, Namespace.of("b", "b")));
    Assert.assertTrue("Should match exact namespace", eval(ltEq, Namespace.of("b.b")));
    Assert.assertTrue("Should match unrelated lower parent", eval(ltEq, Namespace.of("a")));
    Assert.assertTrue("Should match unrelated lower peer", eval(ltEq, Namespace.of("b.a")));
    Assert.assertFalse("Should not match unrelated greater parent", eval(ltEq, Namespace.of("c")));
    Assert.assertFalse("Should not match unrelated greater peer", eval(ltEq, Namespace.of("b.c")));
    Assert.assertFalse("Should not match child", eval(ltEq, Namespace.of("b.b.c")));
    Assert.assertFalse("Should not match child", eval(ltEq, Namespace.of("b", "b", "c")));
  }

  @Test
  public void testLessThanName() {
    Expression lt = Expressions.lessThan(column, "b.b");

    Assert.assertTrue("Should match containing parent", eval(lt, Namespace.of("b")));
    Assert.assertFalse("Should not match exact namespace", eval(lt, Namespace.of("b", "b")));
    Assert.assertFalse("Should not match exact namespace", eval(lt, Namespace.of("b.b")));
    Assert.assertTrue("Should match unrelated lower parent", eval(lt, Namespace.of("a")));
    Assert.assertTrue("Should match unrelated lower peer", eval(lt, Namespace.of("b.a")));
    Assert.assertFalse("Should not match unrelated greater parent", eval(lt, Namespace.of("c")));
    Assert.assertFalse("Should not match unrelated greater peer", eval(lt, Namespace.of("b.c")));
    Assert.assertFalse("Should not match child", eval(lt, Namespace.of("b.b.c")));
    Assert.assertFalse("Should not match child", eval(lt, Namespace.of("b", "b", "c")));
  }

  @Test
  public void testGreaterThanOrEqualToName() {
    Expression gtEq = Expressions.greaterThanOrEqual(column, "b.b");

    Assert.assertTrue("Should match containing parent", eval(gtEq, Namespace.of("b")));
    Assert.assertTrue("Should match exact namespace", eval(gtEq, Namespace.of("b", "b")));
    Assert.assertTrue("Should match exact namespace", eval(gtEq, Namespace.of("b.b")));
    Assert.assertFalse("Should not match unrelated lower parent", eval(gtEq, Namespace.of("a")));
    Assert.assertFalse("Should not match unrelated lower peer", eval(gtEq, Namespace.of("b.a")));
    Assert.assertTrue("Should match unrelated greater parent", eval(gtEq, Namespace.of("c")));
    Assert.assertTrue("Should match unrelated greater peer", eval(gtEq, Namespace.of("b.c")));
    Assert.assertTrue("Should match child", eval(gtEq, Namespace.of("b.b.c")));
    Assert.assertTrue("Should match child", eval(gtEq, Namespace.of("b", "b", "c")));
  }

  @Test
  public void testGreaterThanName() {
    Expression gt = Expressions.greaterThan(column, "b.b");

    Assert.assertTrue("Should match containing parent", eval(gt, Namespace.of("b")));
    Assert.assertFalse("Should not match exact namespace", eval(gt, Namespace.of("b", "b")));
    Assert.assertFalse("Should not match exact namespace", eval(gt, Namespace.of("b.b")));
    Assert.assertFalse("Should not match unrelated lower parent", eval(gt, Namespace.of("a")));
    Assert.assertFalse("Should not match unrelated lower peer", eval(gt, Namespace.of("b.a")));
    Assert.assertTrue("Should match unrelated greater parent", eval(gt, Namespace.of("c")));
    Assert.assertTrue("Should match unrelated greater peer", eval(gt, Namespace.of("b.c")));
    Assert.assertTrue("Should match child", eval(gt, Namespace.of("b.b.c")));
    Assert.assertTrue("Should match child", eval(gt, Namespace.of("b", "b", "c")));
  }

  @Test
  public void testEqualsName() {
    Expression eq = Expressions.equal(column, "a.b");

    Assert.assertTrue("Should match containing parent", eval(eq, Namespace.of("a")));
    Assert.assertTrue("Should match exact namespace", eval(eq, Namespace.of("a", "b")));
    Assert.assertTrue("Should match exact namespace", eval(eq, Namespace.of("a.b")));
    Assert.assertFalse("Should not match unrelated parent", eval(eq, Namespace.of("b")));
    Assert.assertFalse("Should not match unrelated peer", eval(eq, Namespace.of("a.c")));
    Assert.assertFalse("Should not match child", eval(eq, Namespace.of("a.b.c")));
    Assert.assertFalse("Should not match child", eval(eq, Namespace.of("a", "b", "c")));
  }

  @Test
  public void testNotEqualsName() {
    Expression notEq = Expressions.notEqual(column, "a.b");

    Assert.assertTrue("Should match containing parent", eval(notEq, Namespace.of("a")));
    Assert.assertFalse("Should not match exact namespace", eval(notEq, Namespace.of("a", "b")));
    Assert.assertFalse("Should not match exact namespace", eval(notEq, Namespace.of("a.b")));
    Assert.assertTrue("Should match unrelated parent", eval(notEq, Namespace.of("b")));
    Assert.assertTrue("Should match unrelated peer", eval(notEq, Namespace.of("a.c")));
    Assert.assertTrue("Should match child", eval(notEq, Namespace.of("a.b.c")));
    Assert.assertTrue("Should match child", eval(notEq, Namespace.of("a", "b", "c")));
  }

  @Test
  public void testInName() {
    Expression in = Expressions.in(column, "a.b", "a.d.e");

    Assert.assertTrue("Should match containing parent", eval(in, Namespace.of("a")));
    Assert.assertTrue("Should match containing parent", eval(in, Namespace.of("a", "d")));
    Assert.assertTrue("Should match containing parent", eval(in, Namespace.of("a.d")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a", "b")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a.b")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a", "d", "e")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a.d", "e")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a", "d.e")));
    Assert.assertTrue("Should match exact namespace", eval(in, Namespace.of("a.d.e")));
    Assert.assertFalse("Should not match unrelated parent", eval(in, Namespace.of("b")));
    Assert.assertFalse("Should not match unrelated peer", eval(in, Namespace.of("a.c")));
    Assert.assertFalse("Should not match child", eval(in, Namespace.of("a.b.c")));
    Assert.assertFalse("Should not match child", eval(in, Namespace.of("a", "b", "c")));
    Assert.assertFalse("Should not match child", eval(in, Namespace.of("a.d.e.f")));
    Assert.assertFalse("Should not match child", eval(in, Namespace.of("a.d", "e.f")));
  }

  @Test
  public void testNotInName() {
    Expression notIn = Expressions.notIn(column, "a.b", "a.d.e");

    Assert.assertTrue("Should match containing parent", eval(notIn, Namespace.of("a")));
    Assert.assertTrue("Should match containing parent", eval(notIn, Namespace.of("a", "d")));
    Assert.assertTrue("Should match containing parent", eval(notIn, Namespace.of("a.d")));
    Assert.assertFalse("Should not match exact namespace", eval(notIn, Namespace.of("a", "b")));
    Assert.assertFalse("Should not match exact namespace", eval(notIn, Namespace.of("a.b")));
    Assert.assertFalse(
        "Should not match exact namespace", eval(notIn, Namespace.of("a", "d", "e")));
    Assert.assertFalse("Should not match exact namespace", eval(notIn, Namespace.of("a.d", "e")));
    Assert.assertFalse("Should not match exact namespace", eval(notIn, Namespace.of("a", "d.e")));
    Assert.assertFalse("Should not match exact namespace", eval(notIn, Namespace.of("a.d.e")));
    Assert.assertTrue("Should match unrelated parent", eval(notIn, Namespace.of("b")));
    Assert.assertTrue("Should match unrelated peer", eval(notIn, Namespace.of("a.c")));
    Assert.assertTrue("Should match child", eval(notIn, Namespace.of("a.b.c")));
    Assert.assertTrue("Should match child", eval(notIn, Namespace.of("a", "b", "c")));
    Assert.assertTrue("Should match child", eval(notIn, Namespace.of("a.d.e.f")));
    Assert.assertTrue("Should match child", eval(notIn, Namespace.of("a.d", "e.f")));
  }

  @Test
  public void testStartsWithName() {
    Expression startsWith = Expressions.startsWith(column, "a.b");

    Assert.assertTrue("Should match containing parent", eval(startsWith, Namespace.of("a")));
    Assert.assertTrue("Should match exact namespace", eval(startsWith, Namespace.of("a", "b")));
    Assert.assertTrue("Should match exact namespace", eval(startsWith, Namespace.of("a.b")));
    Assert.assertTrue("Should match prefix", eval(startsWith, Namespace.of("a.bc")));
    Assert.assertTrue("Should match child", eval(startsWith, Namespace.of("a.b.c")));
    Assert.assertTrue("Should match child", eval(startsWith, Namespace.of("a", "b", "c")));
    Assert.assertFalse("Should not match unrelated parent", eval(startsWith, Namespace.of("b")));
    Assert.assertFalse("Should not match unrelated peer", eval(startsWith, Namespace.of("a.c")));
  }

  @Test
  public void testNotStartsWithName() {
    Expression notStartsWith = Expressions.notStartsWith(column, "a.b");

    Assert.assertTrue(
        "Should match containing parent (may contain not matching)",
        eval(notStartsWith, Namespace.of("a")));
    Assert.assertFalse(
        "Should not match exact namespace", eval(notStartsWith, Namespace.of("a", "b")));
    Assert.assertFalse(
        "Should not match exact namespace", eval(notStartsWith, Namespace.of("a.b")));
    Assert.assertFalse("Should not match prefix", eval(notStartsWith, Namespace.of("a.bc")));
    Assert.assertFalse("Should not match child", eval(notStartsWith, Namespace.of("a.b.c")));
    Assert.assertFalse("Should not match child", eval(notStartsWith, Namespace.of("a", "b", "c")));
    Assert.assertTrue("Should match unrelated parent", eval(notStartsWith, Namespace.of("b")));
    Assert.assertTrue("Should match unrelated peer", eval(notStartsWith, Namespace.of("a.c")));
  }

  public boolean eval(Expression expr, Namespace ns) {
    PartialNamespaceEvaluator evaluator = new PartialNamespaceEvaluator(expr, true);
    // pass the same namespace as a parent and ns to reuse test cases
    return evaluator.eval(ns.toString(), ns.toString());
  }
}
