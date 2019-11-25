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

import org.apache.iceberg.AssertHelpers;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.or;

public class TestExpressionHelpers {
  private final UnboundPredicate<?> pred = lessThan("x", 7);

  @Test
  public void testSimplifyOr() {
    Assert.assertEquals("alwaysTrue or pred => alwaysTrue",
        alwaysTrue(), or(alwaysTrue(), pred));
    Assert.assertEquals("pred or alwaysTrue => alwaysTrue",
        alwaysTrue(), or(pred, alwaysTrue()));

    Assert.assertEquals("alwaysFalse or pred => pred",
        pred, or(alwaysFalse(), pred));
    Assert.assertEquals("pred or alwaysTrue => pred",
        pred, or(pred, alwaysFalse()));
  }

  @Test
  public void testSimplifyAnd() {
    Assert.assertEquals("alwaysTrue and pred => pred",
        pred, and(alwaysTrue(), pred));
    Assert.assertEquals("pred and alwaysTrue => pred",
        pred, and(pred, alwaysTrue()));

    Assert.assertEquals("alwaysFalse and pred => alwaysFalse",
        alwaysFalse(), and(alwaysFalse(), pred));
    Assert.assertEquals("pred and alwaysFalse => alwaysFalse",
        alwaysFalse(), and(pred, alwaysFalse()));
  }

  @Test
  public void testSimplifyNot() {
    Assert.assertEquals("not(alwaysTrue) => alwaysFalse",
        alwaysFalse(), not(alwaysTrue()));
    Assert.assertEquals("not(alwaysFalse) => alwaysTrue",
        alwaysTrue(), not(alwaysFalse()));

    Assert.assertEquals("not(not(pred)) => pred",
        pred, not(not(pred)));
  }

  @Test
  public void testNullName() {
    AssertHelpers.assertThrows("Should catch null column names when creating expressions",
        NullPointerException.class, "Name cannot be null", () -> equal(null, 5));
  }

  @Test
  public void testMultiAnd() {
    Expression expected = and(
        and(
          equal("a", 1),
          equal("b", 2)),
        equal("c", 3));

    Expression actual = and(
        equal("a", 1),
        equal("b", 2),
        equal("c", 3));

    Assert.assertEquals(expected.toString(), actual.toString());
  }
}
