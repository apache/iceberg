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

package org.apache.iceberg.hive.legacy;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.not;
import static org.apache.iceberg.expressions.Expressions.notEqual;
import static org.apache.iceberg.expressions.Expressions.notIn;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.expressions.Expressions.or;
import static org.apache.iceberg.hive.legacy.HiveExpressions.simplifyPartitionFilter;
import static org.apache.iceberg.hive.legacy.HiveExpressions.toPartitionFilterString;


public class TestHiveExpressions {

  @Test
  public void testSimplifyRemoveNonPartitionColumns() {
    Expression input = and(and(equal("pCol", 1), equal("nonpCol", 2)), isNull("nonpCol"));
    Expression expected = equal("pCol", 1);
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyRemoveNot() {
    Expression input = not(and(equal("pCol", 1), equal("pCol", 2)));
    Expression expected = or(notEqual("pCol", 1), notEqual("pCol", 2));
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyRemoveIsNull() {
    Expression input = isNull("pcol");
    Expression expected = alwaysFalse();
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyRemoveNotNull() {
    Expression input = notNull("pcol");
    Expression expected = alwaysTrue();
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyExpandIn() {
    Expression input = in("pcol", 1, 2, 3);
    Expression expected = or(or(equal("pcol", 1), equal("pcol", 2)), equal("pcol", 3));
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyExpandNotIn() {
    Expression input = notIn("pcol", 1, 2, 3);
    Expression expected = and(and(notEqual("pcol", 1), notEqual("pcol", 2)), notEqual("pcol", 3));
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyRemoveAlwaysTrueChildren() {
    Expression input = and(alwaysTrue(), equal("pcol", 1));
    Expression expected = equal("pcol", 1);
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());

    input = or(alwaysTrue(), equal("pcol", 1));
    expected = alwaysTrue();
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testSimplifyRemoveAlwaysFalseChildren() {
    Expression input = and(alwaysFalse(), equal("pcol", 1));
    Expression expected = alwaysFalse();
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());

    input = or(alwaysFalse(), equal("pcol", 1));
    expected = equal("pcol", 1);
    Assert.assertEquals(expected.toString(), simplifyPartitionFilter(input, ImmutableSet.of("pcol")).toString());
  }

  @Test
  public void testToPartitionFilterStringEscapeStringLiterals() {
    Expression input = equal("pcol", "s'1");
    Assert.assertEquals("( pcol = 's\\'1' )", toPartitionFilterString(input));
  }
}
