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
package org.apache.iceberg.types;

import java.util.Comparator;
import org.apache.iceberg.expressions.Literal;
import org.junit.Assert;
import org.junit.Test;

/**
 * This tests the Comparator returned by ComparableLiteral, which is used for most types.
 *
 * <p>The tests use assertTrue instead of assertEquals because the return value is not necessarily
 * one of {-1, 0, 1}. It is also more clear to compare the return value to 0 because the same
 * operation can be used: a &lt; b is equivalent to compare(a, b) &lt; 0.
 */
public class TestComparableComparator {
  @Test
  public void testNaturalOrder() {
    Comparator<Long> cmp = Literal.of(34L).comparator();
    Assert.assertTrue(
        "Should use the natural order for non-null values", cmp.compare(33L, 34L) < 0);
    Assert.assertTrue("Should use signed ordering", cmp.compare(33L, -34L) > 0);
  }

  @Test
  public void testNullHandling() {
    Comparator<Long> cmp = Literal.of(34L).comparator();
    Assert.assertTrue("null comes before non-null", cmp.compare(null, 34L) < 0);
    Assert.assertTrue("null comes before non-null", cmp.compare(34L, null) > 0);
    Assert.assertEquals("null equals null", 0, cmp.compare(null, null));
  }
}
