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
import org.apache.avro.util.Utf8;
import org.apache.iceberg.expressions.Literal;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the comparator returned by CharSequence literals.
 *
 * <p>The tests use assertTrue instead of assertEquals because the return value is not necessarily
 * one of {-1, 0, 1}. It is also more clear to compare the return value to 0 because the same
 * operation can be used: a &lt; b is equivalent to compare(a, b) &lt; 0.
 */
public class TestCharSeqComparator {
  @Test
  public void testStringAndUtf8() {
    String s1 = "abc";
    Utf8 s2 = new Utf8("abc");

    Comparator<CharSequence> stringComp = Literal.of(s1).comparator();
    Assert.assertEquals("Should consider String and Utf8 equal", 0, stringComp.compare(s1, s2));

    Comparator<CharSequence> utf8Comp = Literal.of(s2).comparator();
    Assert.assertEquals("Should consider String and Utf8 equal", 0, utf8Comp.compare(s1, s2));
  }

  @Test
  public void testSeqLength() {
    String s1 = "abc";
    String s2 = "abcd";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();

    // Sanity check that String.compareTo gives the same result
    Assert.assertTrue(
        "When one string is a substring of the other, the longer is greater", s1.compareTo(s2) < 0);
    Assert.assertTrue(
        "When one string is a substring of the other, the longer is greater", s2.compareTo(s1) > 0);

    // Test the comparator
    Assert.assertTrue(
        "When one string is a substring of the other, the longer is greater",
        cmp.compare(s1, s2) < 0);
    Assert.assertTrue(
        "When one string is a substring of the other, the longer is greater",
        cmp.compare(s2, s1) > 0);
  }

  @Test
  public void testCharOrderBeforeLength() {
    // abcd < adc even though abcd is longer
    String s1 = "adc";
    String s2 = "abcd";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();

    // Sanity check that String.compareTo gives the same result
    Assert.assertTrue("First difference takes precedence over length", s1.compareTo(s2) > 0);
    Assert.assertTrue("First difference takes precedence over length", s2.compareTo(s1) < 0);

    // Test the comparator
    Assert.assertTrue("First difference takes precedence over length", cmp.compare(s1, s2) > 0);
    Assert.assertTrue("First difference takes precedence over length", cmp.compare(s2, s1) < 0);
  }

  @Test
  public void testNullHandling() {
    String s1 = "abc";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();
    Assert.assertTrue("null comes before non-null", cmp.compare(null, s1) < 0);
    Assert.assertTrue("null comes before non-null", cmp.compare(s1, null) > 0);
    Assert.assertEquals("null equals null", 0, cmp.compare(null, null));
  }
}
