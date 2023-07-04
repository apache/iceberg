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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Comparator;
import org.apache.avro.util.Utf8;
import org.apache.iceberg.expressions.Literal;
import org.junit.jupiter.api.Test;

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
    assertThat(stringComp.compare(s1, s2)).as("Should consider String and Utf8 equal").isZero();

    Comparator<CharSequence> utf8Comp = Literal.of(s2).comparator();
    assertThat(utf8Comp.compare(s1, s2)).as("Should consider String and Utf8 equal").isZero();
  }

  @Test
  public void testSeqLength() {
    String s1 = "abc";
    String s2 = "abcd";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();

    // Sanity check that String.compareTo gives the same result
    assertThat(s1)
        .as("When one string is a substring of the other, the longer is greater")
        .isLessThan(s2);
    assertThat(s2)
        .as("When one string is a substring of the other, the longer is greater")
        .isGreaterThan(s1);

    // Test the comparator
    assertThat(cmp.compare(s1, s2))
        .as("When one string is a substring of the other, the longer is greater")
        .isLessThan(0);
    assertThat(cmp.compare(s2, s1))
        .as("When one string is a substring of the other, the longer is greater")
        .isGreaterThan(0);
  }

  @Test
  public void testCharOrderBeforeLength() {
    // abcd < adc even though abcd is longer
    String s1 = "adc";
    String s2 = "abcd";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();

    // Sanity check that String.compareTo gives the same result
    assertThat(s1).as("First difference takes precedence over length").isGreaterThan(s2);
    assertThat(s2).as("First difference takes precedence over length").isLessThan(s1);

    // Test the comparator
    assertThat(cmp.compare(s1, s2))
        .as("First difference takes precedence over length")
        .isGreaterThan(0);
    assertThat(cmp.compare(s2, s1))
        .as("First difference takes precedence over length")
        .isLessThan(0);
  }

  @Test
  public void testNullHandling() {
    String s1 = "abc";

    Comparator<CharSequence> cmp = Literal.of(s1).comparator();
    assertThat(cmp.compare(null, s1)).as("null comes before non-null").isLessThan(0);
    assertThat(cmp.compare(s1, null)).as("null comes before non-null").isGreaterThan(0);
    assertThat(cmp.compare(null, null)).as("null equals null").isZero();
  }
}
